using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #368 / jasperfx#565: <c>ShardState.Failure</c> now rides along on the paused/stopped states the
///     daemon publishes; these pin the persistence half. A supervisor polling the database — CritterWatch
///     when the publishing node is DOWN, which is exactly when this matters — must see the same classified
///     reason an in-process observer does, and must stop seeing it once the shard recovers.
///     <para>
///     The progression rows are seeded directly rather than by running a daemon, so the only writer
///     against them is the call under test.
///     </para>
/// </summary>
public class shard_failure_progression_columns_tests : OneOffConfigurationsContext
{
    private const string TheShard = "FailureTelemetry:All";

    [Fact]
    public async Task persists_the_classified_failure_on_a_paused_shard()
    {
        await SeedProgressionRowAsync();

        await ((IEventDatabase)theDatabase).WriteExtendedProgressionAsync(
            Paused(ShardFailureCategory.EventSerialization, 4815, "quest_started", "tenant-a"));

        var row = await ReadFailureAsync();

        // The enum NAME, never the ordinal — reordering ShardFailureCategory must not silently re-label
        // rows that were written by an older deployment.
        row.Category.ShouldBe(nameof(ShardFailureCategory.EventSerialization));
        row.Sequence.ShouldBe(4815);
        row.EventType.ShouldBe("quest_started");
        row.TenantId.ShouldBe("tenant-a");
    }

    [Fact]
    public async Task a_recovered_shard_stops_reporting_the_reason_it_paused()
    {
        await SeedProgressionRowAsync();

        await ((IEventDatabase)theDatabase).WriteExtendedProgressionAsync(
            Paused(ShardFailureCategory.ApplyEvent, 99, "quest_started"));
        (await ReadFailureAsync()).Category.ShouldNotBeNull();

        // A restart supersedes whatever paused the agent last. Without this, every supervisor built on
        // these columns alerts forever on a failure the operator already fixed.
        await ((IEventDatabase)theDatabase).WriteExtendedProgressionAsync(
            WithoutFailure(ShardAction.Started, "Running"));

        var row = await ReadFailureAsync();
        row.Category.ShouldBeNull();
        row.Sequence.ShouldBeNull();
        row.EventType.ShouldBeNull();
        row.TenantId.ShouldBeNull();
    }

    [Fact]
    public async Task a_failureless_non_start_publication_leaves_the_reason_alone()
    {
        await SeedProgressionRowAsync();

        await ((IEventDatabase)theDatabase).WriteExtendedProgressionAsync(
            Paused(ShardFailureCategory.EventSerialization, 4815, "quest_started"));

        // The load-bearing case: SubscriptionAgent publishes a plain Stopped state (no Failure) right
        // behind the Paused one, and a heartbeat can arrive with no failure at all. An unconditional
        // write would erase the reason microseconds after recording it.
        await ((IEventDatabase)theDatabase).WriteExtendedProgressionAsync(
            WithoutFailure(ShardAction.Stopped, "Stopped"));
        await ((IEventDatabase)theDatabase).WriteExtendedProgressionAsync(
            WithoutFailure(ShardAction.Updated, "Running"));

        var row = await ReadFailureAsync();
        row.Category.ShouldBe(nameof(ShardFailureCategory.EventSerialization));
        row.Sequence.ShouldBe(4815);
    }

    [Fact]
    public async Task rehydrates_the_failure_on_the_read_side()
    {
        await SeedProgressionRowAsync();

        await ((IEventDatabase)theDatabase).WriteExtendedProgressionAsync(
            Paused(ShardFailureCategory.UnknownEventType, 1623, "trip_started", "tenant-b"));

        // A poller must get the same shape as a live ShardState observer, not just "it's Paused".
        var states = await theDatabase.AllProjectionProgress();
        var state = states.Single(x => x.ShardName == TheShard);

        state.Failure.ShouldNotBeNull();
        state.Failure.Category.ShouldBe(ShardFailureCategory.UnknownEventType);
        state.Failure.Event.ShouldNotBeNull();
        state.Failure.Event.Sequence.ShouldBe(1623);
        state.Failure.Event.EventTypeName.ShouldBe("trip_started");
        state.Failure.Event.TenantId.ShouldBe("tenant-b");

        // ShardFailure.Detail is exactly what PauseReason has always carried, which is why the reason
        // text needed no column of its own.
        state.Failure.Detail.ShouldBe(state.PauseReason);
    }

    [Fact]
    public async Task a_healthy_shard_reports_no_failure()
    {
        await SeedProgressionRowAsync();

        await ((IEventDatabase)theDatabase).WriteExtendedProgressionAsync(
            WithoutFailure(ShardAction.Started, "Running"));

        var states = await theDatabase.AllProjectionProgress();
        states.Single(x => x.ShardName == TheShard).Failure.ShouldBeNull();
    }

    [Fact]
    public async Task never_inserts_a_row_and_never_touches_committed_progression()
    {
        await SeedProgressionRowAsync();

        await ((IEventDatabase)theDatabase).WriteExtendedProgressionAsync(
        [
            Paused(ShardFailureCategory.ApplyEvent, 7, "quest_started"),
            // A shard that has never committed progression has nowhere to record a reason: skipped
            // silently, exactly like every other extended-progression write.
            new ShardState("NoSuchProjection:All:98123456", 10)
            {
                Action = ShardAction.Paused, AgentStatus = "Paused", LastHeartbeat = DateTimeOffset.UtcNow
            }
        ]);

        await using var conn = await OpenConnectionAsync();

        var count = conn.CreateCommand();
        count.CommandText = $"SELECT COUNT(*) FROM {theDatabase.Events.ProgressionTableName};";
        Convert.ToInt64(await count.ExecuteScalarAsync()).ShouldBe(1);

        var seq = conn.CreateCommand();
        seq.CommandText =
            $"SELECT last_seq_id FROM {theDatabase.Events.ProgressionTableName} WHERE name = @name;";
        seq.Parameters.AddWithValue("@name", TheShard);
        Convert.ToInt64(await seq.ExecuteScalarAsync()).ShouldBe(10); // committed progress untouched
    }

    private async Task SeedProgressionRowAsync()
    {
        ConfigureStore(opts => opts.Events.EnableExtendedProgressionTracking = true);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        await using var conn = await OpenConnectionAsync();
        var insert = conn.CreateCommand();
        insert.CommandText =
            $"INSERT INTO {theDatabase.Events.ProgressionTableName} (name, last_seq_id) VALUES (@name, 10);";
        insert.Parameters.AddWithValue("@name", TheShard);
        await insert.ExecuteNonQueryAsync();
    }

    private async Task<(string? Category, long? Sequence, string? EventType, string? TenantId)> ReadFailureAsync()
    {
        await using var conn = await OpenConnectionAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT failure_category, failure_event_sequence, failure_event_type, failure_event_tenant_id
            FROM {theDatabase.Events.ProgressionTableName} WHERE name = @name;
            """;
        cmd.Parameters.AddWithValue("@name", TheShard);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return (null, null, null, null);

        return (
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetInt64(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    private static ShardState Paused(ShardFailureCategory category, long sequence, string eventTypeName,
        string? tenantId = null)
    {
        var failure = new ShardFailure
        {
            Category = category,
            ExceptionType = "Polecat.Exceptions.EventDeserializationFailureException",
            RootExceptionType = "System.DivideByZeroException",
            Message = "Boom!",
            Detail = "Polecat.Exceptions.EventDeserializationFailureException: Boom!\n   at Somewhere",
            OccurredAt = DateTimeOffset.UtcNow,
            Event = new EventFailureDetails
            {
                Sequence = sequence, EventTypeName = eventTypeName, TenantId = tenantId
            }
        };

        return new ShardState(TheShard, 10)
        {
            Action = ShardAction.Paused,
            AgentStatus = "Paused",
            PauseReason = failure.Detail,
            Failure = failure,
            LastHeartbeat = DateTimeOffset.UtcNow
        };
    }

    private static ShardState WithoutFailure(ShardAction action, string status)
    {
        return new ShardState(TheShard, 10)
        {
            Action = action, AgentStatus = status, LastHeartbeat = DateTimeOffset.UtcNow
        };
    }
}
