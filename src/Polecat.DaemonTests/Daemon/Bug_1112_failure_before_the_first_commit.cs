using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal.Operations;
using Polecat.Subscriptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

public record EarlyPauseWeighed(bool Poison);

public partial class EarlyPauseWeighIn
{
    // Static because the daemon builds the aggregate itself. Only Bug_1112_failure_before_the_first_commit flips it,
    // and the tests of one class never run in parallel.
    public static volatile bool PoisonFails = true;

    public Guid Id { get; set; }
    public int Count { get; set; }

    public void Apply(EarlyPauseWeighed e)
    {
        if (e.Poison && PoisonFails) throw new InvalidOperationException("Can't weigh this one");
        Count++;
    }
}

public class EarlyFailingSubscription : SubscriptionBase
{
    public EarlyFailingSubscription()
    {
        Name = "EarlyFailing";
    }

    public volatile bool Fails = true;

    public List<long> Processed { get; } = [];

    public override Task<IChangeListener> ProcessEventsAsync(EventRange page, ISubscriptionController controller,
        IDocumentOperations operations, CancellationToken cancellationToken)
    {
        if (Fails) throw new InvalidOperationException("Not today");

        lock (Processed)
        {
            Processed.AddRange(page.Events.Select(x => x.Sequence));
        }

        return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
    }
}

/// <summary>
///     #1112, the port of marten#6127 (and fisher#660): a shard that fails before its first commit has no progression
///     row, so its failure was written nowhere. It now gets a row at the position it started from, which leaves where
///     it starts next time alone.
/// </summary>
public class Bug_1112_failure_before_the_first_commit : OneOffConfigurationsContext
{
    private const string TheShard = "EarlyPauseWeighIn:All";
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(60);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public Bug_1112_failure_before_the_first_commit()
    {
        EarlyPauseWeighIn.PoisonFails = true;
    }

    public override async ValueTask DisposeAsync()
    {
        EarlyPauseWeighIn.PoisonFails = true;
        await base.DisposeAsync();
    }

    private async Task buildStorageAsync(Action<StoreOptions>? alter = null, bool withTheSnapshot = true)
    {
        ConfigureStore(opts =>
        {
            opts.Events.EnableExtendedProgressionTracking = true;
            if (withTheSnapshot) opts.Projections.Snapshot<EarlyPauseWeighIn>(SnapshotLifecycle.Async);
            alter?.Invoke(opts);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private static ShardState paused(string shard, long sequence, ShardFailureCategory category)
    {
        var failure = new ShardFailure
        {
            Category = category,
            ExceptionType = "JasperFx.Events.Daemon.ParkedLimitReachedException",
            RootExceptionType = "JasperFx.Events.Daemon.ParkedLimitReachedException",
            Message = "Paused instead of parking",
            Detail = "JasperFx.Events.Daemon.ParkedLimitReachedException: Paused instead of parking",
            OccurredAt = DateTimeOffset.UtcNow,
            Event = new EventFailureDetails { Sequence = 2, EventTypeName = "early_pause_weighed" }
        };

        return new ShardState(shard, sequence)
        {
            Action = ShardAction.Paused,
            AgentStatus = "Paused",
            PauseReason = failure.Detail,
            Failure = failure,
            LastHeartbeat = DateTimeOffset.UtcNow
        };
    }

    // The first commit of a shard that starts at 0, exactly as the daemon's projection batch writes it
    private async Task commitFromZeroAsync(long ceiling) => await executeAsync(
        (RecordProgressionOperation)theDatabase.Events.UpdateProgressOperation(TheShard, ceiling, upsert: true));

    private async Task executeAsync(RecordProgressionOperation op)
    {
        await using var conn = await OpenConnectionAsync();
        await using var batch = new SqlBatch(conn);
        var builder = new Weasel.SqlServer.BatchBuilder(batch);
        op.ConfigureCommand(builder);
        builder.Compile();
        await using var reader = await batch.ExecuteReaderAsync(Ct);
        await op.PostprocessAsync(reader, new List<Exception>(), Ct);
    }

    private async Task<ShardState?> progressionAsync(string shard)
        => (await theDatabase.AllProjectionProgress(Ct)).SingleOrDefault(x => x.ShardName == shard);

    // AllProjectionProgress does not read pause_reason back into a failure, so the row is read as it is stored
    private async Task<(long Sequence, string? PauseReason, string? Category, long? EventSequence, string? EventType)?>
        rowAsync(string shard)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT last_seq_id, pause_reason, failure_category, failure_event_sequence, failure_event_type
            FROM {theDatabase.Events.ProgressionTableName} WHERE name = @name
            """;
        cmd.Parameters.AddWithValue("@name", shard);

        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        if (!await reader.ReadAsync(Ct)) return null;

        return (reader.GetInt64(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetString(4));
    }

    private async Task<ShardState> waitForTheFailureAsync(string shard)
    {
        var deadline = DateTimeOffset.UtcNow.Add(Generous);
        while (true)
        {
            var state = await progressionAsync(shard);
            if (state?.Failure != null) return state;

            DateTimeOffset.UtcNow.ShouldBeLessThan(deadline, $"The failure of {shard} was never written");
            await Task.Delay(100, Ct);
        }
    }

    private async Task appendAsync(int count)
    {
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(),
            Enumerable.Range(0, count).Select(_ => (object)new EarlyPauseWeighed(false)).ToArray());
        await session.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task a_failure_with_no_row_creates_the_row_at_the_start_position()
    {
        await buildStorageAsync();

        await theDatabase.WriteExtendedProgressionAsync(paused(TheShard, 0, ShardFailureCategory.ParkedLimitReached), Ct);

        var state = (await progressionAsync(TheShard)).ShouldNotBeNull();
        state.Sequence.ShouldBe(0);
        state.AgentStatus.ShouldBe("Paused");
        state.Failure.ShouldNotBeNull().Category.ShouldBe(ShardFailureCategory.ParkedLimitReached);
        state.Failure.Event.ShouldNotBeNull().Sequence.ShouldBe(2);
        state.Failure.Event.EventTypeName.ShouldBe("early_pause_weighed");
    }

    [Fact]
    public async Task a_failure_with_no_row_uses_the_floor_the_shard_started_from()
    {
        await buildStorageAsync();

        await theDatabase.WriteExtendedProgressionAsync(paused(TheShard, 7, ShardFailureCategory.ApplyEvent), Ct);

        var state = (await progressionAsync(TheShard)).ShouldNotBeNull();
        state.Sequence.ShouldBe(7);
        state.Failure.ShouldNotBeNull().Category.ShouldBe(ShardFailureCategory.ApplyEvent);
    }

    [Fact]
    public async Task a_state_without_a_failure_still_creates_no_row()
    {
        await buildStorageAsync();

        await theDatabase.WriteExtendedProgressionAsync(
            new ShardState(TheShard, 0) { Action = ShardAction.Paused, AgentStatus = "Paused", PauseReason = "asked" },
            Ct);

        (await progressionAsync(TheShard)).ShouldBeNull();
    }

    [Fact]
    public async Task a_failure_never_moves_a_committed_position()
    {
        await buildStorageAsync();
        await commitFromZeroAsync(25);

        await theDatabase.WriteExtendedProgressionAsync(paused(TheShard, 3, ShardFailureCategory.ApplyEvent), Ct);

        var state = (await progressionAsync(TheShard)).ShouldNotBeNull();
        state.Sequence.ShouldBe(25);
        state.Failure.ShouldNotBeNull().Category.ShouldBe(ShardFailureCategory.ApplyEvent);
    }

    [Fact]
    public async Task writing_the_same_failure_again_changes_nothing()
    {
        await buildStorageAsync();
        var state = paused(TheShard, 0, ShardFailureCategory.ParkedLimitReached);

        await theDatabase.WriteExtendedProgressionAsync(state, Ct);
        await theDatabase.WriteExtendedProgressionAsync(state, Ct);

        (await theDatabase.AllProjectionProgress(Ct)).Count(x => x.ShardName == TheShard).ShouldBe(1);
        (await progressionAsync(TheShard)).ShouldNotBeNull().Failure.ShouldNotBeNull().Category
            .ShouldBe(ShardFailureCategory.ParkedLimitReached);
    }

    [Fact]
    public async Task the_first_commit_takes_over_the_failure_row_at_zero_and_clears_the_failure()
    {
        await buildStorageAsync();
        await theDatabase.WriteExtendedProgressionAsync(paused(TheShard, 0, ShardFailureCategory.ApplyEvent), Ct);
        (await rowAsync(TheShard)).ShouldNotBeNull().Category.ShouldBe(nameof(ShardFailureCategory.ApplyEvent));

        // Polecat's first commit is a MERGE, so a row already at 0 is matched rather than collided with
        await commitFromZeroAsync(12);

        var row = (await rowAsync(TheShard)).ShouldNotBeNull();
        row.Sequence.ShouldBe(12);
        row.PauseReason.ShouldBeNull();
        row.Category.ShouldBeNull();
        row.EventSequence.ShouldBeNull();
        row.EventType.ShouldBeNull();

        (await progressionAsync(TheShard)).ShouldNotBeNull().Failure.ShouldBeNull();
    }

    [Fact]
    public async Task a_shard_paused_at_the_parking_cap_on_its_first_batch_records_it_and_starts_from_zero_again()
    {
        await buildStorageAsync(opts => opts.Projections.Errors.MaxParkedIdentitiesPerShard = 1);
        ((IEventStore)theStore).RegisteredShardNames().Single().Identity.ShouldBe(TheShard);

        var first = Guid.NewGuid();
        var second = Guid.NewGuid();

        // Two failing identities in the shard's first batch pass a cap of one, before anything has committed
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<EarlyPauseWeighIn>(first, new EarlyPauseWeighed(true));
            session.Events.StartStream<EarlyPauseWeighIn>(second, new EarlyPauseWeighed(true));
            await session.SaveChangesAsync(Ct);
        }

        using (var daemon = await theStore.BuildProjectionDaemonAsync())
        {
            await daemon.StartAllAsync();

            var state = await waitForTheFailureAsync(TheShard);
            state.Sequence.ShouldBe(0);
            state.Failure!.Category.ShouldBe(ShardFailureCategory.ParkedLimitReached);

            await daemon.StopAllAsync();
        }

        // The projection is fixed. The shard starts where it would have without the row, from the beginning.
        EarlyPauseWeighIn.PoisonFails = false;

        using (var daemon = await theStore.BuildProjectionDaemonAsync())
        {
            await daemon.StartAllAsync();
            await theDatabase.WaitForNonStaleProjectionDataAsync(Generous);
            await daemon.StopAllAsync();
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<EarlyPauseWeighIn>(first, Ct)).ShouldNotBeNull().Count.ShouldBe(1);
        (await query.LoadAsync<EarlyPauseWeighIn>(second, Ct)).ShouldNotBeNull().Count.ShouldBe(1);

        var progress = (await progressionAsync(TheShard)).ShouldNotBeNull();
        progress.Sequence.ShouldBe(2);
        progress.Failure.ShouldBeNull();
    }

    [Fact]
    public async Task a_from_present_subscription_that_fails_before_its_first_commit_starts_from_present_after_a_restart()
    {
        var subscription = new EarlyFailingSubscription();
        subscription.Options.SubscribeFromPresent();

        await buildStorageAsync(opts => opts.Projections.Subscribe(subscription), withTheSnapshot: false);

        // Started on an empty store, so "present" is 0 and the subscription has no row
        using (var daemon = await theStore.BuildProjectionDaemonAsync())
        {
            await daemon.StartAllAsync();

            await appendAsync(2);

            var state = await waitForTheFailureAsync("EarlyFailing:All");
            state.Sequence.ShouldBe(0);

            await daemon.StopAllAsync();
        }

        subscription.Fails = false;

        using (var daemon = await theStore.BuildProjectionDaemonAsync())
        {
            await daemon.StartAllAsync();

            await appendAsync(1);
            await theDatabase.WaitForNonStaleProjectionDataAsync(Generous);

            await daemon.StopAllAsync();
        }

        // Only the event appended after the restart: it started from the present (2), not from the row at 0
        subscription.Processed.ShouldBe([3L]);
        (await progressionAsync("EarlyFailing:All")).ShouldNotBeNull().Sequence.ShouldBe(3);
    }

    [Fact]
    public async Task a_from_present_subscription_keeps_its_present_mark_when_it_fails_before_its_first_commit()
    {
        var subscription = new EarlyFailingSubscription();
        subscription.Options.SubscribeFromPresent();

        await buildStorageAsync(opts => opts.Projections.Subscribe(subscription), withTheSnapshot: false);

        await appendAsync(3);

        using var daemon = await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        await appendAsync(1);

        var state = await waitForTheFailureAsync("EarlyFailing:All");
        state.Sequence.ShouldBe(3);

        await daemon.StopAllAsync();
    }
}
