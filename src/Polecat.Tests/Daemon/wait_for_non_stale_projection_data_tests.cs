using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Internal.Operations;
using Polecat.Projections;
using Polecat.Services;
using Polecat.Subscriptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     polecat#602: <see cref="Polecat.Storage.PolecatDatabase.WaitForNonStaleProjectionDataAsync" />
///     used to gate on "at least one progression row exists and every row present is caught up".
///     A shard has no progression row at all until it commits its first batch, so a store with two
///     async projections spends a window where the first shard has reported at the high-water mark
///     and the second has written nothing — and that window satisfies the old condition. The wait
///     returned while a projection had not run, and the caller's next read saw a document that was
///     never written.
/// </summary>
/// <remarks>
///     <para>
///         That is exactly the intermittent CI failure in
///         <c>rebuild_and_catch_up_compliance.rebuilding_one_projection_leaves_another_alone</c>,
///         whose store registers two async snapshots over the same events. It reproduced 4 runs in 5
///         under six concurrent worker processes and never once on an idle box, because the window is
///         only as wide as the gap between the two shards' commits.
///     </para>
///     <para>
///         The tests below plant progression rows directly rather than racing a daemon, so the
///         condition under test is deterministic: the partial-progress state is constructed, not
///         waited for.
///     </para>
/// </remarks>
public class wait_for_non_stale_projection_data_tests : OneOffConfigurationsContext
{
    private static readonly TimeSpan _shortTimeout = TimeSpan.FromSeconds(3);

    private async Task WithTwoAsyncProjectionsAsync()
    {
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "wait_non_stale";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Projections.Snapshot<WaitTurbine>(SnapshotLifecycle.Async);
            opts.Projections.Snapshot<WaitTurbineAudit>(SnapshotLifecycle.Async);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Advanced.Clean.DeleteAllEventDataAsync();
        await ClearProgressionAsync();
    }

    /// <summary>
    ///     The regression. One of the two shards is fully caught up and the other has never reported;
    ///     on the old predicate the wait returned immediately.
    /// </summary>
    [Fact]
    public async Task does_not_return_while_a_configured_shard_has_never_reported()
    {
        await WithTwoAsyncProjectionsAsync();

        var highWater = await AppendAStreamAsync();

        await SeedAsync(ShardFor<WaitTurbine>(), highWater);

        var ex = await Should.ThrowAsync<TimeoutException>(() =>
            theDatabase.WaitForNonStaleProjectionDataAsync(_shortTimeout));

        // ...and the message names the shard that never reported, which is the whole point of
        // reaching the timeout rather than silently returning.
        ex.Message.ShouldContain(nameof(WaitTurbineAudit));
        ex.Message.ShouldContain("no progress recorded");
    }

    [Fact]
    public async Task returns_once_every_configured_shard_has_reported()
    {
        await WithTwoAsyncProjectionsAsync();

        var highWater = await AppendAStreamAsync();

        await SeedAsync(ShardFor<WaitTurbine>(), highWater);
        await SeedAsync(ShardFor<WaitTurbineAudit>(), highWater);

        await theDatabase.WaitForNonStaleProjectionDataAsync(_shortTimeout);
    }

    /// <summary>
    ///     The pre-existing guarantee, unchanged: a row that IS present but behind still blocks.
    /// </summary>
    [Fact]
    public async Task does_not_return_while_a_shard_that_reported_is_behind()
    {
        await WithTwoAsyncProjectionsAsync();

        var highWater = await AppendAStreamAsync();

        await SeedAsync(ShardFor<WaitTurbine>(), highWater);
        await SeedAsync(ShardFor<WaitTurbineAudit>(), highWater - 1);

        await Should.ThrowAsync<TimeoutException>(() =>
            theDatabase.WaitForNonStaleProjectionDataAsync(_shortTimeout));
    }

    /// <summary>
    ///     A per-tenant shard writes its progression under a tenant-qualified identity
    ///     (<c>Name:All:tenant</c>), which is not the configured shard's identity string. Matching on
    ///     the identity would hang here; matching structurally on (name, shard key, version) does not.
    /// </summary>
    [Fact]
    public async Task a_tenant_qualified_row_satisfies_its_configured_shard()
    {
        await WithTwoAsyncProjectionsAsync();

        var highWater = await AppendAStreamAsync();

        await SeedAsync(ShardFor<WaitTurbine>().ForTenant("acme"), highWater);
        await SeedAsync(ShardFor<WaitTurbineAudit>().ForTenant("acme"), highWater);

        await theDatabase.WaitForNonStaleProjectionDataAsync(_shortTimeout);
    }

    /// <summary>
    ///     A <c>CompositeProjection</c> carries <c>Version = 0</c> on its shard while persisting the
    ///     unversioned identity <c>Name:All</c>, which parses back as version 1. Comparing the parsed
    ///     shard's fields therefore never matches a composite's own progression row, and the wait
    ///     hangs its full timeout over a projection that is caught up.
    /// </summary>
    [Fact]
    public async Task a_composite_projections_own_shard_is_matched_to_its_row()
    {
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "wait_non_stale_composite";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Projections.CompositeProjectionFor("WaitComposite", composite =>
            {
                composite.Snapshot<WaitTurbine>();
            });
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Advanced.Clean.DeleteAllEventDataAsync();
        await ClearProgressionAsync();

        var composite = theStore.Options.Projections.AllShards()
            .Single(x => x.Name.Name == "WaitComposite").Name;
        composite.Version.ShouldBe(0u, "the trap this test exists for is gone if the version is 1");

        var highWater = await AppendAStreamAsync();
        await SeedAsync(composite, highWater);

        await theDatabase.WaitForNonStaleProjectionDataAsync(_shortTimeout);
    }

    /// <summary>
    ///     Nothing runs asynchronously, so nothing can be stale. The old predicate could never satisfy
    ///     <c>Count > 0</c> here and timed out instead.
    /// </summary>
    [Fact]
    public async Task returns_immediately_when_no_async_projections_are_registered()
    {
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "wait_non_stale_none";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        await AppendAStreamAsync();

        await theDatabase.WaitForNonStaleProjectionDataAsync(_shortTimeout);
    }

    /// <summary>
    ///     A subscription writes no queryable document, but it is still part of what this wait
    ///     covers: callers wait on it to know a subscription has seen a range, and
    ///     <c>subscription_under_coordinator_tests</c> registers a subscription and nothing else.
    ///     Dropping subscriptions from the expected set makes the wait return before one has run.
    /// </summary>
    [Fact]
    public async Task a_subscription_is_part_of_what_the_wait_covers()
    {
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "wait_non_stale_subscription";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Projections.Snapshot<WaitTurbine>(SnapshotLifecycle.Async);
            opts.Projections.Subscribe(new WaitRecordingSubscription());
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Advanced.Clean.DeleteAllEventDataAsync();
        await ClearProgressionAsync();

        var highWater = await AppendAStreamAsync();

        // The projection has reported; the subscription has not. That must not be enough.
        await SeedAsync(ShardFor<WaitTurbine>(), highWater);

        var ex = await Should.ThrowAsync<TimeoutException>(() =>
            theDatabase.WaitForNonStaleProjectionDataAsync(_shortTimeout));
        ex.Message.ShouldContain(nameof(WaitRecordingSubscription));

        // ...and once it has, the wait completes.
        var subscription = theStore.Options.Projections.AllShards()
            .Single(x => x.Name.Name == nameof(WaitRecordingSubscription)).Name;
        await SeedAsync(subscription, highWater);

        await theDatabase.WaitForNonStaleProjectionDataAsync(_shortTimeout);
    }

    private ShardName ShardFor<T>() => theStore.Options.Projections.AllShards()
        .Single(x => x.Name.Name == typeof(T).Name).Name;

    private async Task<long> AppendAStreamAsync()
    {
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<WaitTurbine>(Guid.NewGuid(),
            new WaitTurbineInstalled("North Ridge"),
            new WaitTurbineSpun(10),
            new WaitTurbineSpun(15));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return await theDatabase.FetchHighestEventSequenceNumber(TestContext.Current.CancellationToken);
    }

    private async Task ClearProgressionAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM {theStore.Events.ProgressionTableName};";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    // Seed through the production write path, same as progression_delete_scoping_tests.
    private async Task SeedAsync(ShardName shardName, long ceiling)
    {
        var events = theStore.Database.Events;
        var op = new RecordProgressionOperation(
            events.ProgressionTableName,
            shardName.Identity,
            ceiling,
            events.EnableExtendedProgressionTracking,
            upsert: true);

        await using var conn = await OpenConnectionAsync();
        await using var batch = new Microsoft.Data.SqlClient.SqlBatch(conn);
        var builder = new Weasel.SqlServer.BatchBuilder(batch);
        op.ConfigureCommand(builder);
        builder.Compile();
        await using var reader = await batch.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        await op.PostprocessAsync(reader, new List<Exception>(), TestContext.Current.CancellationToken);
    }
}

public record WaitTurbineInstalled(string Site);

public record WaitTurbineSpun(int Revolutions);

public partial class WaitTurbine
{
    public Guid Id { get; set; }
    public string Site { get; set; } = string.Empty;
    public int TotalRevolutions { get; set; }

    public static WaitTurbine Create(WaitTurbineInstalled e) => new() { Site = e.Site };

    public void Apply(WaitTurbineSpun e) => TotalRevolutions += e.Revolutions;
}

public partial class WaitTurbineAudit
{
    public Guid Id { get; set; }
    public string Site { get; set; } = string.Empty;
    public int Revisions { get; set; }

    public static WaitTurbineAudit Create(WaitTurbineInstalled e) => new() { Site = e.Site, Revisions = 1 };

    public void Apply(WaitTurbineSpun _) => Revisions++;
}

/// <summary>
///     Registered so the wait has a subscription shard to cover. Its progression rows are planted
///     directly, so it is never actually run — see <c>a_subscription_is_part_of_what_the_wait_covers</c>.
/// </summary>
public class WaitRecordingSubscription : SubscriptionBase
{
    public override Task<IChangeListener> ProcessEventsAsync(EventRange page, ISubscriptionController controller,
        IDocumentOperations operations, CancellationToken cancellationToken)
        => throw new NotSupportedException("This subscription is never started; its progress is planted.");
}
