using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon.Coordination;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #916. Under <c>UseTenantPartitionedEvents</c> an async shard writes one progression row per tenant,
///     <c>Name:All:tenant</c>, and each tenant's <c>seq_id</c> counts from 1 by itself. Neither wait knew
///     that. <c>QueryForNonStaleData</c> looked for the row called <c>Name:All</c>, which is never written,
///     and both compared each row against the highest sequence in the store, which is another tenant's.
/// </summary>
[Collection("tenant-partitioning")]
public class non_stale_per_tenant_tests : IAsyncLifetime
{
    private const string Schema = "pt_non_stale";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async Task<DocumentStore> CreateStoreAsync()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.HotCold;
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        return store;
    }

    private static async Task StartQuestsAsync(DocumentStore store, string tenant, int count)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        for (var i = 0; i < count; i++)
        {
            session.Events.StartStream(Guid.NewGuid(), new QuestStarted($"{tenant}-{i}"));
        }

        await session.SaveChangesAsync(Ct);
    }

    private static async Task<ProjectionCoordinator> StartDaemonAsync(DocumentStore store)
    {
        var coordinator = new ProjectionCoordinator(store, NullLoggerFactory.Instance);
        await coordinator.StartAsync(Ct);
        return coordinator;
    }

    [Fact]
    public async Task a_query_waits_for_its_own_tenant()
    {
        using var store = await CreateStoreAsync();
        await StartQuestsAsync(store, "Red", 3);

        var coordinator = await StartDaemonAsync(store);
        try
        {
            await using var query = store.QuerySession("Red");
            var parties = await query.Query<QuestParty>()
                .QueryForNonStaleData(TimeSpan.FromSeconds(20))
                .ToListAsync(Ct);

            parties.Count.ShouldBe(3);
        }
        finally
        {
            await coordinator.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>
    ///     Blue's sequence stops well short of Red's, so a comparison against the store's highest would
    ///     never be satisfied, for all that Blue's projection is finished.
    /// </summary>
    [Fact]
    public async Task a_query_is_not_held_to_a_taller_tenants_sequence()
    {
        using var store = await CreateStoreAsync();
        await StartQuestsAsync(store, "Red", 6);
        await StartQuestsAsync(store, "Blue", 2);

        var coordinator = await StartDaemonAsync(store);
        try
        {
            await using var query = store.QuerySession("Blue");
            var parties = await query.Query<QuestParty>()
                .QueryForNonStaleData(TimeSpan.FromSeconds(20))
                .ToListAsync(Ct);

            parties.Count.ShouldBe(2);
        }
        finally
        {
            await coordinator.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task a_tenant_with_no_events_has_nothing_to_wait_for()
    {
        using var store = await CreateStoreAsync();
        await StartQuestsAsync(store, "Red", 1);

        await using var query = store.QuerySession("Nobody");
        var parties = await query.Query<QuestParty>()
            .QueryForNonStaleData(TimeSpan.FromSeconds(5))
            .ToListAsync(Ct);

        parties.ShouldBeEmpty();
    }

    [Fact]
    public async Task a_query_still_times_out_when_its_tenant_is_behind()
    {
        using var store = await CreateStoreAsync();
        await StartQuestsAsync(store, "Red", 1);

        // No daemon, so nothing will ever catch up
        await using var query = store.QuerySession("Red");
        await Should.ThrowAsync<TimeoutException>(() => query.Query<QuestParty>()
            .QueryForNonStaleData(TimeSpan.FromSeconds(1))
            .ToListAsync(Ct));
    }

    [Fact]
    public async Task the_store_wide_wait_holds_each_tenant_to_its_own_sequence()
    {
        using var store = await CreateStoreAsync();
        await StartQuestsAsync(store, "Red", 6);
        await StartQuestsAsync(store, "Blue", 2);

        var coordinator = await StartDaemonAsync(store);
        try
        {
            await store.Database.WaitForNonStaleProjectionDataAsync(TimeSpan.FromSeconds(20));
        }
        finally
        {
            await coordinator.StopAsync(CancellationToken.None);
        }

        await using var blue = store.QuerySession("Blue");
        (await blue.Query<QuestParty>().CountAsync(Ct)).ShouldBe(2);
    }

    [Fact]
    public async Task the_store_wide_wait_does_not_let_one_tenant_answer_for_another()
    {
        using var store = await CreateStoreAsync();
        await StartQuestsAsync(store, "Red", 2);
        await StartQuestsAsync(store, "Blue", 2);

        var coordinator = await StartDaemonAsync(store);
        try
        {
            await store.Database.WaitForNonStaleProjectionDataAsync(TimeSpan.FromSeconds(20));

            // Blue's stream of work is already done, and Red has more on the way
            await StartQuestsAsync(store, "Red", 3);
            await store.Database.WaitForNonStaleProjectionDataAsync(TimeSpan.FromSeconds(20));
        }
        finally
        {
            await coordinator.StopAsync(CancellationToken.None);
        }

        await using var red = store.QuerySession("Red");
        (await red.Query<QuestParty>().CountAsync(Ct)).ShouldBe(5);
    }
}
