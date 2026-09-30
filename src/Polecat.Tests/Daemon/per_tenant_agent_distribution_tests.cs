using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon.Coordination;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #697, second half — the FAN-OUT. Under per-tenant event sequencing each tenant has its own
///     seq_id space, so one store-global agent per shard cannot represent the store. Everything needed
///     to expand a shard into per-tenant shards already existed on both sides (JasperFx's
///     <c>PerTenantShardExpansion</c>, Polecat's <c>ICrossTenantRebuildSource</c>); the store simply
///     answered <c>DistributesAgentsPerTenant = false</c> and the distributors were built through the
///     constructor overload that hardcodes the same. The result was one agent for the whole store and
///     no tenant's projections ever advancing.
/// </summary>
[Collection("tenant-partitioning")]
public class per_tenant_agent_distribution_tests : IAsyncLifetime
{
    private const string Schema = "pt_fanout";
    private static readonly string[] Tenants = ["Red", "Blue", "Green"];

    public async ValueTask InitializeAsync()
    {
        await DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore(bool partitioned)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = partitioned;
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.HotCold;
        });
    }

    /// <summary>
    ///     The property itself. Its interface default is <c>false</c>, and false is not a harmless
    ///     "unsupported" — it is the answer that makes Wolverine's <c>EventStoreAgents</c> and
    ///     JasperFx's <c>PerTenantShardExpansion</c> fan out store-globally.
    /// </summary>
    [Fact]
    public void the_store_says_it_distributes_per_tenant_exactly_when_events_are_sequenced_per_tenant()
    {
        using var partitioned = CreateStore(partitioned: true);
        ((IEventStore)partitioned).DistributesAgentsPerTenant.ShouldBeTrue();

        using var flat = CreateStore(partitioned: false);
        ((IEventStore)flat).DistributesAgentsPerTenant
            .ShouldBeFalse("conjoined without per-tenant sequencing keeps one global seq_id, " +
                           "where a single store-global agent is correct");

        // Not the same question as HasMultipleTenants, which is true for both of these.
        ((IEventStore)flat).HasMultipleTenants.ShouldBeTrue();
    }

    /// <summary>
    ///     The fan-out end to end: the distributor the coordinator actually builds hands out one shard
    ///     name PER TENANT, so the coordination loop starts a per-tenant agent for each.
    /// </summary>
    [Fact]
    public async Task the_distributor_expands_each_shard_into_one_agent_per_tenant()
    {
        using var store = CreateStore(partitioned: true);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        foreach (var tenant in Tenants)
        {
            await AppendAsync(store, tenant);
        }

        var coordinator = new ProjectionCoordinator(store);
        coordinator.Distributor.ShouldNotBeNull();

        var sets = await coordinator.Distributor!.BuildDistributionAsync();
        var names = sets.SelectMany(x => x.Names).ToList();

        // One projection, three tenants → three tenant-bearing names, not one store-global name.
        names.Count.ShouldBe(3);
        names.Select(x => x.TenantId).OrderBy(x => x)
            .ShouldBe(["Blue", "Green", "Red"]);

        // Lock granularity is deliberately unchanged: still one advisory lock per store-global shard,
        // with the winning node running all of that shard's tenant agents. Three tenant agents must
        // not become three independently-locked sets, or two nodes could each take a share of one
        // projection's tenants and neither would hold the projection.
        sets.Count.ShouldBe(1);
    }

    /// <summary>
    ///     The negative control, and the guarantee for every store that is NOT tenant-partitioned:
    ///     the shard names stay store-global and nothing about distribution changes.
    /// </summary>
    [Fact]
    public async Task a_store_without_per_tenant_sequencing_keeps_store_global_shard_names()
    {
        using var store = CreateStore(partitioned: false);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        await AppendAsync(store, "Red");
        await AppendAsync(store, "Blue");

        var coordinator = new ProjectionCoordinator(store);
        var sets = await coordinator.Distributor!.BuildDistributionAsync();
        var names = sets.SelectMany(x => x.Names).ToList();

        names.Count.ShouldBe(1);
        names.Single().TenantId.ShouldBeNull("a flat store's agent is not tenant-bearing");
    }

    /// <summary>
    ///     A tenant-partitioned store with no tenants yet must still produce its shard, rather than an
    ///     empty distribution that quietly coordinates nothing. Mirrors JasperFx's zero-tenant fallback
    ///     in <c>PerTenantShardExpansion</c> — there are simply no events to process until a tenant
    ///     appends, and the shard picks tenants up on the next leadership cycle without a restart.
    /// </summary>
    [Fact]
    public async Task a_partitioned_store_with_no_tenants_yet_still_offers_its_shard()
    {
        using var store = CreateStore(partitioned: true);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var coordinator = new ProjectionCoordinator(store);
        var sets = await coordinator.Distributor!.BuildDistributionAsync();

        sets.SelectMany(x => x.Names).Count().ShouldBe(1);
    }

    /// <summary>
    ///     #703 — the usage DESCRIPTOR has to list the tenants too, not just the distributor.
    /// </summary>
    /// <remarks>
    ///     A host using Wolverine-managed distribution never runs Polecat's own
    ///     <c>ProjectionCoordinator</c>, so #700's fan-out does not reach it. Wolverine's
    ///     <c>EventStoreAgents.SupportedAgentsAsync</c> instead gates on
    ///     <c>DistributesAgentsPerTenant &amp;&amp; database.TenantIds.Count > 0</c>, reading the ids
    ///     off this descriptor — so an empty list is indistinguishable from a store that does not
    ///     partition, and only the store-global agent starts.
    /// </remarks>
    [Fact]
    public async Task the_usage_descriptor_lists_the_tenants_it_distributes_over()
    {
        var token = TestContext.Current.CancellationToken;

        using var store = CreateStore(partitioned: true);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        foreach (var tenant in Tenants)
        {
            await AppendAsync(store, tenant);
        }

        // Control: the store knows its tenants. This is what the rebuild path already read, so a
        // failure below is the descriptor's alone.
        (await store.Database.FindRebuildTenantsAsync("QuestParty", token))
            .OrderBy(x => x).ShouldBe(["Blue", "Green", "Red"]);

        var eventStore = (IEventStore)store;
        eventStore.DistributesAgentsPerTenant.ShouldBeTrue();

        var usage = await eventStore.TryCreateUsage(token);

        DescribedTenants(usage!).ShouldBe(["Blue", "Green", "Red"],
            "the descriptor must list the tenants, or a Wolverine-managed host starts only the store-global agent");
    }

    /// <summary>
    ///     A tenant's partition exists only once it has appended, so the descriptor cannot be a
    ///     startup snapshot. Onboarding is the normal case, not an edge one.
    /// </summary>
    [Fact]
    public async Task the_descriptor_picks_up_a_tenant_onboarded_later()
    {
        var token = TestContext.Current.CancellationToken;

        using var store = CreateStore(partitioned: true);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await AppendAsync(store, "Red");

        var eventStore = (IEventStore)store;
        DescribedTenants((await eventStore.TryCreateUsage(token))!).ShouldBe(["Red"]);

        await AppendAsync(store, "Blue");

        DescribedTenants((await eventStore.TryCreateUsage(token))!)
            .ShouldBe(["Blue", "Red"], "a descriptor read after onboarding must include the new tenant");
    }

    /// <summary>
    ///     The negative control. A store that does not sequence events per tenant has no partitioned
    ///     tenants, and must not start claiming tenant ids it cannot distribute over — that would make
    ///     a Wolverine host fan out agents for a store whose daemon would then refuse the
    ///     tenant-bearing shard names.
    /// </summary>
    [Fact]
    public async Task a_store_without_per_tenant_sequencing_describes_no_tenants()
    {
        var token = TestContext.Current.CancellationToken;

        using var store = CreateStore(partitioned: false);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await AppendAsync(store, "Red");
        await AppendAsync(store, "Blue");

        DescribedTenants((await ((IEventStore)store).TryCreateUsage(token))!).ShouldBeEmpty();
    }

    private static List<string> DescribedTenants(JasperFx.Events.Descriptors.EventStoreUsage usage)
        => (usage.Database.MainDatabase?.TenantIds ?? [])
            .Concat(usage.Database.Databases.SelectMany(x => x.TenantIds))
            .Distinct()
            .OrderBy(x => x)
            .ToList();

    private static async Task AppendAsync(DocumentStore store, string tenant)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        session.Events.StartStream(Guid.NewGuid(),
            new QuestStarted($"{tenant} Quest"),
            new MembersJoined(1, $"{tenant} Town", [$"{tenant}Hero"]));
        await session.SaveChangesAsync();
    }

    private static async Task DropSchemaTablesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;

            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task DropSequencesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'DROP SEQUENCE [' + s.name + '].[' + sq.name + '];'
            FROM sys.sequences sq
            JOIN sys.schemas s ON sq.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }
}
