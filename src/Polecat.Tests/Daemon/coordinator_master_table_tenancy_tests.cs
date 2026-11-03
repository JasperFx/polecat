using JasperFx;
using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Events.Daemon.Coordination;
using Polecat.Projections;
using Polecat.Storage;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #847: master table tenancy is <see cref="JasperFx.Descriptors.DatabaseCardinality.DynamicMultiple" />,
///     and the coordinator used to branch on <c>StaticMultiple</c> only. It fell through to the
///     single-database distributor, whose <c>AllDatabases().Single()</c> threw on every leadership cycle
///     with none or several tenants, so no agent ever started.
/// </summary>
/// <remarks>
///     <c>coordinator_spins_up_agents_for_dynamically_added_tenant_databases</c> could not see this, because
///     it calls <c>StartAllAsync</c> on each tenant's daemon itself. Nothing here touches a daemon: the host
///     is started and the only evidence accepted is the projected document, so an agent has to have been
///     started by the coordinator's own loop.
/// </remarks>
public class coordinator_master_table_tenancy_tests : IClassFixture<CoordinatorTenantDatabasesFixture>
{
    private const string TenantA = "coord_tenant_a";
    private const string TenantB = "coord_tenant_b";

    internal static readonly string DbA = ConnectionSource.Scoped("coordmt_a");
    internal static readonly string DbB = ConnectionSource.Scoped("coordmt_b");
    internal static readonly string ControlDb = ConnectionSource.Scoped("coordmt_control");

    internal static readonly string[] AllDatabases = [DbA, DbB, ControlDb];

    private static readonly TimeSpan Timeout = 15.Seconds();

    [Theory]
    [InlineData(DaemonMode.Solo)]
    [InlineData(DaemonMode.HotCold)]
    public async Task agents_start_on_tenants_added_after_the_host_started(DaemonMode mode)
    {
        await using var node = new CoordinatedNode(mode);
        await node.StartAsync();

        await node.AddTenantAsync(TenantA, DbA);
        await node.AddTenantAsync(TenantB, DbB);

        await node.AssertProjectedAsync(TenantA);
        await node.AssertProjectedAsync(TenantB);
    }

    [Fact]
    public async Task agents_start_on_tenants_registered_before_the_host_started()
    {
        await using var node = new CoordinatedNode(DaemonMode.Solo);

        await node.AddTenantAsync(TenantA, DbA);
        await node.AddTenantAsync(TenantB, DbB);

        await node.StartAsync();

        await node.AssertProjectedAsync(TenantA);
        await node.AssertProjectedAsync(TenantB);
    }

    /// <summary>
    ///     The sharp end of the old failure. With exactly one tenant <c>Single()</c> was satisfied and
    ///     everything looked fine, so registering the SECOND tenant is what took the coordinator down.
    /// </summary>
    [Fact]
    public async Task a_second_tenant_joins_the_first_without_disturbing_it()
    {
        await using var node = new CoordinatedNode(DaemonMode.Solo);
        await node.StartAsync();

        await node.AddTenantAsync(TenantA, DbA);
        await node.AssertProjectedAsync(TenantA);

        await node.AddTenantAsync(TenantB, DbB);
        await node.AssertProjectedAsync(TenantB);
        await node.AssertProjectedAsync(TenantA);
    }

    /// <summary>
    ///     A tenant registered on this node lands in its cache directly, so it proves nothing about the
    ///     master table. One registered on ANOTHER node reaches this one only through the table, and only
    ///     while the coordinator keeps re-reading it. The first tenant is here to fill the cache, because
    ///     <c>AllDatabases()</c> falls back to the table while the cache is empty.
    /// </summary>
    [Fact]
    public async Task a_tenant_registered_by_another_node_is_found_in_the_master_table()
    {
        await using var node = new CoordinatedNode(DaemonMode.Solo);
        await node.StartAsync();

        await node.AddTenantAsync(TenantA, DbA);
        await node.AssertProjectedAsync(TenantA);

        using var otherNode = node.BuildStoreWithNoDaemon();
        await AddTenantAsync(otherNode, TenantB, DbB);

        // Only through the other node: touching this node's tenancy would put B in its cache.
        await AssertProjectedAsync(otherNode, TenantB);
    }

    /// <summary>
    ///     #847: <c>AddAsyncDaemon</c> read the tenant list once, so with no tenant at start it built no
    ///     daemon at all and nothing it could find later.
    /// </summary>
    [Theory]
    [InlineData(DaemonMode.Solo)]
    [InlineData(DaemonMode.HotCold)]
    public async Task add_async_daemon_finds_tenants_added_after_the_host_started(DaemonMode mode)
    {
        await using var node = new CoordinatedNode(mode, addAsyncDaemon: true);
        await node.StartAsync();

        await node.AddTenantAsync(TenantA, DbA);
        await node.AddTenantAsync(TenantB, DbB);

        await node.AssertProjectedAsync(TenantA);
        await node.AssertProjectedAsync(TenantB);
    }

    /// <summary>
    ///     #871 item 3, #849. Disabling is a row on the master table, and the coordinator re-reads that
    ///     table every cycle, so a node that never made the call still converges. The other tenant is
    ///     here to show the reaping is that specific, and re-enabling to show it is not a dead end.
    /// </summary>
    [Theory]
    [InlineData(DaemonMode.Solo)]
    [InlineData(DaemonMode.HotCold)]
    public async Task a_tenant_disabled_on_another_node_stops_running_here_until_it_is_enabled(DaemonMode mode)
    {
        var token = TestContext.Current.CancellationToken;

        await using var node = new CoordinatedNode(mode);
        await node.StartAsync();

        await node.AddTenantAsync(TenantA, DbA);
        await node.AddTenantAsync(TenantB, DbB);
        await node.AssertProjectedAsync(TenantA);
        await node.AssertProjectedAsync(TenantB);

        var daemonB = await node.DaemonForAsync(TenantB);
        daemonB.CurrentAgents().ShouldNotBeEmpty();

        using var otherNode = node.BuildStoreWithNoDaemon();
        var tenancy = (MasterTableTenancy)otherNode.Options.Tenancy!;
        await tenancy.DisableTenantAsync(TenantB, token);

        await WaitUntilAsync(() => daemonB.CurrentAgents().Count == 0, "tenant B's agents stopped on this node");
        Should.Throw<DisabledTenantException>(() => node.Store.Options.Tenancy!.GetDatabase(TenantB));
        await node.AssertProjectedAsync(TenantA);

        await tenancy.EnableTenantAsync(TenantB, token);
        await node.AssertProjectedAsync(TenantB);
    }

    [Fact]
    public async Task a_tenant_removed_on_another_node_stops_running_here()
    {
        var token = TestContext.Current.CancellationToken;

        await using var node = new CoordinatedNode(DaemonMode.Solo);
        await node.StartAsync();

        await node.AddTenantAsync(TenantA, DbA);
        await node.AssertProjectedAsync(TenantA);

        var daemon = await node.DaemonForAsync(TenantA);
        daemon.CurrentAgents().ShouldNotBeEmpty();

        using var otherNode = node.BuildStoreWithNoDaemon();
        await ((MasterTableTenancy)otherNode.Options.Tenancy!).DeleteDatabaseRecordAsync(TenantA, token);

        await WaitUntilAsync(() => daemon.CurrentAgents().Count == 0, "tenant A's agents stopped on this node");
    }

    /// <summary>
    ///     Stopping the agents is not enough under HotCold. The lock has to go too, or no other node could
    ///     run the database if the tenant came back.
    /// </summary>
    [Fact]
    public async Task a_hot_cold_node_lets_go_of_the_lock_on_a_tenant_it_evicts()
    {
        await using var node = new CoordinatedNode(DaemonMode.HotCold);
        await node.StartAsync();

        await node.AddTenantAsync(TenantA, DbA);
        await node.AssertProjectedAsync(TenantA);

        (await ApplicationLocksOnAsync(DbA)).ShouldBeGreaterThan(0);

        using var otherNode = node.BuildStoreWithNoDaemon();
        await ((MasterTableTenancy)otherNode.Options.Tenancy!).DisableTenantAsync(TenantA,
            TestContext.Current.CancellationToken);

        var deadline = DateTimeOffset.UtcNow.Add(Timeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await ApplicationLocksOnAsync(DbA) == 0) return;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException("The node stopped running the tenant and kept its lock.");
    }

    /// <summary>
    ///     #916. The distributor keeps one lock per database identifier for as long as it runs, and has no way
    ///     to forget one. That identifier is the tenant's, so a tenant back on another database found the lock
    ///     it left behind, and that lock went on connecting to where the tenant used to be.
    /// </summary>
    [Fact]
    public async Task a_tenant_back_on_another_database_takes_its_lock_there()
    {
        var token = TestContext.Current.CancellationToken;

        await using var node = new CoordinatedNode(DaemonMode.HotCold);
        await node.StartAsync();

        await node.AddTenantAsync(TenantA, DbA);
        await node.AssertProjectedAsync(TenantA);

        using var otherNode = node.BuildStoreWithNoDaemon();
        await ((MasterTableTenancy)otherNode.Options.Tenancy!).DeleteDatabaseRecordAsync(TenantA, token);

        var deadline = DateTimeOffset.UtcNow.Add(Timeout);
        while (await ApplicationLocksOnAsync(DbA) > 0)
        {
            DateTimeOffset.UtcNow.ShouldBeLessThan(deadline, "the node never let go of tenant A's lock");
            await Task.Delay(100, token);
        }

        await node.AddTenantAsync(TenantA, DbB);

        deadline = DateTimeOffset.UtcNow.Add(Timeout);
        while (await ApplicationLocksOnAsync(DbB) == 0)
        {
            DateTimeOffset.UtcNow.ShouldBeLessThan(deadline, "tenant A's lock never reached its new database");
            await Task.Delay(100, token);
        }

        (await ApplicationLocksOnAsync(DbA)).ShouldBe(0);
    }

    /// <summary>
    ///     #871 item 6. An application lock is per database, and the multi-database distributor used the same
    ///     id for every set, so the second of two event stores to reach a database they share lost to the
    ///     first for good and its projections never ran. Each node here has a schema of its own.
    /// </summary>
    [Fact]
    public async Task two_schemas_sharing_a_tenant_database_do_not_block_each_other()
    {
        await using var first = new CoordinatedNode(DaemonMode.HotCold);
        await using var second = new CoordinatedNode(DaemonMode.HotCold);

        await first.StartAsync();
        await second.StartAsync();

        await first.AddTenantAsync(TenantA, DbA);
        await second.AddTenantAsync(TenantA, DbA);

        await first.AssertProjectedAsync(TenantA);
        await second.AssertProjectedAsync(TenantA);
    }

    [Theory]
    [InlineData(DaemonMode.Solo)]
    [InlineData(DaemonMode.HotCold)]
    public void master_table_tenancy_gets_the_multi_database_distributor(DaemonMode mode)
    {
        using var store = DocumentStore.For(opts =>
        {
            // Never opened: building a coordinator reads no tenant.
            opts.ConnectionString =
                "Server=unused;Database=none;User Id=sa;Password=irrelevant;TrustServerCertificate=True";
            opts.MultiTenantedMasterTable(opts.ConnectionString);
            opts.DaemonSettings.AsyncMode = mode;
        });

        new ProjectionCoordinator(store).Distributor.ShouldBeOfType<MultiTenantedProjectionDistributor>();
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string description)
    {
        var deadline = DateTimeOffset.UtcNow.Add(Timeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException($"Timed out waiting until {description}");
    }

    private static async Task<int> ApplicationLocksOnAsync(string database)
    {
        var token = TestContext.Current.CancellationToken;

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.dm_tran_locks
            WHERE resource_type = 'APPLICATION' AND request_status = 'GRANT'
              AND resource_database_id = DB_ID(@database);
            """;
        cmd.Parameters.AddWithValue("@database", database);

        return (int)(await cmd.ExecuteScalarAsync(token))!;
    }

    private static Task AddTenantAsync(DocumentStore store, string tenantId, string database) =>
        ((MasterTableTenancy)store.Options.Tenancy!).AddDatabaseRecordAsync(tenantId,
            ConnectionSource.ConnectionStringFor(database), TestContext.Current.CancellationToken);

    // The whole of the evidence: an event appended to the tenant reaches its projection.
    private static async Task AssertProjectedAsync(DocumentStore store, string tenantId)
    {
        var token = TestContext.Current.CancellationToken;

        await WaitForTheCoordinatorToReachAsync(store, tenantId);

        var id = Guid.NewGuid();
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = tenantId }))
        {
            session.Events.StartStream(id, new CoordinatedTick());
            await session.SaveChangesAsync(token);
        }

        var deadline = DateTimeOffset.UtcNow.Add(Timeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            await using var query = store.QuerySession(new SessionOptions { TenantId = tenantId });
            var tally = await query.LoadAsync<CoordinatedTally>(id, token);
            if (tally?.Count == 1) return;

            await Task.Delay(100, token);
        }

        throw new TimeoutException($"Tenant '{tenantId}' was reached, but its event was never projected.");
    }

    // A tenant database starts with no tables and the coordinator's agent provisions them. Waiting for the
    // high-water row is what says it finished, so the first append does not race it for the same CREATE
    // TABLE. A hidden StartAllAsync could not satisfy this: nothing here calls one.
    private static async Task WaitForTheCoordinatorToReachAsync(DocumentStore store, string tenantId)
    {
        var token = TestContext.Current.CancellationToken;
        IEventDatabase database = store.Options.Tenancy!.GetDatabase(tenantId);

        var deadline = DateTimeOffset.UtcNow.Add(Timeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if ((await database.AllProjectionProgress(token)).Count > 0) return;

            await Task.Delay(50, token);
        }

        throw new TimeoutException(
            $"No high-water agent ran against tenant '{tenantId}' within {Timeout}: the coordinator never reached it.");
    }

    /// <summary>
    ///     A host that is always stopped. <see cref="IDisposable.Dispose" /> alone does not stop hosted
    ///     services, so a failed assertion would leave a coordinator polling and holding its tenant
    ///     database's lock against the next test, and one failure would read as all of them.
    /// </summary>
    private sealed class CoordinatedNode : IAsyncDisposable
    {
        // A schema per test, because pc_tenants lives in it and each test starts from its own tenants.
        private readonly string _schema = "coordmt_" + Guid.NewGuid().ToString("N")[..8];

        private readonly IHost _host;
        private readonly DocumentStore _store;
        private bool _started;

        public CoordinatedNode(DaemonMode mode, bool addAsyncDaemon = false)
        {
            _host = Host.CreateDefaultBuilder()
                .ConfigureServices(services =>
                {
                    var polecat = services.AddPolecat(Configure);

                    if (addAsyncDaemon)
                    {
                        polecat.AddAsyncDaemon(mode);
                    }
                    else
                    {
                        polecat.AddProjectionCoordinator(mode);
                    }
                })
                .Build();

            _store = (DocumentStore)_host.Services.GetRequiredService<IDocumentStore>();
        }

        private void Configure(StoreOptions opts)
        {
            opts.DatabaseSchemaName = _schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.MultiTenantedMasterTable(ConnectionSource.ConnectionStringFor(ControlDb), _schema);
            opts.DaemonSettings.LeadershipPollingTime = 100;
            opts.Projections.Add<CoordinatedTallyProjection>(ProjectionLifecycle.Async);
        }

        public async Task StartAsync()
        {
            await _host.StartAsync(TestContext.Current.CancellationToken);
            _started = true;
        }

        public DocumentStore Store => _store;

        public DocumentStore BuildStoreWithNoDaemon() => DocumentStore.For(Configure);

        public ValueTask<IProjectionDaemon> DaemonForAsync(string tenantId) =>
            _host.Services.GetRequiredService<JasperFx.Events.Daemon.IProjectionCoordinator>()
                .DaemonForDatabase(_store.Options.Tenancy!.GetDatabase(tenantId).Identifier);

        public Task AddTenantAsync(string tenantId, string database) =>
            coordinator_master_table_tenancy_tests.AddTenantAsync(_store, tenantId, database);

        public Task AssertProjectedAsync(string tenantId) =>
            coordinator_master_table_tenancy_tests.AssertProjectedAsync(_store, tenantId);

        public async ValueTask DisposeAsync()
        {
            if (_started)
            {
                await _host.StopAsync(CancellationToken.None);
            }

            _host.Dispose();
        }
    }
}

public record CoordinatedTick;

public class CoordinatedTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class CoordinatedTallyProjection : SingleStreamProjection<CoordinatedTally, Guid>
{
    public override CoordinatedTally? Evolve(CoordinatedTally? snapshot, Guid id, IEvent e)
    {
        snapshot ??= new CoordinatedTally { Id = id };
        if (e.Data is CoordinatedTick) snapshot.Count++;
        return snapshot;
    }
}

public class CoordinatorTenantDatabasesFixture : IAsyncLifetime
{
    public async ValueTask InitializeAsync()
    {
        await DropAsync();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in coordinator_master_table_tenancy_tests.AllDatabases)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync();
        }

        SqlConnection.ClearAllPools();
    }

    public ValueTask DisposeAsync() => new(DropAsync());

    private static async Task DropAsync()
    {
        SqlConnection.ClearAllPools();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in coordinator_master_table_tenancy_tests.AllDatabases)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF DB_ID('{db}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{db}];
                END
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
