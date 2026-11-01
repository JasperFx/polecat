using JasperFx;
using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
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

        public CoordinatedNode(DaemonMode mode)
        {
            _host = Host.CreateDefaultBuilder()
                .ConfigureServices(services => services.AddPolecat(Configure).AddProjectionCoordinator(mode))
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

        public DocumentStore BuildStoreWithNoDaemon() => DocumentStore.For(Configure);

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
