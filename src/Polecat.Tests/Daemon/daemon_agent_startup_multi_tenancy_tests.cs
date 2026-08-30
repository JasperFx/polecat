using JasperFx;
using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Events.Daemon;
using IProjectionCoordinator = Polecat.Events.Daemon.Coordination.IProjectionCoordinator;
using Polecat.Exceptions;
using Polecat.Internal;
using Polecat.Storage;
using Polecat.TestUtils;
using Weasel.Core;

namespace Polecat.Tests.Daemon;

/// <summary>
///     The async daemon must actually START AGENTS on every tenant database — not merely construct
///     a daemon object. Polecat had no coverage of this: it is the difference between "the host came
///     up" and "tenant B's projections are running", and the two looked identical from the outside
///     while tenant B was silently never processed. polecat#514.
///     <para>
///     Mirrors Marten's <c>DaemonTests/MultiTenancy/multi_tenancy_by_database.cs</c> shard-running
///     assertions and <c>dynamic_spin_up_of_dynamic_tenants.cs</c>.
///     </para>
/// </summary>
public class daemon_agent_startup_multi_tenancy_tests : IClassFixture<AgentStartupDatabasesFixture>
{
    internal const string TenantA = "agent_tenant_a";
    internal const string TenantB = "agent_tenant_b";

    internal static readonly string DbA = ConnectionSource.Scoped("agentstart_a");
    internal static readonly string DbB = ConnectionSource.Scoped("agentstart_b");
    internal static readonly string ControlDb = ConnectionSource.Scoped("agentstart_control");

    internal static readonly string[] AllDatabases = [DbA, DbB, ControlDb];

    private const string Schema = "agentstart";

    private static string ConnectionFor(string db) => ConnectionSource.ConnectionStringFor(db);

    private static readonly TimeSpan Timeout = 30.Seconds();

    // The shard every one of these stores registers. AgentTallyProjection => "AgentTally:All".
    private const string ShardName = "AgentTally:All";

    // -------------------------------------------------------------------------------------
    // Static database-per-tenant — with and without a default tenant registered
    // -------------------------------------------------------------------------------------

    /// <summary>
    ///     The supported shape: no "*DEFAULT*" tenant anywhere. Every tenant database must get its
    ///     own daemon with the projection shard actually in the Running state.
    /// </summary>
    [Fact]
    public async Task agents_start_on_every_tenant_database_without_a_default_tenant()
    {
        using var host = await StartHostAsync(registerDefaultTenant: false);
        var hostedService = host.Services.GetRequiredService<PolecatDaemonHostedService>();

        hostedService.Daemons.Count.ShouldBe(2);

        foreach (var daemon in hostedService.Daemons)
        {
            await daemon.WaitForShardToBeRunning(ShardName, Timeout);
            daemon.CurrentAgents().ShouldNotBeEmpty();
        }

        // The daemons must be on DIFFERENT databases — one daemon per tenant database is the whole
        // point, and two daemons pointed at the same database would satisfy a naive count check.
        hostedService.Daemons
            .Select(d => ((PolecatProjectionDaemon)d).Database.Identifier)
            .Distinct()
            .Count()
            .ShouldBe(2);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     The shape users fall into when they register a placeholder "*DEFAULT*" tenant to get past
    ///     startup (polecat#514). It must not be necessary — but if someone does register a real
    ///     default tenant database, the daemon has to treat it as just another tenant database:
    ///     three databases, three daemons, all with running agents, none skipped or doubled up.
    /// </summary>
    [Fact]
    public async Task agents_start_on_every_tenant_database_with_a_default_tenant_registered()
    {
        using var host = await StartHostAsync(registerDefaultTenant: true);
        var hostedService = host.Services.GetRequiredService<PolecatDaemonHostedService>();

        hostedService.Daemons.Count.ShouldBe(3);

        foreach (var daemon in hostedService.Daemons)
        {
            await daemon.WaitForShardToBeRunning(ShardName, Timeout);
            daemon.CurrentAgents().ShouldNotBeEmpty();
        }

        hostedService.Daemons
            .Select(d => ((PolecatProjectionDaemon)d).Database.Identifier)
            .Distinct()
            .Count()
            .ShouldBe(3);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     Registering a default tenant does NOT re-enable default tenant usage. The flag is set by
    ///     configuring the tenancy, not by which tenant ids happen to be in it — otherwise the
    ///     placeholder workaround would quietly restore the silent-wrong-database behaviour it was
    ///     invented to work around.
    /// </summary>
    [Fact]
    public async Task registering_a_default_tenant_does_not_re_enable_default_tenant_usage()
    {
        using var host = await StartHostAsync(registerDefaultTenant: true);
        var store = (DocumentStore)host.Services.GetRequiredService<IDocumentStore>();

        store.Options.DefaultTenantUsageEnabled.ShouldBeFalse();
        Should.Throw<DefaultTenantUsageDisabledException>(() => store.LightweightSession());

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     Agents that are running must actually process events — a Running shard that never
    ///     advances is the failure mode a state-only assertion misses.
    /// </summary>
    [Fact]
    public async Task running_agents_process_events_on_their_own_tenant_database()
    {
        using var host = await StartHostAsync(registerDefaultTenant: false);
        var store = (DocumentStore)host.Services.GetRequiredService<IDocumentStore>();

        var idA = Guid.NewGuid();
        var idB = Guid.NewGuid();

        await AppendAsync(store, TenantA, idA, 2);
        await AppendAsync(store, TenantB, idB, 5);

        await WaitForTallyAsync(store, TenantA, idA, 2);
        await WaitForTallyAsync(store, TenantB, idB, 5);

        // Cross-checks: neither tenant's stream leaked into the other's database.
        (await LoadTallyAsync(store, TenantA, idB)).ShouldBeNull();
        (await LoadTallyAsync(store, TenantB, idA)).ShouldBeNull();

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    // -------------------------------------------------------------------------------------
    // Master table tenancy — tenant databases discovered at runtime
    // -------------------------------------------------------------------------------------

    /// <summary>
    ///     Port of Marten's <c>dynamic_spin_up_of_dynamic_tenants</c>. Tenant databases added to the
    ///     control table AFTER the host is running must be discovered by the projection coordinator,
    ///     which then starts agents against each of them. Polecat's master-table tests covered the
    ///     registry mechanics (add/disable/enable/delete) but never that a daemon follows.
    /// </summary>
    [Fact]
    public async Task coordinator_spins_up_agents_for_dynamically_added_tenant_databases()
    {
        using var host = await Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddPolecat(opts =>
                    {
                        opts.ConnectionString = ConnectionFor(ControlDb);
                        opts.DatabaseSchemaName = Schema;
                        opts.AutoCreateSchemaObjects = AutoCreate.All;
                        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
                        opts.MultiTenantedMasterTable(ConnectionFor(ControlDb), Schema);
                        opts.Projections.Add<AgentTallyProjection>(ProjectionLifecycle.Async);
                    })
                    .AddProjectionCoordinator(DaemonMode.Solo);
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var store = (DocumentStore)host.Services.GetRequiredService<IDocumentStore>();
        // The class fixture drops and recreates the control database, so pc_tenants starts empty.
        var tenancy = (MasterTableTenancy)store.Options.Tenancy!;
        await tenancy.AddDatabaseRecordAsync(TenantA, ConnectionFor(DbA), TestContext.Current.CancellationToken);
        await tenancy.AddDatabaseRecordAsync(TenantB, ConnectionFor(DbB), TestContext.Current.CancellationToken);

        var coordinator = host.Services.GetRequiredService<IProjectionCoordinator>();

        var daemonA = await coordinator.DaemonForDatabase(
            store.Options.Tenancy!.GetDatabase(TenantA).Identifier);
        var daemonB = await coordinator.DaemonForDatabase(
            store.Options.Tenancy!.GetDatabase(TenantB).Identifier);

        await daemonA.StartAllAsync();
        await daemonB.StartAllAsync();

        await daemonA.WaitForShardToBeRunning(ShardName, Timeout);
        await daemonB.WaitForShardToBeRunning(ShardName, Timeout);

        daemonA.CurrentAgents().ShouldNotBeEmpty();
        daemonB.CurrentAgents().ShouldNotBeEmpty();

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     Master table tenancy disables the default tenant for the same reason static
    ///     database-per-tenant does, and a daemon built with no tenant must say so.
    /// </summary>
    [Fact]
    public async Task master_table_tenancy_requires_a_tenant_for_a_daemon()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionFor(ControlDb);
            opts.DatabaseSchemaName = Schema;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.MultiTenantedMasterTable(ConnectionFor(ControlDb), Schema);
            opts.Projections.Add<AgentTallyProjection>(ProjectionLifecycle.Async);
        });

        store.Options.DefaultTenantUsageEnabled.ShouldBeFalse();

        await Should.ThrowAsync<DefaultTenantUsageDisabledException>(async () =>
        {
            await store.BuildProjectionDaemonAsync();
        });
    }

    // -------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------

    private static async Task<IHost> StartHostAsync(bool registerDefaultTenant)
    {
        return await Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddPolecat(opts =>
                    {
                        opts.ConnectionString = ConnectionFor(DbA);
                        opts.DatabaseSchemaName = Schema;
                        opts.AutoCreateSchemaObjects = AutoCreate.All;
                        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

                        opts.MultiTenantedDatabases(tenancy =>
                        {
                            tenancy.AddTenant(TenantA, ConnectionFor(DbA));
                            tenancy.AddTenant(TenantB, ConnectionFor(DbB));

                            if (registerDefaultTenant)
                            {
                                tenancy.AddTenant(StorageConstants.DefaultTenantId, ConnectionFor(ControlDb));
                            }
                        });

                        opts.Projections.Add<AgentTallyProjection>(ProjectionLifecycle.Async);
                    })
                    .ApplyAllDatabaseChangesOnStartup()
                    .AddAsyncDaemon(DaemonMode.Solo);
            })
            .StartAsync(TestContext.Current.CancellationToken);
    }

    private static async Task AppendAsync(DocumentStore store, string tenantId, Guid streamId, int count)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenantId });
        session.Events.StartStream(streamId, new AgentCounted());
        for (var i = 1; i < count; i++) session.Events.Append(streamId, new AgentCounted());
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<AgentTally?> LoadTallyAsync(DocumentStore store, string tenantId, Guid id)
    {
        await using var session = store.QuerySession(new SessionOptions { TenantId = tenantId });
        return await session.LoadAsync<AgentTally>(id, TestContext.Current.CancellationToken);
    }

    private static async Task WaitForTallyAsync(DocumentStore store, string tenantId, Guid id, int expected)
    {
        var deadline = DateTimeOffset.UtcNow.Add(Timeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var tally = await LoadTallyAsync(store, tenantId, id);
            if (tally?.Count == expected) return;
            await Task.Delay(250, TestContext.Current.CancellationToken);
        }

        var actual = await LoadTallyAsync(store, tenantId, id);
        throw new TimeoutException(
            $"Tenant '{tenantId}' never reached an AgentTally count of {expected}; last seen {actual?.Count.ToString() ?? "<null>"}.");
    }
}

public record AgentCounted;

public class AgentTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class AgentTallyProjection : Polecat.Projections.SingleStreamProjection<AgentTally, Guid>
{
    public override AgentTally? Evolve(AgentTally? snapshot, Guid id, IEvent e)
    {
        snapshot ??= new AgentTally { Id = id };
        if (e.Data is AgentCounted) snapshot.Count++;
        return snapshot;
    }
}

/// <summary>
///     Creates the tenant databases once for the class — see the note on
///     <c>TenantDatabasesFixture</c> about the connection-pool race when they are dropped per test.
/// </summary>
public class AgentStartupDatabasesFixture : IAsyncLifetime
{
    public async ValueTask InitializeAsync()
    {
        await DropAsync();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in daemon_agent_startup_multi_tenancy_tests.AllDatabases)
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

        foreach (var db in daemon_agent_startup_multi_tenancy_tests.AllDatabases)
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
