using JasperFx;
using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Events.Daemon;
using Polecat.Exceptions;
using Polecat.Projections;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     Async daemon under database-per-tenant multi-tenancy. Mirrors Marten's
///     <c>DaemonTests/MultiTenancy/multi_tenancy_by_database.cs</c> and
///     <c>MultiTenancyTests/using_static_database_multitenancy.cs</c>, which Polecat had no
///     equivalent of: <c>MultiTenancy/separate_database_tenancy_tests.cs</c> never registers a
///     projection, and <c>Daemon/multi_tenant_daemon_tests.cs</c> is single-database despite the
///     name. polecat#514.
/// </summary>
public class daemon_multi_tenancy_by_database_tests : IClassFixture<TenantDatabasesFixture>
{
    internal const string Tenant1 = "tenant1";
    internal const string Tenant2 = "tenant2";
    internal const string Tenant3 = "tenant3";

    internal static readonly string Db1 = ConnectionSource.Scoped("mtdb_one");
    internal static readonly string Db2 = ConnectionSource.Scoped("mtdb_two");
    internal static readonly string Db3 = ConnectionSource.Scoped("mtdb_three");

    internal static readonly string[] AllDatabases = [Db1, Db2, Db3];

    private static string ConnectionFor(string db) => ConnectionSource.ConnectionStringFor(db);

    private readonly TenantDatabasesFixture _fixture;

    public daemon_multi_tenancy_by_database_tests(TenantDatabasesFixture fixture)
    {
        _fixture = fixture;
    }

    private static void ConfigureTenancy(StoreOptions opts, string schemaName = "mtdb")
    {
        opts.DatabaseSchemaName = schemaName;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

        opts.MultiTenantedDatabases(tenancy =>
        {
            tenancy.AddTenant(Tenant1, ConnectionFor(Db1));
            tenancy.AddTenant(Tenant2, ConnectionFor(Db2));
            tenancy.AddTenant(Tenant3, ConnectionFor(Db3));
        });

        opts.Projections.Add<TenantTallyProjection>(ProjectionLifecycle.Async);
    }

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            // Marten does not require a top level connection string once a tenancy is configured.
            // Polecat's DocumentStore constructor calls StoreOptions.CreateConnectionFactory()
            // unconditionally, so one has to be supplied. polecat#514 (first bullet).
            opts.ConnectionString = ConnectionFor(Db1);
            ConfigureTenancy(opts);
        });
    }

    private static async Task ApplyToAllAsync(DocumentStore store)
    {
        foreach (var database in store.Options.Tenancy!.AllDatabases())
        {
            await database.ApplyAllConfiguredChangesToDatabaseAsync(
                ct: TestContext.Current.CancellationToken);
        }
    }

    // -------------------------------------------------------------------------------------
    // Configuration surface
    // -------------------------------------------------------------------------------------

    /// <summary>
    ///     Marten's <c>using_static_database_multitenancy.default_tenant_usage_is_disabled</c>.
    ///     <c>MultiTenantedDatabases()</c> flips <c>Advanced.DefaultTenantUsageEnabled</c> to false,
    ///     so a session opened with no tenant fails loudly instead of silently landing somewhere.
    /// </summary>
    [Fact]
    public void default_tenant_usage_is_disabled_by_configuring_separate_databases()
    {
        using var store = CreateStore();

        store.Options.DefaultTenantUsageEnabled.ShouldBeFalse();
    }

    /// <summary>
    ///     A default tenant should not be necessary. Marten throws
    ///     <c>DefaultTenantUsageDisabledException</c>; the current Polecat behaviour is an
    ///     <c>UnknownTenantIdException</c> for "*DEFAULT*" only because the tenancy has no such
    ///     entry — which is what pushes users into registering a dummy "*DEFAULT*" tenant.
    /// </summary>
    [Fact]
    public void opening_a_session_with_no_tenant_throws_default_tenant_usage_disabled()
    {
        using var store = CreateStore();

        Should.Throw<DefaultTenantUsageDisabledException>(() => store.LightweightSession());
    }

    /// <summary>
    ///     Marten's <c>multi_tenancy_by_database.fail_when_trying_to_create_daemon_with_no_tenant</c>.
    /// </summary>
    [Fact]
    public async Task fail_when_trying_to_create_daemon_with_no_tenant()
    {
        using var store = CreateStore();

        await Should.ThrowAsync<DefaultTenantUsageDisabledException>(async () =>
        {
            await store.BuildProjectionDaemonAsync();
        });
    }

    // -------------------------------------------------------------------------------------
    // Schema provisioning
    // -------------------------------------------------------------------------------------

    /// <summary>
    ///     Marten's <c>using_static_database_multitenancy.changes_are_applied_to_each_database</c>.
    ///     <c>ApplyAllDatabaseChangesOnStartup()</c> must reach EVERY tenant database, not just the
    ///     one behind <c>StoreOptions.ConnectionString</c>. polecat#514 (second bullet).
    /// </summary>
    [Fact]
    public async Task apply_all_database_changes_on_startup_reaches_every_tenant_database()
    {
        using var host = await Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddPolecat(opts =>
                    {
                        opts.ConnectionString = ConnectionFor(Db1);
                        ConfigureTenancy(opts, StartupSchema);
                    })
                    .ApplyAllDatabaseChangesOnStartup();
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var missing = new List<string>();
        foreach (var db in AllDatabases)
        {
            if (!await EventsTableExistsAsync(db, StartupSchema)) missing.Add(db);
        }

        missing.ShouldBeEmpty(
            $"ApplyAllDatabaseChangesOnStartup() did not provision {StartupSchema}.pc_events in: {string.Join(", ", missing)}");

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    // A schema no other test in this class touches — the tenant databases are shared across the
    // class, so checking the default "mtdb" schema would go green off a sibling test's manual
    // ApplyToAllAsync rather than off the activator under test.
    private const string StartupSchema = "mtdb_startup";

    private static async Task<bool> EventsTableExistsAsync(string databaseName, string schemaName)
    {
        await using var conn = new SqlConnection(ConnectionFor(databaseName));
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id " +
            $"WHERE s.name = '{schemaName}' AND t.name = 'pc_events'";
        var count = (int)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        return count > 0;
    }

    // -------------------------------------------------------------------------------------
    // Daemon routing
    // -------------------------------------------------------------------------------------

    /// <summary>
    ///     Marten's <c>multi_tenancy_by_database.build_daemon_for_database</c> — the daemon built
    ///     for a tenant id runs against that tenant's database.
    /// </summary>
    [Fact]
    public async Task build_daemon_for_database()
    {
        using var store = CreateStore();
        await ApplyToAllAsync(store);

        using var daemon = await store.BuildProjectionDaemonAsync(Tenant2);

        var database = ((PolecatProjectionDaemon)daemon).Database.ShouldBeOfType<PolecatDatabase>();
        new SqlConnectionStringBuilder(database.ConnectionString).InitialCatalog.ShouldBe(Db2);
    }

    /// <summary>
    ///     Marten's <c>projection_statuses_per_database</c> — every tenant database gets its own
    ///     daemon, and each tracks its own progression.
    /// </summary>
    [Fact]
    public async Task each_tenant_database_has_its_own_projection_progress()
    {
        using var store = CreateStore();
        await ApplyToAllAsync(store);

        var before1 = await ProgressFor(store, Tenant1);
        var before2 = await ProgressFor(store, Tenant2);
        var before3 = await ProgressFor(store, Tenant3);

        await AppendAsync(store, Tenant1, 2);
        await AppendAsync(store, Tenant3, 4);

        await RunDaemonToCompletionAsync(store, Tenant1);
        await RunDaemonToCompletionAsync(store, Tenant3);

        (await ProgressFor(store, Tenant1) - before1).ShouldBe(2);
        (await ProgressFor(store, Tenant3) - before3).ShouldBe(4);

        // Tenant2 saw no events at all, so its progression must not have moved.
        (await ProgressFor(store, Tenant2)).ShouldBe(before2);
    }

    /// <summary>
    ///     Marten's <c>multi_tenancy_by_database.run_projections_end_to_end</c>. Each tenant's
    ///     stream lives in its own database and projects into its own database — the same stream id
    ///     in three databases must produce three independent aggregates.
    /// </summary>
    [Fact]
    public async Task run_projections_end_to_end()
    {
        using var store = CreateStore();
        await ApplyToAllAsync(store);

        var id = Guid.NewGuid();

        await AppendAsync(store, Tenant1, 1, id);
        await AppendAsync(store, Tenant2, 3, id);
        await AppendAsync(store, Tenant3, 5, id);

        await RunDaemonToCompletionAsync(store, Tenant1);
        await RunDaemonToCompletionAsync(store, Tenant2);
        await RunDaemonToCompletionAsync(store, Tenant3);

        (await LoadTallyAsync(store, Tenant1, id)).ShouldNotBeNull().Count.ShouldBe(1);
        (await LoadTallyAsync(store, Tenant2, id)).ShouldNotBeNull().Count.ShouldBe(3);
        (await LoadTallyAsync(store, Tenant3, id)).ShouldNotBeNull().Count.ShouldBe(5);
    }

    /// <summary>
    ///     The daemon has to keep reading an existing aggregate out of the tenant's own database
    ///     across batches. A projection that only ever creates would pass even if the read side
    ///     routed to the wrong database, so this appends in two rounds with a daemon run between.
    /// </summary>
    [Fact]
    public async Task projections_continue_from_existing_snapshots_in_the_tenant_database()
    {
        using var store = CreateStore();
        await ApplyToAllAsync(store);

        var id = Guid.NewGuid();

        await AppendAsync(store, Tenant1, 2, id);
        await RunDaemonToCompletionAsync(store, Tenant1);
        (await LoadTallyAsync(store, Tenant1, id)).ShouldNotBeNull().Count.ShouldBe(2);

        await AppendAsync(store, Tenant1, 3, id);
        await RunDaemonToCompletionAsync(store, Tenant1);
        (await LoadTallyAsync(store, Tenant1, id)).ShouldNotBeNull().Count.ShouldBe(5);
    }

    /// <summary>
    ///     Marten's docs — and Polecat's — promise a daemon per tenant database. With
    ///     <c>AddAsyncDaemon</c> registered against a database-per-tenant store, events appended to
    ///     every tenant must get projected, not just the one behind the top level connection string.
    /// </summary>
    [Fact]
    public async Task add_async_daemon_runs_a_daemon_for_every_tenant_database()
    {
        using var host = await Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddPolecat(opts =>
                    {
                        opts.ConnectionString = ConnectionFor(Db1);
                        ConfigureTenancy(opts);
                    })
                    .ApplyAllDatabaseChangesOnStartup()
                    .AddAsyncDaemon(DaemonMode.Solo);
            })
            .StartAsync(TestContext.Current.CancellationToken);

        var store = (DocumentStore)host.Services.GetRequiredService<IDocumentStore>();

        var id = Guid.NewGuid();
        await AppendAsync(store, Tenant1, 2, id);
        await AppendAsync(store, Tenant2, 3, id);
        await AppendAsync(store, Tenant3, 4, id);

        await WaitForTallyAsync(store, Tenant1, id, 2);
        await WaitForTallyAsync(store, Tenant2, id, 3);
        await WaitForTallyAsync(store, Tenant3, id, 4);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    // -------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------

    private static async Task AppendAsync(DocumentStore store, string tenantId, int count, Guid? streamId = null)
    {
        var id = streamId ?? Guid.NewGuid();
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenantId });

        if (await session.Events.FetchStreamStateAsync(id, TestContext.Current.CancellationToken) == null)
        {
            session.Events.StartStream(id, new TenantCounted());
            count--;
        }

        for (var i = 0; i < count; i++)
        {
            session.Events.Append(id, new TenantCounted());
        }

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private static async Task RunDaemonToCompletionAsync(DocumentStore store, string tenantId)
    {
        using var daemon = await store.BuildProjectionDaemonAsync(tenantId);
        await daemon.StartAllAsync();
        await daemon.WaitForNonStaleData(30.Seconds());
        await daemon.StopAllAsync();
    }

    private static async Task<long> ProgressFor(DocumentStore store, string tenantId)
    {
        var database = store.Options.Tenancy!.GetDatabase(tenantId);
        var progress = await database.AllProjectionProgress(TestContext.Current.CancellationToken);
        return progress
            .Where(x => x.ShardName.StartsWith("TenantTally", StringComparison.OrdinalIgnoreCase))
            .Select(x => x.Sequence)
            .DefaultIfEmpty(0)
            .Max();
    }

    private static async Task<TenantTally?> LoadTallyAsync(DocumentStore store, string tenantId, Guid id)
    {
        await using var session = store.QuerySession(new SessionOptions { TenantId = tenantId });
        return await session.LoadAsync<TenantTally>(id, TestContext.Current.CancellationToken);
    }

    private static async Task WaitForTallyAsync(DocumentStore store, string tenantId, Guid id, int expected)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var tally = await LoadTallyAsync(store, tenantId, id);
            if (tally?.Count == expected) return;
            await Task.Delay(250, TestContext.Current.CancellationToken);
        }

        var actual = await LoadTallyAsync(store, tenantId, id);
        throw new TimeoutException(
            $"Tenant '{tenantId}' never reached a TenantTally count of {expected}; last seen {actual?.Count.ToString() ?? "<null>"}.");
    }
}

public record TenantCounted;

public class TenantTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class TenantTallyProjection : SingleStreamProjection<TenantTally, Guid>
{
    public override TenantTally? Evolve(TenantTally? snapshot, Guid id, IEvent e)
    {
        snapshot ??= new TenantTally { Id = id };
        if (e.Data is TenantCounted) snapshot.Count++;
        return snapshot;
    }
}

/// <summary>
///     Creates and drops the three tenant databases once for the whole class. Creating and dropping
///     them per test method races SqlClient's connection pool: a pooled connection to the just
///     dropped database is handed back out and the next test fails with "Cannot open database".
/// </summary>
public class TenantDatabasesFixture : IAsyncLifetime
{
    public async ValueTask InitializeAsync()
    {
        await DropDatabasesAsync();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in daemon_multi_tenancy_by_database_tests.AllDatabases)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync();
        }

        SqlConnection.ClearAllPools();
    }

    public ValueTask DisposeAsync() => new(DropDatabasesAsync());

    private static async Task DropDatabasesAsync()
    {
        SqlConnection.ClearAllPools();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in daemon_multi_tenancy_by_database_tests.AllDatabases)
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
