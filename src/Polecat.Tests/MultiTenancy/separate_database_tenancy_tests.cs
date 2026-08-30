using JasperFx;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Weasel.Core;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.MultiTenancy;

public class separate_database_tenancy_tests : IAsyncLifetime
{
    private const string TenantA = "tenant_a";
    private const string TenantB = "tenant_b";
    private static readonly string DbA = ConnectionSource.Scoped("tenant_a");
    private static readonly string DbB = ConnectionSource.Scoped("tenant_b");

    private static readonly string MasterConnectionString = ConnectionSource.MasterConnectionString;

    private static string TenantConnectionString(string dbName) =>
        ConnectionSource.ConnectionStringFor(dbName);

    public async ValueTask InitializeAsync()
    {
        // Create tenant databases if they don't exist
        await using var conn = new SqlConnection(MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in new[] { DbA, DbB })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Drop tenant databases
        await using var conn = new SqlConnection(MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in new[] { DbA, DbB })
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

    private DocumentStore CreateSeparateTenantStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = TenantConnectionString(DbA); // default connection
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant(TenantA, TenantConnectionString(DbA));
                tenancy.AddTenant(TenantB, TenantConnectionString(DbB));
            });
        });
    }

    /// <summary>
    ///     Port of Marten's <c>using_static_database_multitenancy.can_use_bulk_inserts</c>.
    /// </summary>
    [Fact]
    public async Task can_use_bulk_inserts()
    {
        using var store = CreateSeparateTenantStore();
        await EnsureSchemaOnAllDatabasesAsync(store);

        var targetsA = GenerateTargets(100);
        var targetsB = GenerateTargets(50);

        await store.Advanced.Clean.DeleteAllDocumentsAsync(TestContext.Current.CancellationToken);

        await store.Advanced.BulkInsertAsync(targetsA, BulkInsertMode.InsertsOnly, 200, TenantA,
            TestContext.Current.CancellationToken);
        await store.Advanced.BulkInsertAsync(targetsB, BulkInsertMode.InsertsOnly, 200, TenantB,
            TestContext.Current.CancellationToken);

        await using (var queryA = store.QuerySession(new SessionOptions { TenantId = TenantA }))
        {
            var ids = await queryA.Query<Target>().Select(x => x.Id).ToListAsync(TestContext.Current.CancellationToken);
            ids.OrderBy(x => x).ToList().ShouldBe(targetsA.OrderBy(x => x.Id).Select(x => x.Id).ToList());
        }

        await using (var queryB = store.QuerySession(new SessionOptions { TenantId = TenantB }))
        {
            var ids = await queryB.Query<Target>().Select(x => x.Id).ToListAsync(TestContext.Current.CancellationToken);
            ids.OrderBy(x => x).ToList().ShouldBe(targetsB.OrderBy(x => x.Id).Select(x => x.Id).ToList());
        }
    }

    /// <summary>
    ///     Port of Marten's <c>using_static_database_multitenancy.clean_crosses_the_tenanted_databases</c>.
    ///     Polecat's cleaners used to read StoreOptions.ConnectionString and so emptied exactly one
    ///     database, silently leaving every other tenant populated. polecat#514.
    /// </summary>
    [Fact]
    public async Task clean_crosses_the_tenanted_databases()
    {
        using var store = CreateSeparateTenantStore();
        await EnsureSchemaOnAllDatabasesAsync(store);

        var targetsA = GenerateTargets(100);
        var targetsB = GenerateTargets(50);

        await store.Advanced.BulkInsertAsync(targetsA, BulkInsertMode.InsertsOnly, 200, TenantA,
            TestContext.Current.CancellationToken);
        await store.Advanced.BulkInsertAsync(targetsB, BulkInsertMode.InsertsOnly, 200, TenantB,
            TestContext.Current.CancellationToken);

        await store.Advanced.Clean.DeleteAllDocumentsAsync(TestContext.Current.CancellationToken);

        await using (var queryA = store.QuerySession(new SessionOptions { TenantId = TenantA }))
        {
            (await queryA.Query<Target>().AnyAsync(TestContext.Current.CancellationToken)).ShouldBeFalse();
        }

        await using (var queryB = store.QuerySession(new SessionOptions { TenantId = TenantB }))
        {
            (await queryB.Query<Target>().AnyAsync(TestContext.Current.CancellationToken)).ShouldBeFalse();
        }
    }

    /// <summary>
    ///     The event-store twin of <c>clean_crosses_the_tenanted_databases</c> — Polecat has
    ///     CleanAllEventDataAsync where Marten's cleaner exposes DeleteAllEventDataAsync, and it had
    ///     the same single-database bug.
    /// </summary>
    [Fact]
    public async Task clean_event_data_crosses_the_tenanted_databases()
    {
        using var store = CreateSeparateTenantStore();
        await EnsureSchemaOnAllDatabasesAsync(store);

        foreach (var tenant in new[] { TenantA, TenantB })
        {
            await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
            session.Events.StartStream(Guid.NewGuid(), new TenancyEventHappened());
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await store.Advanced.Clean.DeleteAllEventDataAsync(TestContext.Current.CancellationToken);

        foreach (var tenant in new[] { TenantA, TenantB })
        {
            await using var session = store.QuerySession(new SessionOptions { TenantId = tenant });
            var events = await session.Events.QueryAllRawEvents()
                .ToListAsync(TestContext.Current.CancellationToken);
            events.ShouldBeEmpty();
        }
    }

    private static async Task EnsureSchemaOnAllDatabasesAsync(DocumentStore store)
    {
        foreach (var database in store.Options.Tenancy!.AllDatabases())
        {
            await database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        }
    }

    private static Target[] GenerateTargets(int count) =>
        Enumerable.Range(0, count)
            .Select(i => new Target { Id = Guid.NewGuid(), Number = i, Color = "Blue" })
            .ToArray();

    /// <summary>
    ///     Mirrors Marten's <c>using_static_database_multitenancy.default_tenant_usage_is_disabled</c>:
    ///     configuring a database per tenant turns the default tenant off, so no dummy "*DEFAULT*"
    ///     tenant is ever needed. polecat#514.
    /// </summary>
    [Fact]
    public void default_tenant_usage_is_disabled()
    {
        using var store = CreateSeparateTenantStore();

        store.Options.DefaultTenantUsageEnabled.ShouldBeFalse();
    }

    [Fact]
    public async Task separate_databases_store_documents_independently()
    {
        using var store = CreateSeparateTenantStore();

        // Ensure schema exists on both tenant databases
        foreach (var db in store.Options.Tenancy!.AllDatabases())
        {
            await db.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        }

        var docId = Guid.NewGuid();

        // Store a document in tenant A
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
        {
            session.Store(new TestDoc { Id = docId, Name = "Tenant A Doc" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Tenant A can load it
        await using (var query = store.QuerySession(new SessionOptions { TenantId = TenantA }))
        {
            var doc = await query.LoadAsync<TestDoc>(docId, TestContext.Current.CancellationToken);
            doc.ShouldNotBeNull();
            doc.Name.ShouldBe("Tenant A Doc");
        }

        // Tenant B cannot see it (separate database)
        await using (var query = store.QuerySession(new SessionOptions { TenantId = TenantB }))
        {
            var doc = await query.LoadAsync<TestDoc>(docId, TestContext.Current.CancellationToken);
            doc.ShouldBeNull();
        }
    }

    [Fact]
    public async Task separate_databases_store_events_independently()
    {
        using var store = CreateSeparateTenantStore();

        foreach (var db in store.Options.Tenancy!.AllDatabases())
        {
            await db.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        }

        var streamId = Guid.NewGuid();

        // Start a stream in tenant A
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
        {
            session.Events.StartStream(streamId, new QuestStarted("Quest in A"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Tenant A can fetch the stream
        await using (var query = store.QuerySession(new SessionOptions { TenantId = TenantA }))
        {
            var state = await query.Events.FetchStreamStateAsync(streamId, TestContext.Current.CancellationToken);
            state.ShouldNotBeNull();
        }

        // Tenant B cannot see the stream
        await using (var query = store.QuerySession(new SessionOptions { TenantId = TenantB }))
        {
            var state = await query.Events.FetchStreamStateAsync(streamId, TestContext.Current.CancellationToken);
            state.ShouldBeNull();
        }
    }

    [Fact]
    public async Task query_session_routes_to_correct_tenant_database()
    {
        using var store = CreateSeparateTenantStore();

        foreach (var db in store.Options.Tenancy!.AllDatabases())
        {
            await db.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        }

        var docIdA = Guid.NewGuid();
        var docIdB = Guid.NewGuid();

        // Store in tenant A
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
        {
            session.Store(new TestDoc { Id = docIdA, Name = "A" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Store in tenant B
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantB }))
        {
            session.Store(new TestDoc { Id = docIdB, Name = "B" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Verify cross-tenant isolation
        await using (var qa = store.QuerySession(new SessionOptions { TenantId = TenantA }))
        await using (var qb = store.QuerySession(new SessionOptions { TenantId = TenantB }))
        {
            (await qa.LoadAsync<TestDoc>(docIdA, TestContext.Current.CancellationToken)).ShouldNotBeNull();
            (await qa.LoadAsync<TestDoc>(docIdB, TestContext.Current.CancellationToken)).ShouldBeNull();

            (await qb.LoadAsync<TestDoc>(docIdB, TestContext.Current.CancellationToken)).ShouldNotBeNull();
            (await qb.LoadAsync<TestDoc>(docIdA, TestContext.Current.CancellationToken)).ShouldBeNull();
        }
    }

    [Fact]
    public void unknown_tenant_throws()
    {
        using var store = CreateSeparateTenantStore();

        Should.Throw<UnknownTenantIdException>(() =>
        {
            store.LightweightSession(new SessionOptions { TenantId = "nonexistent" });
        });
    }

    [Fact]
    public void all_databases_returns_all_tenants()
    {
        using var store = CreateSeparateTenantStore();

        var databases = store.Options.Tenancy!.AllDatabases();
        databases.Count.ShouldBe(2);
    }

    [Fact]
    public async Task event_store_all_databases_returns_one_per_configured_database()
    {
        using var store = CreateSeparateTenantStore();

        // The store-agnostic IEventStore.AllDatabases() (jasperfx#387) must surface every
        // configured database as an IEventDatabase, matching the tenancy accessor.
        var databases = await ((IEventStore)store).AllDatabases();

        databases.Count.ShouldBe(2);
        databases.ShouldAllBe(db => db is PolecatDatabase);
    }

    [Fact]
    public void default_tenancy_returns_single_database()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        store.Options.Tenancy.ShouldNotBeNull();
        store.Options.Tenancy.Cardinality.ShouldBe(DatabaseCardinality.Single);
        store.Options.Tenancy.AllDatabases().Count.ShouldBe(1);
    }

    public record TenancyEventHappened;

    public class TestDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }
}
