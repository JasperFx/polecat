using JasperFx;
using JasperFx.Descriptors;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.MultiTenancy;

public class separate_database_tenancy_tests : IAsyncLifetime
{
    private const string TenantA = "tenant_a";
    private const string TenantB = "tenant_b";
    private const string DbA = "polecat_tenant_a";
    private const string DbB = "polecat_tenant_b";

    private static readonly string MasterConnectionString =
        "Server=localhost,11433;Database=master;User Id=sa;Password=Polecat#Dev2025;TrustServerCertificate=true";

    private static string TenantConnectionString(string dbName) =>
        $"Server=localhost,11433;Database={dbName};User Id=sa;Password=Polecat#Dev2025;TrustServerCertificate=true";

    public async Task InitializeAsync()
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

    public async Task DisposeAsync()
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

            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant(TenantA, TenantConnectionString(DbA));
                tenancy.AddTenant(TenantB, TenantConnectionString(DbB));
            });
        });
    }

    [Fact]
    public async Task separate_databases_store_documents_independently()
    {
        using var store = CreateSeparateTenantStore();

        // Ensure schema exists on both tenant databases
        foreach (var db in store.Options.Tenancy!.AllDatabases())
        {
            await db.ApplyAllConfiguredChangesToDatabaseAsync();
        }

        var docId = Guid.NewGuid();

        // Store a document in tenant A
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
        {
            session.Store(new TestDoc { Id = docId, Name = "Tenant A Doc" });
            await session.SaveChangesAsync();
        }

        // Tenant A can load it
        await using (var query = store.QuerySession(new SessionOptions { TenantId = TenantA }))
        {
            var doc = await query.LoadAsync<TestDoc>(docId);
            doc.ShouldNotBeNull();
            doc.Name.ShouldBe("Tenant A Doc");
        }

        // Tenant B cannot see it (separate database)
        await using (var query = store.QuerySession(new SessionOptions { TenantId = TenantB }))
        {
            var doc = await query.LoadAsync<TestDoc>(docId);
            doc.ShouldBeNull();
        }
    }

    [Fact]
    public async Task separate_databases_store_events_independently()
    {
        using var store = CreateSeparateTenantStore();

        foreach (var db in store.Options.Tenancy!.AllDatabases())
        {
            await db.ApplyAllConfiguredChangesToDatabaseAsync();
        }

        var streamId = Guid.NewGuid();

        // Start a stream in tenant A
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
        {
            session.Events.StartStream(streamId, new QuestStarted("Quest in A"));
            await session.SaveChangesAsync();
        }

        // Tenant A can fetch the stream
        await using (var query = store.QuerySession(new SessionOptions { TenantId = TenantA }))
        {
            var state = await query.Events.FetchStreamStateAsync(streamId);
            state.ShouldNotBeNull();
        }

        // Tenant B cannot see the stream
        await using (var query = store.QuerySession(new SessionOptions { TenantId = TenantB }))
        {
            var state = await query.Events.FetchStreamStateAsync(streamId);
            state.ShouldBeNull();
        }
    }

    [Fact]
    public async Task query_session_routes_to_correct_tenant_database()
    {
        using var store = CreateSeparateTenantStore();

        foreach (var db in store.Options.Tenancy!.AllDatabases())
        {
            await db.ApplyAllConfiguredChangesToDatabaseAsync();
        }

        var docIdA = Guid.NewGuid();
        var docIdB = Guid.NewGuid();

        // Store in tenant A
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
        {
            session.Store(new TestDoc { Id = docIdA, Name = "A" });
            await session.SaveChangesAsync();
        }

        // Store in tenant B
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantB }))
        {
            session.Store(new TestDoc { Id = docIdB, Name = "B" });
            await session.SaveChangesAsync();
        }

        // Verify cross-tenant isolation
        await using (var qa = store.QuerySession(new SessionOptions { TenantId = TenantA }))
        await using (var qb = store.QuerySession(new SessionOptions { TenantId = TenantB }))
        {
            (await qa.LoadAsync<TestDoc>(docIdA)).ShouldNotBeNull();
            (await qa.LoadAsync<TestDoc>(docIdB)).ShouldBeNull();

            (await qb.LoadAsync<TestDoc>(docIdB)).ShouldNotBeNull();
            (await qb.LoadAsync<TestDoc>(docIdA)).ShouldBeNull();
        }
    }

    [Fact]
    public void unknown_tenant_throws()
    {
        using var store = CreateSeparateTenantStore();

        Should.Throw<UnknownTenantException>(() =>
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
    public void default_tenancy_returns_single_database()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
        });

        store.Options.Tenancy.ShouldNotBeNull();
        store.Options.Tenancy.Cardinality.ShouldBe(DatabaseCardinality.Single);
        store.Options.Tenancy.AllDatabases().Count.ShouldBe(1);
    }

    public class TestDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }
}
