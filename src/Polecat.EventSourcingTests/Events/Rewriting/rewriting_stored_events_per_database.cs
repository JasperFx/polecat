using JasperFx;
using Microsoft.Data.SqlClient;

namespace Polecat.Tests.Events.Rewriting;

/// <summary>
///     #1034 (marten#5643) on a database-per-tenant store: every database is rewritten, and reports, on its own.
/// </summary>
public class rewriting_stored_events_per_database : IAsyncLifetime
{
    private const string Schema = "rw1034";

    public record ParcelSentV1(string To);

    public record ParcelSent(string Recipient);

    private readonly string _databaseA = ConnectionSource.Scoped("rw1034_a");
    private readonly string _databaseB = ConnectionSource.Scoped("rw1034_b");

    private DocumentStore _store = null!;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await recreateDatabasesAsync(drop: true, create: true);

        _store = DocumentStore.For(opts =>
        {
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("tenant-a", ConnectionSource.ConnectionStringFor(_databaseA));
                tenancy.AddTenant("tenant-b", ConnectionSource.ConnectionStringFor(_databaseB));
            });

            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.Upcast<ParcelSentV1, ParcelSent>(x => new ParcelSent(x.To));
        });

        foreach (var database in await _store.Options.Tenancy!.BuildDatabasesAsync(Ct))
        {
            await database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        }

        await appendAsync("tenant-a", 3);
        await appendAsync("tenant-b", 2);
    }

    public async ValueTask DisposeAsync()
    {
        _store.Dispose();
        SqlConnection.ClearAllPools();
        await recreateDatabasesAsync(drop: true, create: false);
    }

    private async Task recreateDatabasesAsync(bool drop, bool create)
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in new[] { _databaseA, _databaseB })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = (drop
                ? $"""
                   IF DB_ID('{db}') IS NOT NULL
                   BEGIN
                       ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                       DROP DATABASE [{db}];
                   END
                   """
                : "") + (create ? $"\nCREATE DATABASE [{db}];" : "");
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task appendAsync(string tenantId, int count)
    {
        await using var session = _store.LightweightSession(tenantId);
        for (var i = 0; i < count; i++)
        {
            session.Events.StartStream(Guid.NewGuid(), new ParcelSentV1($"{tenantId} {i}"));
        }

        await session.SaveChangesAsync(Ct);
    }

    private string databaseOf(string tenantId) => _store.Options.Tenancy!.GetDatabase(tenantId).Identifier;

    [Fact]
    public async Task every_database_is_rewritten_and_reported_on_its_own()
    {
        var result = await _store.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.IncludeType<ParcelSent>();
            x.MaxParallelDatabases = 2;
            x.BatchSize = 2;
        }, Ct);

        result.Databases.Count.ShouldBe(2);
        result.Databases.Single(x => x.Cursor.Database == databaseOf("tenant-a")).Rewritten.ShouldBe(3);
        result.Databases.Single(x => x.Cursor.Database == databaseOf("tenant-b")).Rewritten.ShouldBe(2);
        result.Databases.ShouldAllBe(x => x.Completed);
    }

    [Fact]
    public async Task one_tenant_is_its_database()
    {
        var result = await _store.Advanced.RewriteStoredEventsAsync(x => x
            .IncludeType<ParcelSent>()
            .ForTenant("tenant-b"), Ct);

        var database = result.Databases.Single();
        database.Cursor.Database.ShouldBe(databaseOf("tenant-b"));
        database.Rewritten.ShouldBe(2);

        // Tenant a's database was not touched
        var rest = await _store.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.IncludeType<ParcelSent>().ForDatabase(databaseOf("tenant-a"));
            x.DryRun = true;
        }, Ct);
        rest.Databases.Single().Rewritten.ShouldBe(3);
    }

    [Fact]
    public async Task a_tenant_and_a_database_that_do_not_belong_together_are_refused()
    {
        await Should.ThrowAsync<InvalidOperationException>(() =>
            _store.Advanced.RewriteStoredEventsAsync(x => x
                .IncludeType<ParcelSent>()
                .ForTenant("tenant-b")
                .ForDatabase(databaseOf("tenant-a")), Ct));
    }
}
