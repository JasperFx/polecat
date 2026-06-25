using JasperFx;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat;
using Polecat.Internal;
using Polecat.Linq;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Seeding;

/// <summary>
/// #219: IInitialData seeders, added via AddPolecat(opts => opts.InitialData.Add(...)), must run on
/// host startup WITHOUT having to call ApplyAllDatabaseChangesOnStartup. Previously the activator that
/// runs the seeders was only registered by ApplyAllDatabaseChangesOnStartup / AddAsyncDaemon /
/// AddProjectionCoordinator, so the reporter's seeding never executed.
/// </summary>
public class initial_data_host_startup_tests
{
    private const string Schema = "initial_data_host";
    private static readonly Guid SeededId = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    public class SeededDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // Mirrors marcominerva's seeder: query first (must create the table on the fly), then seed.
    private class Seeder : IInitialData
    {
        public async Task Populate(IDocumentStore store, CancellationToken cancellation)
        {
            await using var session = store.LightweightSession();
            if (await session.Query<SeededDoc>().AnyAsync(cancellation)) return;

            session.Store(new SeededDoc { Id = SeededId, Name = "seeded" });
            await session.SaveChangesAsync(cancellation);
        }
    }

    [Fact]
    public async Task initial_data_runs_on_host_startup_without_ApplyAllDatabaseChangesOnStartup()
    {
        await DropSchemaAsync();

        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.InitialData.Add(new Seeder());
            // NOTE: deliberately NOT calling ApplyAllDatabaseChangesOnStartup()
        });

        await using var provider = services.BuildServiceProvider();

        // The activator that runs InitialData must be registered just by calling AddPolecat.
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        hostedServices.OfType<PolecatActivator>().ShouldHaveSingleItem();

        // Simulate host startup.
        foreach (var hosted in hostedServices)
        {
            await hosted.StartAsync(CancellationToken.None);
        }

        // Seeding ran — and the document table was created on the fly by the seeder.
        var store = provider.GetRequiredService<IDocumentStore>();
        await using var query = store.QuerySession();
        var doc = await query.LoadAsync<SeededDoc>(SeededId);
        doc.ShouldNotBeNull();
        doc!.Name.ShouldBe("seeded");
    }

    private static async Task DropSchemaAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF SCHEMA_ID('{Schema}') IS NOT NULL
            BEGIN
                DECLARE @sql NVARCHAR(MAX) = '';
                SELECT @sql += 'DROP TABLE IF EXISTS ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';' + CHAR(13)
                FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{Schema}';
                EXEC sp_executesql @sql;
            END
            """;
        await cmd.ExecuteNonQueryAsync();
    }
}
