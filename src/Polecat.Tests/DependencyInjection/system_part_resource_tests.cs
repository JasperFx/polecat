using JasperFx.CommandLine.Descriptions;
using JasperFx.Resources;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Tests.Harness;
using Weasel.Core.CommandLine;

namespace Polecat.Tests.DependencyInjection;

/// <summary>
///     #187: AddPolecat must register the Polecat database with JasperFx's resource
///     model (ISystemPart) so the idiomatic services.AddResourceSetupOnStartup()
///     provisions the schema with no Polecat-specific call, matching Marten.
/// </summary>
public class system_part_resource_tests
{
    [Fact]
    public void add_polecat_registers_a_polecat_system_part()
    {
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        using var provider = services.BuildServiceProvider();

        var parts = provider.GetServices<ISystemPart>().ToArray();
        parts.OfType<PolecatSystemPart>().ShouldNotBeEmpty();
    }

    [Fact]
    public async Task find_resources_wraps_each_tenant_database_as_a_database_resource()
    {
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        using var provider = services.BuildServiceProvider();

        var part = provider.GetServices<ISystemPart>().OfType<PolecatSystemPart>().Single();
        var store = (DocumentStore)provider.GetRequiredService<IDocumentStore>();

        var resources = await part.FindResources();

        var resource = resources.ShouldHaveSingleItem().ShouldBeOfType<DatabaseResource>();
        resource.Database.ShouldBeSameAs(store.Database);
        resource.Name.ShouldBe(store.Database.Identifier);
    }

    [Fact]
    public async Task add_resource_setup_on_startup_creates_the_polecat_schema()
    {
        const string schema = "resource_setup_187";
        await DropSchemaAsync(schema);

        // No .ApplyAllDatabaseChangesOnStartup() — rely solely on the generic
        // JasperFx resource bootstrapping, the path Marten supports out of the box.
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddPolecat(opts =>
                {
                    opts.ConnectionString = ConnectionSource.ConnectionString;
                    opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
                    opts.DatabaseSchemaName = schema;
                });

                services.AddResourceSetupOnStartup();
            })
            .Build();

        await host.StartAsync();
        try
        {
            var tables = await SchemaInspector.GetTableNamesAsync(schema);
            tables.ShouldContain("pc_streams");
            tables.ShouldContain("pc_events");
            tables.ShouldContain("pc_event_progression");
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static async Task DropSchemaAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF SCHEMA_ID('{schema}') IS NOT NULL
            BEGIN
                DECLARE @fksql NVARCHAR(MAX) = '';
                SELECT @fksql += 'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
                    + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';' + CHAR(13)
                FROM sys.foreign_keys fk
                JOIN sys.tables t ON fk.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{schema}';
                IF LEN(@fksql) > 0 EXEC sp_executesql @fksql;

                DECLARE @sql NVARCHAR(MAX) = '';
                SELECT @sql += 'DROP TABLE IF EXISTS ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';' + CHAR(13)
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{schema}';
                EXEC sp_executesql @sql;
            END
            """;
        await cmd.ExecuteNonQueryAsync();
    }
}
