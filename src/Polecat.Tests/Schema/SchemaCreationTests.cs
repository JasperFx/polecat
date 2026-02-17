using JasperFx.Events;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Schema;

/// <summary>
///     Integration tests verifying that Weasel creates the event store tables correctly.
///     Requires a running SQL Server 2025 instance (see docker-compose.yml).
///     Each test drops and recreates tables since they test different schema configurations.
/// </summary>
public class SchemaCreationTests : IAsyncLifetime
{
    public Task InitializeAsync() => SchemaInspector.DropEventStoreTablesAsync();

    public Task DisposeAsync() => Task.CompletedTask;

    private static PolecatDatabase CreateDatabase(Action<StoreOptions>? configure = null)
    {
        var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };
        configure?.Invoke(options);
        return new PolecatDatabase(options);
    }

    [Fact]
    public async Task creates_all_three_event_store_tables()
    {
        var database = CreateDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var tables = await SchemaInspector.GetTableNamesAsync();
        tables.ShouldContain("pc_streams");
        tables.ShouldContain("pc_events");
        tables.ShouldContain("pc_event_progression");
    }

    [Fact]
    public async Task streams_table_has_guid_id_by_default()
    {
        var database = CreateDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_streams");
        var idCol = columns.Single(c => c.Name == "id");
        idCol.TypeName.ShouldBe("uniqueidentifier");
    }

    [Fact]
    public async Task streams_table_has_string_id_when_configured()
    {
        var database = CreateDatabase(o => o.Events.StreamIdentity = StreamIdentity.AsString);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_streams");
        var idCol = columns.Single(c => c.Name == "id");
        idCol.TypeName.ShouldBe("varchar");
    }

    [Fact]
    public async Task streams_table_has_expected_columns()
    {
        var database = CreateDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_streams");
        var names = columns.Select(c => c.Name).ToList();

        names.ShouldContain("id");
        names.ShouldContain("type");
        names.ShouldContain("version");
        names.ShouldContain("timestamp");
        names.ShouldContain("created");
        names.ShouldContain("snapshot");
        names.ShouldContain("snapshot_version");
        names.ShouldContain("tenant_id");
        names.ShouldContain("is_archived");
    }

    [Fact]
    public async Task events_table_has_expected_columns()
    {
        var database = CreateDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_events");
        var names = columns.Select(c => c.Name).ToList();

        names.ShouldContain("seq_id");
        names.ShouldContain("id");
        names.ShouldContain("stream_id");
        names.ShouldContain("version");
        names.ShouldContain("data");
        names.ShouldContain("type");
        names.ShouldContain("timestamp");
        names.ShouldContain("tenant_id");
        names.ShouldContain("dotnet_type");
        names.ShouldContain("is_archived");
    }

    [Fact]
    public async Task events_table_seq_id_is_identity()
    {
        var database = CreateDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var isIdentity = await SchemaInspector.IsColumnIdentityAsync("pc_events", "seq_id");
        isIdentity.ShouldBeTrue();
    }

    [Fact]
    public async Task events_table_has_unique_index_on_stream_and_version()
    {
        var database = CreateDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var indexes = await SchemaInspector.GetIndexInfoAsync("pc_events");
        var streamVersionIndex = indexes.Where(i => i.Name == "ix_pc_events_stream_and_version").ToList();
        streamVersionIndex.ShouldNotBeEmpty();
        streamVersionIndex.First().IsUnique.ShouldBeTrue();
    }

    [Fact]
    public async Task events_table_has_optional_metadata_columns_when_enabled()
    {
        var database = CreateDatabase(o =>
        {
            o.Events.EnableCorrelationId = true;
            o.Events.EnableCausationId = true;
            o.Events.EnableHeaders = true;
        });
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_events");
        var names = columns.Select(c => c.Name).ToList();

        names.ShouldContain("correlation_id");
        names.ShouldContain("causation_id");
        names.ShouldContain("headers");
    }

    [Fact]
    public async Task events_table_omits_metadata_columns_when_disabled()
    {
        var database = CreateDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_events");
        var names = columns.Select(c => c.Name).ToList();

        names.ShouldNotContain("correlation_id");
        names.ShouldNotContain("causation_id");
        names.ShouldNotContain("headers");
    }

    [Fact]
    public async Task event_progression_table_has_expected_columns()
    {
        var database = CreateDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_event_progression");
        var names = columns.Select(c => c.Name).ToList();

        names.ShouldContain("name");
        names.ShouldContain("last_seq_id");
        names.ShouldContain("last_updated");
    }

    [Fact]
    public async Task conjoined_tenancy_adds_tenant_id_to_streams_primary_key()
    {
        var database = CreateDatabase(o => o.Events.TenancyStyle = TenancyStyle.Conjoined);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var pkColumns = await SchemaInspector.GetPrimaryKeyColumnsAsync("pc_streams");
        pkColumns.ShouldContain("tenant_id");
        pkColumns.ShouldContain("id");
    }

    [Fact]
    public async Task conjoined_tenancy_includes_tenant_id_in_events_unique_index()
    {
        var database = CreateDatabase(o => o.Events.TenancyStyle = TenancyStyle.Conjoined);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var indexes = await SchemaInspector.GetIndexInfoAsync("pc_events");
        var streamVersionIndex = indexes
            .Where(i => i.Name == "ix_pc_events_stream_and_version")
            .ToList();
        streamVersionIndex.ShouldNotBeEmpty();

        var indexColumns = await SchemaInspector.GetIndexColumnsAsync("pc_events", "ix_pc_events_stream_and_version");
        indexColumns.ShouldContain("tenant_id");
        indexColumns.ShouldContain("stream_id");
        indexColumns.ShouldContain("version");
    }

    [Fact]
    public async Task apply_is_idempotent()
    {
        var database = CreateDatabase();

        // Apply twice — second call should not throw
        await database.ApplyAllConfiguredChangesToDatabaseAsync();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var tables = await SchemaInspector.GetTableNamesAsync();
        tables.ShouldContain("pc_streams");
        tables.ShouldContain("pc_events");
        tables.ShouldContain("pc_event_progression");
    }

    [Fact]
    public async Task events_table_has_foreign_key_to_streams()
    {
        var database = CreateDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var fks = await SchemaInspector.GetForeignKeysAsync("pc_events");
        fks.ShouldNotBeEmpty();
        var streamFk = fks.FirstOrDefault(fk => fk.ReferencedTable == "pc_streams");
        streamFk.ShouldNotBeNull();
    }
}
