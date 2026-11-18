using System.Text.Json;
using JasperFx.Events;
using Polecat.Linq;
using Polecat.Serialization;
using Weasel.Core;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Configuration;

/// <summary>
///     #1032: the defaults Polecat 6 changed to match Marten 10 (marten#5812), and
///     <see cref="StoreOptions.RestoreV5Defaults" /> putting each one back.
/// </summary>
public class polecat_6_default_tests
{
    [Fact]
    public void event_metadata_is_on_by_default()
    {
        var options = new StoreOptions();

        options.Events.EnableCorrelationId.ShouldBeTrue();
        options.Events.EnableCausationId.ShouldBeTrue();
        options.Events.EnableHeaders.ShouldBeTrue();
        options.Events.EnableUserName.ShouldBeTrue();
    }

    [Fact]
    public void the_rest_of_the_new_defaults()
    {
        var options = new StoreOptions();

        options.Events.EnableSideEffectsOnInlineProjections.ShouldBeTrue();
        options.Policies.EnableDocumentMetadataByDefault.ShouldBeTrue();
        options.Policies.StartIndexesByTenantId.ShouldBeTrue();
        options.OpenTelemetry.EventCountersEnabled.ShouldBeTrue();
        ((Serializer)options.Serializer).Options.AllowOutOfOrderMetadataProperties.ShouldBeTrue();
        options.EventGraph.UseArchivedStreamPartitioning.ShouldBeTrue();
        options.Events.EnableLateEventDetection.ShouldBeTrue();
        options.Events.EnableEventRangeMarks.ShouldBeTrue();
        options.Events.EnableProjectionFloors.ShouldBeTrue();
    }

    [Fact]
    public void per_tenant_event_partitioning_keeps_archived_partitioning_off_unless_asked()
    {
        // A table has one partition scheme, and per-tenant partitioning takes it
        var options = new StoreOptions();
        options.Events.TenancyStyle = TenancyStyle.Conjoined;
        options.EventGraph.UseTenantPartitionedEvents = true;

        options.EventGraph.UseArchivedStreamPartitioning.ShouldBeFalse();
    }

    [Fact]
    public void restore_v5_defaults_puts_every_one_back()
    {
        var options = new StoreOptions();

        #region sample_polecat_restore_v5_defaults
        // Upgrade with the Polecat 5 defaults, then take the new ones one at a time
        options.RestoreV5Defaults();
        #endregion

        options.Events.EnableCorrelationId.ShouldBeFalse();
        options.Events.EnableCausationId.ShouldBeFalse();
        options.Events.EnableHeaders.ShouldBeFalse();
        options.Events.EnableUserName.ShouldBeFalse();
        options.Events.EnableSideEffectsOnInlineProjections.ShouldBeFalse();
        options.Policies.EnableDocumentMetadataByDefault.ShouldBeFalse();
        options.Policies.StartIndexesByTenantId.ShouldBeFalse();
        options.OpenTelemetry.EventCountersEnabled.ShouldBeFalse();
        options.Events.DcbConflictScope.ShouldBe(Polecat.Events.DcbConflictScope.TagValue);
        options.EventGraph.UseArchivedStreamPartitioning.ShouldBeFalse();
        options.Events.EnableLateEventDetection.ShouldBeFalse();
        options.Events.EnableEventRangeMarks.ShouldBeFalse();
        options.Events.EnableProjectionFloors.ShouldBeFalse();

        // Registered after the call, and still restored when the store is built
        var projection = new Polecat.Tests.Projections.PublishingTallyProjection();
        projection.GateSideEffectsBehindPriorVersion.ShouldBeTrue();
        options.Projections.Add(projection, JasperFx.Events.Projections.ProjectionLifecycle.Inline);

        options.ConnectionString = ConnectionSource.ConnectionString;
        using var store = new DocumentStore(options);
        ((Serializer)store.Options.Serializer).Options.AllowOutOfOrderMetadataProperties.ShouldBeFalse();
        projection.GateSideEffectsBehindPriorVersion.ShouldBeFalse();
    }

    [Fact]
    public void without_restoring_a_projection_keeps_the_side_effect_gate()
    {
        var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };
        var projection = new Polecat.Tests.Projections.PublishingTallyProjection();
        options.Projections.Add(projection, JasperFx.Events.Projections.ProjectionLifecycle.Inline);

        using var store = new DocumentStore(options);
        projection.GateSideEffectsBehindPriorVersion.ShouldBeTrue();
        options.Events.DcbConflictScope.ShouldBe(Polecat.Events.DcbConflictScope.TagValueAndEventType);
    }

    [Fact]
    public void restore_v5_defaults_reaches_a_serializer_configured_after_it()
    {
        var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };
        options.RestoreV5Defaults();
        options.ConfigureSerialization(enumStorage: EnumStorage.AsString);

        using var store = new DocumentStore(options);
        ((Serializer)store.Options.Serializer).Options.AllowOutOfOrderMetadataProperties.ShouldBeFalse();
    }

    [Fact]
    public void restore_v5_defaults_leaves_options_you_passed_alone()
    {
        var mine = new JsonSerializerOptions { AllowOutOfOrderMetadataProperties = true };

        var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };
        options.ConfigureSerialization(mine);
        options.RestoreV5Defaults();

        using var store = new DocumentStore(options);
        ((Serializer)store.Options.Serializer).Options.AllowOutOfOrderMetadataProperties.ShouldBeTrue();
    }

    [Fact]
    public void options_you_pass_are_not_given_the_new_default()
    {
        var options = new StoreOptions();
        options.ConfigureSerialization(new JsonSerializerOptions());

        ((Serializer)options.Serializer).Options.AllowOutOfOrderMetadataProperties.ShouldBeFalse();
    }

    [Fact]
    public void a_new_default_can_be_turned_back_on_after_restoring()
    {
        var options = new StoreOptions();
        options.RestoreV5Defaults();
        options.Events.EnableUserName = true;

        options.Events.EnableUserName.ShouldBeTrue();
        options.Events.EnableCorrelationId.ShouldBeFalse();
    }
}

public class polecat_6_default_schema_tests : OneOffConfigurationsContext
{
    public class Shipment
    {
        public Guid Id { get; set; }
        public string Carrier { get; set; } = string.Empty;
        public string Tracking { get; set; } = string.Empty;
    }

    public class Invoice
    {
        public Guid Id { get; set; }
        public string Number { get; set; } = string.Empty;
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<List<string>> columnsAsync(string table)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT c.name FROM sys.columns c
            WHERE c.object_id = OBJECT_ID('[{theStore.Options.DatabaseSchemaName}].[{table}]')
            """;
        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct)) names.Add(reader.GetString(0));
        return names;
    }

    private async Task<List<string>> indexKeyColumnsAsync(string table, string index)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT c.name FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID('[{theStore.Options.DatabaseSchemaName}].[{table}]')
              AND i.name = '{index}' AND ic.key_ordinal > 0
            ORDER BY ic.key_ordinal
            """;
        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct)) names.Add(reader.GetString(0));
        return names;
    }

    private async Task saveAShipmentAsync()
    {
        await using var session = theStore.LightweightSession("tenant-a");
        session.CorrelationId = "corr-1";
        session.CausationId = "cause-1";
        session.LastModifiedBy = "someone";
        session.Store(new Shipment { Id = Guid.NewGuid(), Carrier = "c", Tracking = Guid.NewGuid().ToString() });
        session.Events.StartStream(Guid.NewGuid(), new QuestStarted("Defaults"));
        await session.SaveChangesAsync(Ct);
    }

    private void configure(bool restore, Action<StoreOptions>? more = null)
    {
        ConfigureStore(opts =>
        {
            if (restore) opts.RestoreV5Defaults();
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Schema.For<Shipment>().MultiTenanted()
                .Index(x => x.Carrier)
                .UniqueIndex(x => x.Tracking);
            more?.Invoke(opts);
        });
    }

    [Fact]
    public async Task documents_and_events_store_metadata_by_default()
    {
        configure(restore: false);
        await saveAShipmentAsync();

        var documentColumns = await columnsAsync("pc_doc_shipment");
        foreach (var column in new[] { "correlation_id", "causation_id", "last_modified_by", "headers" })
        {
            documentColumns.ShouldContain(column);
        }

        var eventColumns = await columnsAsync("pc_events");
        foreach (var column in new[] { "correlation_id", "causation_id", "headers", "user_name" })
        {
            eventColumns.ShouldContain(column);
        }

        await using var query = theStore.QuerySession("tenant-a");
        var shipment = await query.Query<Shipment>().SingleAsync(Ct);
        var metadata = await query.MetadataForAsync(shipment, Ct);
        metadata!.CorrelationId.ShouldBe("corr-1");
        metadata.CausationId.ShouldBe("cause-1");
        metadata.LastModifiedBy.ShouldBe("someone");
    }

    [Fact]
    public async Task restored_v5_defaults_add_no_metadata_columns()
    {
        configure(restore: true);
        await saveAShipmentAsync();

        (await columnsAsync("pc_doc_shipment"))
            .ShouldNotContain(x => x == "correlation_id" || x == "causation_id" || x == "last_modified_by" || x == "headers");
        (await columnsAsync("pc_events"))
            .ShouldNotContain(x => x == "correlation_id" || x == "causation_id" || x == "headers" || x == "user_name");
    }

    [Fact]
    public async Task one_document_type_can_turn_a_default_column_off()
    {
        configure(restore: false, opts => opts.Schema.For<Invoice>().Metadata(m => m.Headers.Enabled = false));

        await using (var session = theStore.LightweightSession("tenant-a"))
        {
            session.Store(new Invoice { Id = Guid.NewGuid(), Number = "1" });
            await session.SaveChangesAsync(Ct);
        }

        var columns = await columnsAsync("pc_doc_invoice");
        columns.ShouldNotContain("headers");
        columns.ShouldContain("correlation_id");
    }

    [Fact]
    public async Task one_document_type_can_turn_a_column_on_after_restoring()
    {
        configure(restore: true, opts => opts.Schema.For<Invoice>().Metadata(m => m.CorrelationId.Enabled = true));

        await using (var session = theStore.LightweightSession("tenant-a"))
        {
            session.Store(new Invoice { Id = Guid.NewGuid(), Number = "1" });
            await session.SaveChangesAsync(Ct);
        }

        var columns = await columnsAsync("pc_doc_invoice");
        columns.ShouldContain("correlation_id");
        columns.ShouldNotContain("headers");
    }

    [Fact]
    public async Task conjoined_indexes_start_with_the_tenant_by_default()
    {
        configure(restore: false);
        await saveAShipmentAsync();

        (await indexKeyColumnsAsync("pc_doc_shipment", "ix_pc_doc_shipment_carrier"))
            .ShouldBe(["tenant_id", "cc_carrier"]);

        // A unique index keeps what it enforces
        (await indexKeyColumnsAsync("pc_doc_shipment", "ux_pc_doc_shipment_tracking"))
            .ShouldBe(["cc_tracking"]);
    }

    [Fact]
    public async Task restored_v5_defaults_leave_the_tenant_out_of_the_index()
    {
        configure(restore: true);
        await saveAShipmentAsync();

        (await indexKeyColumnsAsync("pc_doc_shipment", "ix_pc_doc_shipment_carrier"))
            .ShouldBe(["cc_carrier"]);
    }

    [Fact]
    public async Task a_v5_store_upgrades_to_the_new_defaults()
    {
        configure(restore: true);
        await saveAShipmentAsync();

        // The same schema, now on the Polecat 6 defaults: additive columns and a rebuilt index
        configure(restore: false);
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await saveAShipmentAsync();

        (await columnsAsync("pc_doc_shipment")).ShouldContain("last_modified_by");
        (await columnsAsync("pc_events")).ShouldContain("user_name");
        (await indexKeyColumnsAsync("pc_doc_shipment", "ix_pc_doc_shipment_carrier"))
            .ShouldBe(["tenant_id", "cc_carrier"]);

        await using var query = theStore.QuerySession("tenant-a");
        (await query.Query<Shipment>().CountAsync(Ct)).ShouldBe(2);
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync(Ct);
    }
}
