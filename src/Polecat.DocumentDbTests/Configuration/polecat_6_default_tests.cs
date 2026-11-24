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
///     <see cref="StoreOptions.RestoreV5Defaults" /> putting each one back. #1134 (marten#6144) put three of them
///     back to their Polecat 5 default before 6.0: event metadata, document metadata and
///     <see cref="StorePolicies.StartIndexesByTenantId" />.
/// </summary>
public class polecat_6_default_tests
{
    // #1134 (marten#6144): the optional event metadata stays off by default, as in Polecat 5. The 6.0 alphas
    // turned it on (#1032)
    [Fact]
    public void event_metadata_is_off_by_default()
    {
        var options = new StoreOptions();

        options.Events.EnableCorrelationId.ShouldBeFalse();
        options.Events.EnableCausationId.ShouldBeFalse();
        options.Events.EnableHeaders.ShouldBeFalse();
        options.Events.EnableUserName.ShouldBeFalse();
    }

    // #1134 (marten#6144): document metadata and tenant-leading indexes stay opt-in, as in Polecat 5
    [Fact]
    public void document_metadata_and_tenant_leading_indexes_are_off_by_default()
    {
        var options = new StoreOptions();

        options.Policies.EnableDocumentMetadataByDefault.ShouldBeFalse();
        options.Policies.StartIndexesByTenantId.ShouldBeFalse();
    }

    [Fact]
    public void settings_turned_on_before_restoring_stay_on()
    {
        var options = new StoreOptions();
        options.Events.EnableCorrelationId = true;
        options.Events.EnableCausationId = true;
        options.Policies.EnableDocumentMetadataByDefault = true;
        options.Policies.StartIndexesByTenantId = true;

        options.RestoreV5Defaults();

        // Nothing to restore for these three since #1134, so RestoreV5Defaults() leaves them as they were set
        options.Events.EnableCorrelationId.ShouldBeTrue();
        options.Events.EnableCausationId.ShouldBeTrue();
        options.Events.EnableHeaders.ShouldBeFalse();
        options.Events.EnableUserName.ShouldBeFalse();
        options.Policies.EnableDocumentMetadataByDefault.ShouldBeTrue();
        options.Policies.StartIndexesByTenantId.ShouldBeTrue();
    }

    [Fact]
    public void the_rest_of_the_new_defaults()
    {
        var options = new StoreOptions();

        options.Events.EnableSideEffectsOnInlineProjections.ShouldBeTrue();
        options.OpenTelemetry.EventCountersEnabled.ShouldBeTrue();
        ((Serializer)options.Serializer).Options.AllowOutOfOrderMetadataProperties.ShouldBeTrue();
        options.Events.EnableLateEventDetection.ShouldBeTrue();
        options.Events.EnableEventRangeMarks.ShouldBeTrue();
        options.Events.EnableProjectionFloors.ShouldBeTrue();
        options.Events.EnableParkedIdentities.ShouldBeTrue();
        options.Events.EnableExtendedProgressionTracking.ShouldBeTrue();
    }

    [Fact]
    public void a_failing_async_projection_parks_by_default()
    {
        // #1053 (marten#6003): park the failing identity, or pause where a projection can't park
        var options = new StoreOptions();

        options.Projections.Errors.ApplyErrors.ShouldBe(JasperFx.Events.Daemon.ProjectionErrorAction.Park);
        options.Projections.Errors.SerializationErrors.ShouldBe(JasperFx.Events.Daemon.ProjectionErrorAction.Park);
        options.Projections.Errors.SkipUnknownEvents.ShouldBeTrue();

        // A rebuild still pauses
        options.Projections.RebuildErrors.ApplyErrors.ShouldBe(JasperFx.Events.Daemon.ProjectionErrorAction.Pause);
    }

    [Fact]
    public void archived_stream_partitioning_is_off_by_default()
    {
        // Opt-in, as in Polecat 5. It was on by default in the 6.0 alphas (#1041)
        new StoreOptions().EventGraph.UseArchivedStreamPartitioning.ShouldBeFalse();
    }

    [Fact]
    public void per_tenant_event_partitioning_keeps_archived_partitioning_off_unless_asked()
    {
        // A table has one partition scheme, and per-tenant partitioning takes it, so setting both is refused
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
        options.Events.EnableParkedIdentities.ShouldBeFalse();
        options.Events.EnableExtendedProgressionTracking.ShouldBeFalse();
        options.Projections.Errors.SkipApplyErrors.ShouldBeTrue();
        options.Projections.Errors.SkipSerializationErrors.ShouldBeTrue();

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
        options.Events.EnableLateEventDetection = true;

        options.Events.EnableLateEventDetection.ShouldBeTrue();
        options.Events.EnableEventRangeMarks.ShouldBeFalse();
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

    // The settings the 6.0 alphas and rc.1 had on by default, which #1134 (marten#6144) made opt-in again
    private static void turnOnTheOptInMetadataAndIndexes(StoreOptions opts)
    {
        opts.Events.EnableAllEventMetadata();
        opts.Policies.EnableDocumentMetadataByDefault = true;
        opts.Policies.StartIndexesByTenantId = true;
    }

    private async Task<int> eventsWithMetadataAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT COUNT(*) FROM [{theStore.Options.DatabaseSchemaName}].[pc_events]
            WHERE correlation_id IS NOT NULL OR causation_id IS NOT NULL OR headers IS NOT NULL OR user_name IS NOT NULL
            """;
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    [Fact]
    public async Task documents_and_events_store_metadata_when_turned_on()
    {
        configure(restore: false, turnOnTheOptInMetadataAndIndexes);
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

        (await eventsWithMetadataAsync()).ShouldBe(1);

        await using var query = theStore.QuerySession("tenant-a");
        var shipment = await query.Query<Shipment>().SingleAsync(Ct);
        var metadata = await query.MetadataForAsync(shipment, Ct);
        metadata!.CorrelationId.ShouldBe("corr-1");
        metadata.CausationId.ShouldBe("cause-1");
        metadata.LastModifiedBy.ShouldBe("someone");
    }

    // #1134: off by default, as in Polecat 5, so the default and RestoreV5Defaults() give the same schema
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task no_document_metadata_columns_and_no_event_metadata_written_by_default(bool restore)
    {
        configure(restore);
        await saveAShipmentAsync();

        (await columnsAsync("pc_doc_shipment"))
            .ShouldNotContain(x => x == "correlation_id" || x == "causation_id" || x == "last_modified_by" || x == "headers");

        // #1116 (marten#5765): pc_events has the metadata columns whatever the flags say, and the flags decide
        // whether an append writes them
        var eventColumns = await columnsAsync("pc_events");
        foreach (var column in new[] { "correlation_id", "causation_id", "headers", "user_name" })
        {
            eventColumns.ShouldContain(column);
        }

        (await eventsWithMetadataAsync()).ShouldBe(0);
    }

    [Fact]
    public async Task one_document_type_can_turn_a_store_wide_column_off()
    {
        configure(restore: false, opts =>
        {
            opts.Policies.EnableDocumentMetadataByDefault = true;
            opts.Schema.For<Invoice>().Metadata(m => m.Headers.Enabled = false);
        });

        await using (var session = theStore.LightweightSession("tenant-a"))
        {
            session.Store(new Invoice { Id = Guid.NewGuid(), Number = "1" });
            await session.SaveChangesAsync(Ct);
        }

        var columns = await columnsAsync("pc_doc_invoice");
        columns.ShouldNotContain("headers");
        columns.ShouldContain("correlation_id");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task one_document_type_can_turn_a_column_on(bool restore)
    {
        configure(restore, opts => opts.Schema.For<Invoice>().Metadata(m => m.CorrelationId.Enabled = true));

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
    public async Task conjoined_indexes_start_with_the_tenant_when_turned_on()
    {
        configure(restore: false, opts => opts.Policies.StartIndexesByTenantId = true);
        await saveAShipmentAsync();

        (await indexKeyColumnsAsync("pc_doc_shipment", "ix_pc_doc_shipment_carrier"))
            .ShouldBe(["tenant_id", "cc_carrier"]);

        // A unique index keeps what it enforces: a global one stays global
        (await indexKeyColumnsAsync("pc_doc_shipment", "ux_pc_doc_shipment_tracking"))
            .ShouldBe(["cc_tracking"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task conjoined_indexes_leave_the_tenant_out_by_default(bool restore)
    {
        configure(restore);
        await saveAShipmentAsync();

        (await indexKeyColumnsAsync("pc_doc_shipment", "ix_pc_doc_shipment_carrier"))
            .ShouldBe(["cc_carrier"]);
    }

    [Fact]
    public async Task a_v5_store_upgrades_to_the_new_defaults()
    {
        configure(restore: true);
        await saveAShipmentAsync();

        // The same schema, now on the Polecat 6 defaults. Since #1134 neither the document table nor its
        // indexes change; pc_events already has its metadata columns (#1116)
        configure(restore: false);
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await saveAShipmentAsync();

        (await columnsAsync("pc_doc_shipment")).ShouldNotContain("last_modified_by");
        (await columnsAsync("pc_events")).ShouldContain("user_name");
        (await indexKeyColumnsAsync("pc_doc_shipment", "ix_pc_doc_shipment_carrier"))
            .ShouldBe(["cc_carrier"]);

        await using var query = theStore.QuerySession("tenant-a");
        (await query.Query<Shipment>().CountAsync(Ct)).ShouldBe(2);
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync(Ct);
    }

    // #1134: a store created by a 6.0 alpha or rc.1 with their defaults has the four document metadata columns and
    // tenant-leading indexes. On the new defaults the columns are kept (the document table is additive, #267) and
    // left NULL from then on; the index is rebuilt without tenant_id, because an index is declared in full.
    [Fact]
    public async Task an_rc1_store_keeps_its_metadata_columns_on_the_new_defaults()
    {
        configure(restore: false, turnOnTheOptInMetadataAndIndexes);
        await saveAShipmentAsync();

        configure(restore: false);
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await saveAShipmentAsync();

        var documentColumns = await columnsAsync("pc_doc_shipment");
        foreach (var column in new[] { "correlation_id", "causation_id", "last_modified_by", "headers" })
        {
            documentColumns.ShouldContain(column);
        }

        (await indexKeyColumnsAsync("pc_doc_shipment", "ix_pc_doc_shipment_carrier"))
            .ShouldBe(["cc_carrier"]);

        // Only the first append wrote event metadata
        (await eventsWithMetadataAsync()).ShouldBe(1);

        await using var query = theStore.QuerySession("tenant-a");
        (await query.Query<Shipment>().CountAsync(Ct)).ShouldBe(2);
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync(Ct);
    }

    // #1134: how a store that ran rc.1 keeps its tenant-leading indexes as they are
    [Fact]
    public async Task an_rc1_store_keeps_its_indexes_by_opting_in()
    {
        configure(restore: false, turnOnTheOptInMetadataAndIndexes);
        await saveAShipmentAsync();

        configure(restore: false, opts => opts.Policies.StartIndexesByTenantId = true);
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        (await indexKeyColumnsAsync("pc_doc_shipment", "ix_pc_doc_shipment_carrier"))
            .ShouldBe(["tenant_id", "cc_carrier"]);
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync(Ct);
    }
}
