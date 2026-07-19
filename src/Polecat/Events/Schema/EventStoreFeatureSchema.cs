using JasperFx.Events;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Events.Schema;

/// <summary>
///     Groups the event store tables (pc_streams, pc_events, pc_event_progression)
///     into a single Weasel feature schema for coordinated migrations.
/// </summary>
internal class EventStoreFeatureSchema : FeatureSchemaBase
{
    private readonly EventGraph _events;
    private readonly IReadOnlyList<NaturalKeyDefinition> _naturalKeys;

    public EventStoreFeatureSchema(EventGraph events, IReadOnlyList<NaturalKeyDefinition> naturalKeys)
        : base("EventStore", new SqlServerMigrator())
    {
        _events = events;
        _naturalKeys = naturalKeys;
    }

    public override Type StorageType => typeof(EventStoreFeatureSchema);

    protected override IEnumerable<ISchemaObject> schemaObjects()
    {
        // Streams table must be created first (events table references it via FK)
        yield return _events.BuildStreamsTable();

        // When UseArchivedStreamPartitioning is enabled, the EventsTable configures
        // Weasel.SqlServer's Table.SqlServerPartitioning which generates the partition
        // function, scheme, and ON clause DDL automatically.
        yield return _events.BuildEventsTable();
        yield return _events.BuildEventProgressionTable();

        // Per-tenant partitioning registry (#163 / polecat#171). Only materialized when the feature is
        // on, so off-flag stores see no schema delta. The registry (pc_tenant_partitions: tenant_id ->
        // ordinal) is owned by the Weasel.SqlServer ManagedTenantPartitions strategy; the physical
        // partition function/scheme are emitted as part of the partitioned pc_events DDL above, and the
        // per-tenant pc_events_sequence_{ordinal} objects are created on demand at first append.
        // #335: tenant-partitioned documents share the same one-registry-per-database, so the registry
        // also materializes when only the document-side policy is on.
        if (_events.AnyTenantPartitioning)
        {
            foreach (var schemaObject in
                     ((Weasel.Core.Migrations.IFeatureSchema)_events.TenantPartitionManager).Objects)
            {
                yield return schemaObject;
            }
        }

        // Tag tables for DCB support
        foreach (var tagRegistration in _events.TagTypes)
        {
            yield return _events.BuildEventTagTable(tagRegistration);
        }

        // Natural key tables
        foreach (var naturalKey in _naturalKeys)
        {
            yield return new NaturalKeyTable(_events, naturalKey);
        }
    }
}
