using JasperFx.Events;
using JasperFx.Events.Aggregation;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Groups the event store tables (pc_streams, pc_events, pc_event_progression, and the tables
///     flat table projections write to) into a single Weasel feature schema for coordinated migrations.
/// </summary>
internal class EventStoreFeatureSchema : FeatureSchemaBase
{
    private readonly EventGraph _events;
    private readonly IReadOnlyList<NaturalKeyDefinition> _naturalKeys;
    private readonly IReadOnlyList<Table> _flatTables;

    /// <summary>
    ///     One answer to "what does the event store declare", because the whole-database schema and a
    ///     session's first use both have to apply it and must not disagree.
    /// </summary>
    public EventStoreFeatureSchema(StoreOptions options)
        : base("EventStore", new SqlServerMigrator())
    {
        _events = options.EventGraph;

        _naturalKeys = options.Projections.All
            .OfType<IAggregateProjection>()
            .Where(p => p.NaturalKeyDefinition != null)
            .Select(p => p.NaturalKeyDefinition!)
            .ToList();

        _flatTables = options.Projections.AllFlatTableProjections()
            .Select(p => p.Table)
            .DistinctBy(t => t.Identifier.QualifiedName, StringComparer.OrdinalIgnoreCase)
            .ToList();
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

        // #813 (jasperfx#809): projection start floors, OPT-IN in Polecat 5
        // (Events.EnableProjectionFloors) so upgrading adds no table and no schema delta to an existing
        // store. Planned to become opt-out in Polecat 6.
        if (_events.EnableProjectionFloors)
        {
            yield return _events.BuildEventProjectionFloorsTable();
        }

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

        // gh-515: the side table the DCB boundary check serializes on. One table for every registered
        // tag type, so it materializes once as soon as any tag type exists — a store with no tag types
        // has no DCB boundaries to guard and sees no schema delta.
        if (_events.TagTypes.Count > 0)
        {
            yield return _events.BuildDcbTagVersionTable();

            // #890: every value that table holds comes from here, so a row deleted by a tombstone and
            // created again can never match a capture of the old one.
            yield return _events.BuildDcbTagVersionSequence();
        }

        // Natural key tables
        foreach (var naturalKey in _naturalKeys)
        {
            yield return new NaturalKeyTable(_events, naturalKey);
        }

        // #882: the projection used to create its own table the first time an event reached it, which
        // kept it away from AutoCreate, from the migration lock and from every database but the first.
        foreach (var flatTable in _flatTables)
        {
            yield return flatTable;
        }
    }
}
