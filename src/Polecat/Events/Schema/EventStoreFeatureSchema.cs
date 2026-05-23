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
