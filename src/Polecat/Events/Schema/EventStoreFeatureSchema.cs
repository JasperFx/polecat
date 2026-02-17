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

    public EventStoreFeatureSchema(EventGraph events)
        : base("EventStore", new SqlServerMigrator())
    {
        _events = events;
    }

    public override Type StorageType => typeof(EventStoreFeatureSchema);

    protected override IEnumerable<ISchemaObject> schemaObjects()
    {
        // Streams table must be created first (events table references it via FK)
        yield return _events.BuildStreamsTable();
        yield return _events.BuildEventsTable();
        yield return _events.BuildEventProgressionTable();
    }
}
