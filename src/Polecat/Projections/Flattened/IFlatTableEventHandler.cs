using JasperFx.Events;

namespace Polecat.Projections.Flattened;

/// <summary>
///     Internal handler for a single event type in a FlatTableProjection.
/// </summary>
internal interface IFlatTableEventHandler
{
    void Compile(Events.EventGraph events);
    FlatTableSqlOperation CreateOperation(IEvent e);
}
