using JasperFx.Events.Projections;

namespace Polecat.Projections;

/// <summary>
///     Base class for event projections that apply arbitrary per-event logic.
///     Supports conventional Project/Create methods or lambda registration.
///
///     Usage:
///     <code>
///     public class QuestLogProjection : EventProjection
///     {
///         public void Project(QuestStarted e, IDocumentSession ops)
///         {
///             ops.Store(new QuestLog { ... });
///         }
///     }
///     </code>
/// </summary>
public abstract class EventProjection
    : JasperFxEventProjectionBase<IDocumentSession, IQuerySession>
{
    protected sealed override void storeEntity<T>(IDocumentSession ops, T entity)
    {
        ops.Store(entity);
    }
}
