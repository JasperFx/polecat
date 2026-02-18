using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using JasperFx.Events.Subscriptions;

namespace Polecat.Subscriptions;

/// <summary>
///     Convenience base class for subscriptions. Extends JasperFxSubscriptionBase
///     to integrate with the async daemon framework.
/// </summary>
public abstract class SubscriptionBase
    : JasperFxSubscriptionBase<IDocumentSession, IQuerySession, ISubscription>, ISubscription
{
    protected SubscriptionBase()
    {
        Name = GetType().Name;
    }

    public abstract Task<IChangeListener> ProcessEventsAsync(
        EventRange page,
        ISubscriptionController controller,
        IDocumentOperations operations,
        CancellationToken cancellationToken);
}
