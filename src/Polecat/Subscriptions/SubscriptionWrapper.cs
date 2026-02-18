using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;

namespace Polecat.Subscriptions;

/// <summary>
///     Wraps a raw ISubscription instance as a SubscriptionBase for registration
///     with the projection graph. Used when the subscription doesn't extend SubscriptionBase directly.
/// </summary>
internal class SubscriptionWrapper : SubscriptionBase
{
    private readonly ISubscription _subscription;

    public SubscriptionWrapper(ISubscription subscription)
    {
        _subscription = subscription;
        Name = subscription.GetType().Name;
    }

    public override Task<IChangeListener> ProcessEventsAsync(
        EventRange page,
        ISubscriptionController controller,
        IDocumentOperations operations,
        CancellationToken cancellationToken) =>
        _subscription.ProcessEventsAsync(page, controller, operations, cancellationToken);
}
