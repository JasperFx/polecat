using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;

namespace Polecat.Subscriptions;

/// <summary>
///     User-facing subscription interface for push-based event processing.
///     Subscriptions process events for side effects (sending emails, publishing
///     to Kafka, etc.) rather than building read models.
/// </summary>
public interface ISubscription
{
    /// <summary>
    ///     Process a page of events. Called by the async daemon as events become available.
    /// </summary>
    Task<IChangeListener> ProcessEventsAsync(
        EventRange page,
        ISubscriptionController controller,
        IDocumentOperations operations,
        CancellationToken cancellationToken);
}
