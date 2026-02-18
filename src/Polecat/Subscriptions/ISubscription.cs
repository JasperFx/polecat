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

/// <summary>
///     Optional listener for post-commit actions. Returned by ISubscription.ProcessEventsAsync.
/// </summary>
public interface IChangeListener
{
    Task AfterCommitAsync(CancellationToken token);
}

/// <summary>
///     No-op change listener for subscriptions that don't need post-commit hooks.
/// </summary>
public sealed class NullChangeListener : IChangeListener
{
    public static readonly NullChangeListener Instance = new();

    private NullChangeListener()
    {
    }

    public Task AfterCommitAsync(CancellationToken token) => Task.CompletedTask;
}
