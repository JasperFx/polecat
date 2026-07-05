using Polecat.Services;

namespace Polecat;

/// <summary>
///     Listens to the commit boundary of async daemon projection batches (and subscriptions). Register
///     via <c>StoreOptions.Projections.AsyncListeners</c>, or return one from
///     <c>ISubscription.ProcessEventsAsync</c>. Mirrors Marten's <c>Marten.IChangeListener</c>.
/// </summary>
public interface IChangeListener
{
    /// <summary>
    ///     Runs *after* the projection batch has been committed to the database. Gives "at most once"
    ///     delivery semantics — ideal for post-commit side effects such as cache invalidation, where
    ///     acting before the commit would risk repopulating a cache with stale state.
    /// </summary>
    Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token);

    /// <summary>
    ///     Runs *before* the projection batch is committed to the database. Gives "at least once"
    ///     delivery semantics.
    /// </summary>
    Task BeforeCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token);
}

/// <summary>
///     No-op <see cref="IChangeListener" /> for subscriptions that need no commit hooks.
/// </summary>
public sealed class NullChangeListener : IChangeListener
{
    public static readonly NullChangeListener Instance = new();

    private NullChangeListener()
    {
    }

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        => Task.CompletedTask;

    public Task BeforeCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        => Task.CompletedTask;
}
