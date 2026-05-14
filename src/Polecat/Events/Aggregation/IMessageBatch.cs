using JasperFx.Events;

namespace Polecat.Events.Aggregation;

/// <summary>
///     A message-publishing surface scoped to a single projection daemon batch.
///     Projections call <see cref="IMessageSink.PublishAsync{T}(T, string)"/> (or
///     the metadata variant) when they want to emit side-effect messages
///     transactionally with the projection's database changes.
/// </summary>
/// <remarks>
///     The batch is enlisted as a post-commit listener on the projection update —
///     <see cref="BeforeCommitAsync"/> fires inside the projection's SQL
///     transaction (right before <c>COMMIT</c>), <see cref="AfterCommitAsync"/>
///     fires once the projection's database changes are durably committed.
///     Implementations (e.g. a future Wolverine.Polecat bridge) buffer messages
///     in <c>PublishAsync</c> and flush them in one of the two hooks depending
///     on the desired delivery guarantee.
///
///     The default outbox registered with <see cref="EventStoreOptions.MessageOutbox"/>
///     is <see cref="NulloMessageOutbox"/> — a no-op that drops every published
///     message, so projections that don't need messaging incur zero overhead.
/// </remarks>
public interface IMessageBatch : IMessageSink
{
    /// <summary>
    ///     Called inside the projection's database transaction, right before
    ///     <c>COMMIT</c>. Use for "at-least-once" delivery patterns where the
    ///     downstream outbox must be persisted in the same transaction as the
    ///     projection write.
    /// </summary>
    Task BeforeCommitAsync(CancellationToken token);

    /// <summary>
    ///     Called once the projection's database changes are durably committed.
    ///     Use for "best-effort" or external-broker delivery where the messages
    ///     should only fire after the projection write succeeds.
    /// </summary>
    Task AfterCommitAsync(CancellationToken token);
}
