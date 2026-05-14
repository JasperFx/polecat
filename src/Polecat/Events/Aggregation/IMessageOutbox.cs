namespace Polecat.Events.Aggregation;

/// <summary>
///     Factory for <see cref="IMessageBatch"/> instances. Registered on the
///     event store via <see cref="EventStoreOptions.MessageOutbox"/>; the
///     projection daemon asks for a fresh batch once per projection update,
///     scoped to the session that's persisting the write.
/// </summary>
/// <remarks>
///     The default outbox is <see cref="NulloMessageOutbox"/>. Downstream
///     integrations (Wolverine.Polecat is the canonical case) ship their own
///     <see cref="IMessageOutbox"/> that vends batches enlisted into their
///     own outgoing-message machinery.
/// </remarks>
public interface IMessageOutbox
{
    /// <summary>
    ///     Build a fresh <see cref="IMessageBatch"/> for the given session.
    ///     Called at most once per projection daemon batch (the first time
    ///     a projection in that batch publishes a message); the returned
    ///     batch is reused for every subsequent publish in the same batch.
    /// </summary>
    ValueTask<IMessageBatch> CreateBatch(IDocumentSession session);
}
