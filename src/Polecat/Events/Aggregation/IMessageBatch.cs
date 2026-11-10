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
///     <see cref="JasperFx.Events.Documents.IMessageBatch.BeforeCommitAsync"/> fires inside the projection's SQL
///     transaction (right before <c>COMMIT</c>), <see cref="JasperFx.Events.Documents.IMessageBatch.AfterCommitAsync"/>
///     fires once the projection's database changes are durably committed.
///     Implementations (e.g. a future Wolverine.Polecat bridge) buffer messages
///     in <c>PublishAsync</c> and flush them in one of the two hooks depending
///     on the desired delivery guarantee.
///
///     The default outbox registered with <see cref="EventStoreOptions.MessageOutbox"/>
///     is <see cref="NulloMessageOutbox"/> — a no-op that drops every published
///     message, so projections that don't need messaging incur zero overhead.
///
/// <para>
///     jasperfx#1045 (polecat#806): derives from the shared
///     <see cref="JasperFx.Events.Documents.IMessageBatch" />, which now declares both hooks, so one
///     implementation can serve every store. Kept for one release so existing implementers keep
///     compiling; move to the shared interface.
/// </para>
/// </remarks>
public interface IMessageBatch : JasperFx.Events.Documents.IMessageBatch;
