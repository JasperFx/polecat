using JasperFx.Events;

namespace Polecat.Events.Aggregation;

/// <summary>
///     The default <see cref="IMessageOutbox"/> + <see cref="IMessageBatch"/>:
///     drops every message, fires no commit hooks. Apps that don't integrate
///     a message bus (most of them) pay zero overhead.
/// </summary>
internal sealed class NulloMessageOutbox : IMessageOutbox, IMessageBatch
{
    public static readonly NulloMessageOutbox Instance = new();

    private NulloMessageOutbox()
    {
    }

    public ValueTask<IMessageBatch> CreateBatch(IDocumentSession session) =>
        new(this);

    public ValueTask PublishAsync<T>(T message, string tenantId) =>
        ValueTask.CompletedTask;

    public ValueTask PublishAsync<T>(T message, MessageMetadata metadata) =>
        ValueTask.CompletedTask;

    public Task BeforeCommitAsync(CancellationToken token) => Task.CompletedTask;

    public Task AfterCommitAsync(CancellationToken token) => Task.CompletedTask;
}
