using JasperFx.Events;

namespace Polecat.Projections.Vectors;

/// <summary>
///     Declares which events contribute an embedding's text, and which retract it.
/// </summary>
public sealed class VectorProjectionMap<TDoc, TId>
    where TDoc : class, IVectorized<TId>, new()
    where TId : notnull
{
    private readonly Dictionary<Type, Mapping> _content = new();
    private readonly Dictionary<Type, Func<IEvent, TId>> _deletes = new();

    internal bool IsEmpty => _content.Count == 0 && _deletes.Count == 0;

    /// <summary>An event that carries text worth embedding, and the document id it belongs to.</summary>
    public VectorProjectionMap<TDoc, TId> Map<TEvent>(
        Func<IEvent<TEvent>, string?> content,
        Func<IEvent<TEvent>, TId> id) where TEvent : notnull
    {
        ArgumentNullException.ThrowIfNull(content);
        ArgumentNullException.ThrowIfNull(id);

        if (_content.ContainsKey(typeof(TEvent)))
        {
            throw new InvalidOperationException(
                $"'{typeof(TEvent).Name}' is already mapped. Two mappings for one event type would "
                + "leave which one wins depending on registration order.");
        }

        _content[typeof(TEvent)] = new Mapping(
            e => content((IEvent<TEvent>)e),
            e => id((IEvent<TEvent>)e));

        return this;
    }

    /// <summary>
    ///     An event that retracts a document from the index.
    /// </summary>
    /// <remarks>
    ///     ⚠️ <b>There is deliberately no overload without an id selector.</b> Marten's template reads
    ///     <c>@event.StreamId</c> unconditionally and ignores the configured selector, so a projection
    ///     keyed on a payload member deletes nothing and the row stays in the index forever. Making the
    ///     two structurally incapable of disagreeing beats checking that they agree, and the common case
    ///     costs <c>e =&gt; e.StreamId</c>.
    /// </remarks>
    public VectorProjectionMap<TDoc, TId> Delete<TEvent>(Func<IEvent<TEvent>, TId> id)
        where TEvent : notnull
    {
        ArgumentNullException.ThrowIfNull(id);

        if (_content.ContainsKey(typeof(TEvent)))
        {
            throw new InvalidOperationException(
                $"'{typeof(TEvent).Name}' is mapped for content and for deletion, so one event would "
                + "both write and remove the same document and the outcome would depend on which "
                + "branch ran first.");
        }

        if (!_deletes.TryAdd(typeof(TEvent), e => id((IEvent<TEvent>)e)))
        {
            throw new InvalidOperationException($"'{typeof(TEvent).Name}' is already mapped for deletion.");
        }

        return this;
    }

    internal bool TryDelete(IEvent @event, out TId id)
    {
        if (_deletes.TryGetValue(@event.EventType, out var selector))
        {
            id = selector(@event);
            return true;
        }

        id = default!;
        return false;
    }

    internal bool TryContent(IEvent @event, out TId id, out string? content)
    {
        if (_content.TryGetValue(@event.EventType, out var mapping))
        {
            // ⚠️ NOT wrapped in a try/catch, and that is the point. Marten's template catches
            // everything a selector throws and returns null, which the caller reads as "no content for
            // this event" -- so a selector with a bug drops the document out of the index with nothing
            // reported anywhere. A throw here faults the shard, which is what the daemon's error
            // handling is for and is the only outcome an operator can act on.
            id = mapping.Id(@event);
            content = mapping.Content(@event);
            return true;
        }

        id = default!;
        content = null;
        return false;
    }

    private sealed record Mapping(Func<IEvent, string?> Content, Func<IEvent, TId> Id);
}
