using System.Linq.Expressions;
using JasperFx.Events;
using Polecat.Linq.Members;

namespace Polecat.Events.Linq;

/// <summary>
///     Resolves <see cref="StreamState"/> property expressions to SQL column references on the
///     <c>pc_streams</c> table, for <see cref="QueryEventStore.QueryStreamStates"/> (jasperfx#740).
///     Every public get member of <see cref="StreamState"/> translates; anything else — a member of
///     a nested object like <c>AggregateType.Name</c> included — throws a
///     <see cref="NotSupportedException"/> naming the member, never silently matching all rows
///     (an ignored predicate returns unfiltered streams that read as filtered, the jasperfx#737
///     failure mode the whole surface refuses).
/// </summary>
internal class StreamStateMemberFactory : IMemberResolver
{
    private readonly EventGraph _events;

    public StreamStateMemberFactory(EventGraph events)
    {
        _events = events;
    }

    public IQueryableMember ResolveMember(MemberExpression expression)
    {
        // Id and Key are the same column under the two identity styles, exactly like
        // StreamId/StreamKey on the events queryable.
        return expression.Member.Name switch
        {
            nameof(StreamState.Id) => new QueryableMember("id", "id", typeof(Guid)),
            nameof(StreamState.Key) => new QueryableMember("id", "id", typeof(string)),
            nameof(StreamState.Version) => new QueryableMember("version", "version", typeof(long)),
            nameof(StreamState.AggregateType) => new AggregateTypeQueryableMember(_events),
            nameof(StreamState.LastTimestamp) =>
                new QueryableMember("timestamp", "timestamp", typeof(DateTimeOffset)),
            nameof(StreamState.Created) => new QueryableMember("created", "created", typeof(DateTimeOffset)),
            nameof(StreamState.IsArchived) => new QueryableMember("is_archived", "is_archived", typeof(bool)),
            nameof(StreamState.CompactedVersion) =>
                new QueryableMember("compacted_version", "compacted_version", typeof(long)),
            _ => throw new NotSupportedException(
                $"Polecat cannot translate the member '{expression.Member.DeclaringType?.Name}.{expression.Member.Name}' " +
                $"in a stream state query. Translatable members are the public properties of {nameof(StreamState)}: " +
                $"{nameof(StreamState.Id)}, {nameof(StreamState.Key)}, {nameof(StreamState.Version)}, " +
                $"{nameof(StreamState.AggregateType)}, {nameof(StreamState.LastTimestamp)}, " +
                $"{nameof(StreamState.Created)}, {nameof(StreamState.IsArchived)}, " +
                $"{nameof(StreamState.CompactedVersion)}.")
        };
    }

    /// <summary>
    ///     The <c>x.AggregateType == typeof(X)</c> translation — the Stream Compaction Policy's
    ///     selector. The column stores the aggregate-type alias (the simple type name, stamped by
    ///     <see cref="EventGraph.AggregateAliasFor"/> on the stream insert), so the comparison
    ///     value converts a CLR <see cref="Type"/> to that same alias through the same method —
    ///     one spelling, both directions.
    /// </summary>
    private sealed class AggregateTypeQueryableMember : IQueryableMember
    {
        private readonly EventGraph _events;

        public AggregateTypeQueryableMember(EventGraph events)
        {
            _events = events;
        }

        public Type MemberType => typeof(string);
        public string TypedLocator => "type";
        public string RawLocator => "type";
        public bool IsBoolean => false;

        public object? ConvertValue(object? value)
            => value switch
            {
                null => null,
                Type aggregateType => _events.AggregateAliasFor(aggregateType),
                string alias => alias,
                _ => throw new NotSupportedException(
                    $"{nameof(StreamState)}.{nameof(StreamState.AggregateType)} can only be compared against a " +
                    $"CLR Type (x.AggregateType == typeof(X)) or null, not {value.GetType().FullName}.")
            };
    }
}
