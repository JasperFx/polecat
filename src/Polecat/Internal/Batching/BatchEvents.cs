using JasperFx.Events;
using Polecat.Batching;
using Polecat.Events;

namespace Polecat.Internal.Batching;

/// <summary>
///     #370: <see cref="IBatchEvents" /> over <see cref="BatchedQuery" />. Each call appends one item to
///     the batch and hands back its unresolved task; the tasks complete when
///     <see cref="BatchedQuery.Execute" /> walks the result sets.
/// </summary>
internal class BatchEvents : IBatchEvents
{
    private readonly BatchedQuery _parent;
    private readonly QuerySession _session;

    public BatchEvents(BatchedQuery parent, QuerySession session)
    {
        _parent = parent;
        _session = session;
    }

    public Task<StreamState?> FetchStreamState(Guid streamId) => AddStateItem(streamId);

    public Task<StreamState?> FetchStreamState(string streamKey)
        => AddStateItem(streamKey ?? throw new ArgumentNullException(nameof(streamKey)));

    public Task<IReadOnlyList<IEvent>> FetchStream(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0)
        => AddStreamItem(streamId, version, timestamp, fromVersion);

    public Task<IReadOnlyList<IEvent>> FetchStream(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0)
        => AddStreamItem(streamKey ?? throw new ArgumentNullException(nameof(streamKey)),
            version, timestamp, fromVersion);

    private Task<StreamState?> AddStateItem(object streamId)
    {
        var item = new FetchStreamStateBatchItem(EventGraph(), streamId, _session.TenantId);
        _parent.RequireEventStore();
        _parent.AddItem(item);
        return item.Result;
    }

    private Task<IReadOnlyList<IEvent>> AddStreamItem(object streamId, long version,
        DateTimeOffset? timestamp, long fromVersion)
    {
        var item = new FetchStreamBatchItem(EventGraph(), _session.Serializer, streamId, _session.TenantId,
            version, timestamp, fromVersion);
        _parent.RequireEventStore();
        _parent.AddItem(item);
        return item.Result;
    }

    private EventGraph EventGraph() => _session.Options.EventGraph;
}
