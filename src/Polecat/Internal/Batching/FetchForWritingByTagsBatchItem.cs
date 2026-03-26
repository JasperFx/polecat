using System.Data.Common;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events;
using Polecat.Events.Dcb;
using Polecat.Internal.Operations;
using Polecat.Serialization;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

internal class FetchForWritingByTagsBatchItem<T> : IBatchQueryItem where T : class
{
    private readonly EventGraph _eventGraph;
    private readonly EventTagQuery _query;
    private readonly DocumentSessionBase _session;
    private readonly ISerializer _serializer;
    private readonly string _tenantId;
    private readonly TaskCompletionSource<IEventBoundary<T>> _tcs = new();

    public FetchForWritingByTagsBatchItem(EventGraph eventGraph, EventTagQuery query,
        DocumentSessionBase session, ISerializer serializer)
    {
        _eventGraph = eventGraph;
        _query = query;
        _session = session;
        _serializer = serializer;
        _tenantId = session.TenantId;
    }

    public Task<IEventBoundary<T>> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder)
    {
        EventOperations.WriteTagQuerySql(builder, _eventGraph, _query, _tenantId);
    }

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        var events = new List<IEvent>();
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            var @event = EventOperations.ReadEventFromReader(reader, _serializer, _eventGraph);
            if (@event != null) events.Add(@event);
        }

        var lastSeenSequence = events.Count > 0 ? events.Max(e => e.Sequence) : 0;

        T? aggregate = default;
        if (events.Count > 0)
        {
            var aggregator = _session.Options.Projections.AggregatorFor<T>();
            if (aggregator != null)
            {
                aggregate = await aggregator.BuildAsync(events, _session, default, token).ConfigureAwait(false);
            }
        }

        _session.WorkTracker.Add(new AssertDcbConsistencyOperation(_eventGraph, _query, lastSeenSequence, _tenantId));

        _tcs.SetResult(new EventBoundary<T>(_session, _eventGraph, aggregate, events, lastSeenSequence));
    }
}
