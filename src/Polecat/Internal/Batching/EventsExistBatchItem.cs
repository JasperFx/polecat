using System.Data.Common;
using JasperFx.Events.Tags;
using Polecat.Events;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

internal class EventsExistBatchItem : IBatchQueryItem
{
    private readonly EventGraph _eventGraph;
    private readonly EventTagQuery _query;
    private readonly string _tenantId;
    private readonly TaskCompletionSource<bool> _tcs = new();

    public EventsExistBatchItem(EventGraph eventGraph, EventTagQuery query, string tenantId)
    {
        _eventGraph = eventGraph;
        _query = query;
        _tenantId = tenantId;
    }

    public Task<bool> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder)
        => EventOperations.WriteEventsExistSql(builder, _eventGraph, _query, _tenantId);

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        var exists = false;
        if (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            exists = reader.GetInt32(0) == 1;
        }

        _tcs.SetResult(exists);
    }
}
