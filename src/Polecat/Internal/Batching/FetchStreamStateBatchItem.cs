using System.Data;
using System.Data.Common;
using JasperFx.Events;
using Polecat.Events;
using Polecat.Events.Internal;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

/// <summary>
///     #370: the batched half of <c>QueryEventStore.FetchStreamStateAsync</c>. Reads the same
///     <c>pc_streams</c> column projection through the same <see cref="PcStreamsRowReader" />, so a
///     batched fetch and a standalone one can never drift apart across a schema migration.
/// </summary>
internal class FetchStreamStateBatchItem : IBatchQueryItem
{
    private readonly TaskCompletionSource<StreamState?> _tcs = new();
    private readonly EventGraph _events;
    private readonly object _streamId;
    private readonly string _tenantId;

    public FetchStreamStateBatchItem(EventGraph events, object streamId, string tenantId)
    {
        _events = events;
        _streamId = streamId;
        _tenantId = tenantId;
    }

    public Task<StreamState?> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder)
    {
        builder.Append($"SELECT {PcStreamsRowReader.SelectColumns} FROM {_events.StreamsTableName} WHERE id = ");
        builder.AppendParameter(_streamId, _streamId is string ? SqlDbType.VarChar : null);
        builder.Append(" AND tenant_id = ");
        builder.AppendParameter(_tenantId, SqlDbType.VarChar);
        builder.Append(";\n");
    }

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        if (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            _tcs.SetResult(PcStreamsRowReader.ReadStreamState(reader, _events.StreamIdentity, _events));
        }
        else
        {
            // A stream that does not exist is null, not an error — same answer as the standalone fetch.
            _tcs.SetResult(null);
        }
    }
}
