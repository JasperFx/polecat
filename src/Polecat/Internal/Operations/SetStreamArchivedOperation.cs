using System.Data;
using System.Data.Common;
using Polecat.Events;
using Polecat.Events.Dcb;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

/// <summary>
///     Flips <c>is_archived</c> on a stream and all of its events. Archive and un-archive differ only
///     in the flag value, so both ride this one operation (polecat#318).
/// </summary>
/// <remarks>
///     The flip also moves the <c>pc_dcb_tag_version</c> row of every tag on the stream's events (#841). A
///     tag read excludes archived events, so without that, a boundary that read a tag while the stream was
///     archived would save straight through an un-archive that brought the tag's events back: a decision
///     made without them. See <see cref="StreamTagVersionBump" />.
/// </remarks>
internal class SetStreamArchivedOperation : IStorageOperation, IStreamKeyedOperation,
    Weasel.Storage.NoDataReturnedCall
{
    private readonly EventGraph _events;
    private readonly object _streamId;
    private readonly string _tenantId;
    private readonly bool _archived;

    public SetStreamArchivedOperation(EventGraph events, object streamId, string tenantId, bool archived)
    {
        _events = events;
        _streamId = streamId;
        _tenantId = tenantId;
        _archived = archived;
    }

    public object StreamId => _streamId;
    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        // A literal rather than a parameter so SQL Server can still eliminate partitions at compile
        // time where is_archived participates in the partition scheme.
        var flag = _archived ? 1 : 0;

        // Bound here rather than through AddParameters, whose dictionary binds a string as nvarchar, which
        // against a varchar stream key or tenant_id puts CONVERT_IMPLICIT on the column and scans it (#363).
        // Measured on 50,000 string streams: 456 logical reads against 3 on pc_streams, and 309 against 6
        // on pc_events. The tombstone does the same.
        builder.Append($"DECLARE @id {(_streamId is string ? "varchar(250)" : "uniqueidentifier")} = ");
        builder.AppendParameter(_streamId, _streamId is string ? SqlDbType.VarChar : null);
        builder.Append("; DECLARE @tenant_id varchar(250) = ");
        builder.AppendParameter(_tenantId, SqlDbType.VarChar);
        builder.Append(";\n");

        builder.Append($"""
            UPDATE {_events.StreamsTableName} SET is_archived = {flag}
            WHERE id = @id AND tenant_id = @tenant_id;
            UPDATE {_events.EventsTableName} SET is_archived = {flag}
            WHERE stream_id = @id AND tenant_id = @tenant_id;
            """);

        StreamTagVersionBump.WriteSql(builder, _events);
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
