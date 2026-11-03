using System.Data;
using System.Data.Common;
using Polecat.Events;
using Polecat.Events.Dcb;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

internal class TombstoneStreamOperation : IStorageOperation, IStreamKeyedOperation,
    Weasel.Storage.NoDataReturnedCall
{
    private readonly EventGraph _events;
    private readonly object _streamId;
    private readonly string _tenantId;

    public TombstoneStreamOperation(EventGraph events, object streamId, string tenantId)
    {
        _events = events;
        _streamId = streamId;
        _tenantId = tenantId;
    }

    public object StreamId => _streamId;
    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Deletion;

    /// <summary>
    ///     Is this the tombstone of that stream? #891: anything queued for a stream in the same save runs
    ///     after this, so something that would write on the stream's behalf needs to ask.
    /// </summary>
    public bool Tombstones(object streamId, string tenantId) => _streamId.Equals(streamId) && _tenantId == tenantId;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        // #891: every statement below filters on these. They are bound here rather than through
        // AddParameters, whose dictionary binds a string as nvarchar, which against a varchar stream key
        // or tenant_id puts CONVERT_IMPLICIT on the column and scans it (#363). Measured at 230 logical
        // reads against 2 on 50,000 rows.
        builder.Append($"DECLARE @id {(_streamId is string ? "varchar(250)" : "uniqueidentifier")} = ");
        builder.AppendParameter(_streamId, _streamId is string ? SqlDbType.VarChar : null);
        builder.Append("; DECLARE @tenant_id varchar(250) = ");
        builder.AppendParameter(_tenantId, SqlDbType.VarChar);
        builder.Append(";\n");

        // #890: a boundary that read this stream's tagged events has to hear about it, or it saves straight
        // through the tombstone, and a tag value nothing carries any more shouldn't outlive the stream. First
        // because it finds the tags through the events and their tag rows, both about to go.
        StreamTagVersionBump.WriteErasureSql(builder, _events);

        // #841: ahead of the events, because this finds the tag rows through them. Everywhere else the
        // foreign key does this, but archived partitioning has none and the rows would be orphaned.
        if (!_events.TagRowsCascadeWithEvents)
        {
            foreach (var registration in _events.TagTypes)
            {
                builder.Append($"""
                    DELETE t FROM {_events.TagTableName(registration)} t
                    INNER JOIN {_events.EventsTableName} e ON {_events.TagToEventJoin("t", "e")}
                    WHERE e.stream_id = @id AND e.tenant_id = @tenant_id;

                    """);
            }
        }

        // #865: the same gap for the natural key tables, which have no foreign key under conjoined
        // tenancy. A row left here keeps the key mapped to a stream that is gone, so reusing the key is
        // refused as a duplicate of a live stream.
        if (!_events.NaturalKeyRowsCascadeWithStreams)
        {
            var streamColumn = _events.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
                ? "stream_id"
                : "stream_key";

            foreach (var definition in _events.NaturalKeyDefinitions())
            {
                builder.Append($"""
                    DELETE FROM {_events.NaturalKeyTableName(definition.AggregateType)}
                    WHERE {streamColumn} = @id AND tenant_id = @tenant_id;

                    """);
            }
        }

        builder.Append($"""
            DELETE FROM {_events.EventsTableName}
            WHERE stream_id = @id AND tenant_id = @tenant_id;
            DELETE FROM {_events.StreamsTableName}
            WHERE id = @id AND tenant_id = @tenant_id;
            """);
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
