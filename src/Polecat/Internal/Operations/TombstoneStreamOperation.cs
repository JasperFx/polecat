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

        // Tombstones in one event store take turns. Deleting a stream deletes through two cascading
        // foreign keys, pc_streams -> pc_events and pc_events -> pc_event_tag_*, and SQL Server runs a
        // cascade with serializable range locks: each one locks the key just past the rows it removes. The
        // two indexes those land on order rows differently (ix_pc_events_stream_and_version by stream,
        // ix_pc_event_tag_*_seq_id by sequence), so two tombstones can each hold the key the other's cascade
        // needs next, and no ordering of this command's statements prevents it. Measured as about one run in
        // twenty of appends_and_tombstones_racing_over_shared_tags_leave_the_table_right, every deadlock
        // graph between two tombstones. Tombstoning is rare and appends don't take this lock, so making
        // tombstones wait for each other costs nothing an application would notice.
        //
        // Owned by the transaction, so it is released at commit or rollback and never outlives the save.
        // sp_getapplock reports a timeout, a cancellation or a deadlock through its return value rather
        // than an error, so a negative answer is thrown here: carrying on without the lock would put the
        // deadlock back.
        //
        // Only inside a transaction, which every save is. Run on its own, each statement below commits as
        // it finishes and keeps nothing for the next one to cross, so there is no cycle to prevent, and a
        // transaction-owned lock can't be taken without a transaction anyway.
        var lockResource = SqlEscaping.LiteralBody($"polecat:tombstone:{_events.EventsTableName}");
        builder.Append($"""
            IF @@TRANCOUNT > 0
            BEGIN
                DECLARE @pc_tombstone_lock int;
                EXEC @pc_tombstone_lock = sp_getapplock @Resource = '{lockResource}', @LockMode = 'Exclusive',
                    @LockOwner = 'Transaction', @LockTimeout = -1;
                IF @pc_tombstone_lock < 0
                    THROW 51000, 'Polecat could not take the tombstone lock for this event store.', 1;
            END;

            """);

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

        // A seek, so a tombstone only ever touches its own stream's rows. Left to itself, on a small or
        // young table the optimizer scans the clustered index, taking a U lock on every row it passes, and
        // two tombstones then deadlock: one scans onto a row the other has just deleted, while the other's
        // DELETE FROM pc_streams cascades into pc_events with serializable range locks on
        // ix_pc_events_stream_and_version, the next key past its own stream being a row the first has
        // deleted. The cascade's range locks alone cannot close a cycle, because a stream's keys are
        // contiguous in that index; the scan is what closes it. Captured from both deadlock graphs on
        // tombstone_tag_erasure_concurrency_tests.appends_and_tombstones_racing_over_shared_tags_leave_the_table_right.
        //
        // SQL Server refuses FORCESEEK on the target of a DELETE, so the seek is on a read of the stream's
        // keys and the target is reached from those by its primary key, the same shape as
        // StreamTagVersionBump's RowsOfTheStreamsTags. Every key column the join names leads the clustered
        // key in each layout this runs under.
        //
        // Not under per-tenant partitioning: there the index leads with tenant_ordinal, which this
        // predicate doesn't bind, so there is no index to seek and FORCESEEK would refuse the statement
        // outright (error 8622). See EventGraph.StreamEventsSql for why that layout can't always name it.
        var deleteEvents = _events.UseTenantPartitionedEvents
            ? $"""
               DELETE FROM {_events.EventsTableName}
               WHERE stream_id = @id AND tenant_id = @tenant_id;
               """
            : $"""
               DELETE e FROM (
                       SELECT s.seq_id FROM {_events.EventsTableName} s WITH (FORCESEEK)
                       WHERE s.stream_id = @id AND s.tenant_id = @tenant_id
                   ) x
                   INNER LOOP JOIN {_events.EventsTableName} e ON e.seq_id = x.seq_id;
               """;

        builder.Append(deleteEvents);
        builder.Append($"""

            DELETE FROM {_events.StreamsTableName}
            WHERE id = @id AND tenant_id = @tenant_id;
            """);
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
