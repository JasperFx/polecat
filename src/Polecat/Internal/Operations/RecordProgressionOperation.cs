using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using Weasel.SqlServer;
using JasperFx.Core.Exceptions;

namespace Polecat.Internal.Operations;

/// <summary>
///     Writes a shard's progression high-water mark. Two axes vary independently (polecat#318):
///     <paramref name="upsert" /> picks MERGE (the row may not exist yet — Floor == 0) versus a plain
///     UPDATE (Floor > 0, so the row is already there), and extended tracking adds the heartbeat
///     column. The SET clause is shared by both shapes.
/// </summary>
/// <remarks>
///     #1060: a daemon batch passes the floor its range was built from, and the UPDATE only moves the row when it
///     is still there, as Marten's <c>UpdateProjectionProgress</c> does. A row that has moved (a second daemon on
///     the shard, or a rewind under the batch) fails the batch with
///     <see cref="ProgressionProgressOutOfOrderException" /> instead of overwriting it. JasperFx's per-identity
///     rebuild relies on it: an idle shard carries the rebuild in an empty <c>(C, C]</c> batch, and that batch has
///     to fail with everything in it when the progression moved.
///     <para>
///         #1112 (marten#6127): the first commit, the MERGE at floor 0, finds a row already there when the shard
///         failed before it ever committed, because the failure is written at the shard's start position
///         (<c>PolecatDatabase.WriteExtendedProgressionAsync</c>). That row has no progress in it, so the MERGE
///         takes it over and clears the failure it carried, which this commit has moved past.
///     </para>
/// </remarks>
internal class RecordProgressionOperation : IStorageOperation, Weasel.Storage.NoDataReturnedCall, IExceptionTransform
{
    // Raised by the floor check below and mapped back by TryTransform
    internal const int ProgressionOutOfOrderErrorNumber = 51013;

    private readonly string _progressionTableName;
    private readonly string _name;
    private readonly long _ceiling;
    private readonly bool _extendedTracking;
    private readonly bool _upsert;
    private readonly bool _unicode; // #935
    private readonly long? _expectedFloor; // #1060

    public RecordProgressionOperation(
        string progressionTableName, string name, long ceiling, bool extendedTracking, bool upsert,
        bool unicode = false, long? expectedFloor = null)
    {
        _progressionTableName = progressionTableName;
        _name = name;
        _ceiling = ceiling;
        _extendedTracking = extendedTracking;
        _upsert = upsert;
        _unicode = unicode;
        _expectedFloor = expectedFloor;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var set = _extendedTracking
            ? "last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET(), heartbeat = SYSDATETIMEOFFSET()"
            : "last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()";

        // #916: varchar, as the column is, and the same mistake as #363. As nvarchar the column converts
        // rather than the parameter, so this walks the table, and with these update locks that means
        // waiting for the open batch of every shard it passes on the way to its own row.
        if (_upsert)
        {
            // #1112: a row at 0 may carry the failure of a shard that paused before its first commit. Moving past
            // 0 moves past that failure, so it goes, as it does on Fisher. Marten deletes the row and inserts.
            var matchedSet = _extendedTracking
                ? set + ", pause_reason = NULL, failure_category = NULL, failure_event_sequence = NULL, " +
                        "failure_event_type = NULL, failure_event_tenant_id = NULL"
                : set;

            var columns = _extendedTracking
                ? "name, last_seq_id, last_updated, heartbeat"
                : "name, last_seq_id, last_updated";
            var values = _extendedTracking
                ? "source.name, @seq, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET()"
                : "source.name, @seq, SYSDATETIMEOFFSET()";

            // HOLDLOCK: this is the Floor == 0 path, where the row does not exist yet and two
            // agents starting together both take the INSERT branch without it. See #500.
            builder.Append($"MERGE {_progressionTableName} WITH (UPDLOCK, HOLDLOCK) AS target USING (SELECT ");
            builder.AppendParameter(_name, Storage.KeyColumns.DbType(_unicode));
            builder.Append($"""
                 AS name) AS source ON target.name = source.name
                WHEN MATCHED THEN UPDATE SET {matchedSet}
                WHEN NOT MATCHED THEN INSERT ({columns})
                    VALUES ({values});
                """);
        }
        else if (_expectedFloor is { } floor)
        {
            builder.Append($"UPDATE {_progressionTableName} SET {set} WHERE name = ");
            builder.AppendParameter(_name, Storage.KeyColumns.DbType(_unicode));
            builder.Append(" AND last_seq_id = ");
            builder.AppendParameter(floor);
            builder.Append($"; IF @@ROWCOUNT = 0 THROW {ProgressionOutOfOrderErrorNumber}, 'The shard''s progression is not at the floor this batch was built from.', 1;");
        }
        else
        {
            builder.Append($"UPDATE {_progressionTableName} SET {set} WHERE name = ");
            builder.AppendParameter(_name, Storage.KeyColumns.DbType(_unicode));
            builder.Append(";");
        }

        builder.AddParameters(new Dictionary<string, object?>
        {
            ["seq"] = _ceiling
        });
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) =>
        Task.CompletedTask;

    public bool TryTransform(Exception original, out Exception? transformed)
    {
        if (_expectedFloor is { } floor && original is SqlException { Number: ProgressionOutOfOrderErrorNumber })
        {
            transformed = new ProgressionProgressOutOfOrderException(_name, floor, _ceiling);
            return true;
        }

        transformed = null;
        return false;
    }
}
