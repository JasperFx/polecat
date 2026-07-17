using System.Data.Common;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

/// <summary>
///     Writes a shard's progression high-water mark. Two axes vary independently (polecat#318):
///     <paramref name="upsert" /> picks MERGE (the row may not exist yet — Floor == 0) versus a plain
///     UPDATE (Floor > 0, so the row is already there), and extended tracking adds the heartbeat
///     column. The SET clause is shared by both shapes.
/// </summary>
internal class RecordProgressionOperation : IStorageOperation
{
    private readonly string _progressionTableName;
    private readonly string _name;
    private readonly long _ceiling;
    private readonly bool _extendedTracking;
    private readonly bool _upsert;

    public RecordProgressionOperation(
        string progressionTableName, string name, long ceiling, bool extendedTracking, bool upsert)
    {
        _progressionTableName = progressionTableName;
        _name = name;
        _ceiling = ceiling;
        _extendedTracking = extendedTracking;
        _upsert = upsert;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var set = _extendedTracking
            ? "last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET(), heartbeat = SYSDATETIMEOFFSET()"
            : "last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()";

        if (_upsert)
        {
            var columns = _extendedTracking
                ? "name, last_seq_id, last_updated, heartbeat"
                : "name, last_seq_id, last_updated";
            var values = _extendedTracking
                ? "@name, @seq, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET()"
                : "@name, @seq, SYSDATETIMEOFFSET()";

            builder.Append($"""
                MERGE {_progressionTableName} AS target
                USING (SELECT @name AS name) AS source ON target.name = source.name
                WHEN MATCHED THEN UPDATE SET {set}
                WHEN NOT MATCHED THEN INSERT ({columns})
                    VALUES ({values});
                """);
        }
        else
        {
            builder.Append($"""
                UPDATE {_progressionTableName}
                SET {set}
                WHERE name = @name;
                """);
        }

        builder.AddParameters(new Dictionary<string, object?>
        {
            ["name"] = _name,
            ["seq"] = _ceiling
        });
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) =>
        Task.CompletedTask;
}
