using System.Data.Common;
using Polecat.Events;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

/// <summary>
///     MERGE-based upsert for event progression tracking (used when Floor == 0).
/// </summary>
internal class ProgressMergeOperation : IStorageOperation
{
    private readonly string _progressionTableName;
    private readonly string _name;
    private readonly long _ceiling;
    private readonly bool _extendedTracking;

    public ProgressMergeOperation(string progressionTableName, string name, long ceiling, bool extendedTracking)
    {
        _progressionTableName = progressionTableName;
        _name = name;
        _ceiling = ceiling;
        _extendedTracking = extendedTracking;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        if (_extendedTracking)
        {
            builder.Append($"""
                MERGE {_progressionTableName} AS target
                USING (SELECT @name AS name) AS source ON target.name = source.name
                WHEN MATCHED THEN UPDATE SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET(), heartbeat = SYSDATETIMEOFFSET()
                WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated, heartbeat)
                    VALUES (@name, @seq, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET());
                """);
        }
        else
        {
            builder.Append($"""
                MERGE {_progressionTableName} AS target
                USING (SELECT @name AS name) AS source ON target.name = source.name
                WHEN MATCHED THEN UPDATE SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
                WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                    VALUES (@name, @seq, SYSDATETIMEOFFSET());
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
