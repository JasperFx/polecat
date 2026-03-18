using System.Data.Common;
using Polecat.Events;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

/// <summary>
///     UPDATE-based progression tracking (used when Floor > 0, row already exists).
/// </summary>
internal class ProgressUpdateOperation : IStorageOperation
{
    private readonly string _progressionTableName;
    private readonly string _name;
    private readonly long _ceiling;
    private readonly bool _extendedTracking;

    public ProgressUpdateOperation(string progressionTableName, string name, long ceiling, bool extendedTracking)
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
                UPDATE {_progressionTableName}
                SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET(), heartbeat = SYSDATETIMEOFFSET()
                WHERE name = @name;
                """);
        }
        else
        {
            builder.Append($"""
                UPDATE {_progressionTableName}
                SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
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
