using System.Data.Common;
using JasperFx.Events.Projections;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Daemon.Progress;

/// <summary>
///     Updates an existing projection progression row with optimistic concurrency.
///     Throws ProgressionProgressOutOfOrderException if the current floor doesn't match.
/// </summary>
internal class UpdateProjectionProgress : Polecat.Internal.IStorageOperation
{
    private readonly EventGraph _events;
    private readonly EventRange _range;

    public UpdateProjectionProgress(EventGraph events, EventRange range)
    {
        _events = events;
        _range = range;
    }

    public Type DocumentType => typeof(ShardState);
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append($"""
            UPDATE {_events.ProgressionTableName}
            SET last_seq_id = @ceiling, last_updated = SYSDATETIMEOFFSET()
            OUTPUT inserted.name
            WHERE name = @name AND last_seq_id = @floor;
            """);

        builder.AddParameters(new Dictionary<string, object?>
        {
            ["name"] = _range.ShardName.Identity,
            ["ceiling"] = _range.SequenceCeiling,
            ["floor"] = _range.SequenceFloor
        });
    }

    public async Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        if (!await reader.ReadAsync(token))
        {
            throw new ProgressionProgressOutOfOrderException(
                _range.ShardName.Identity,
                _range.SequenceFloor,
                _range.SequenceCeiling);
        }
    }
}
