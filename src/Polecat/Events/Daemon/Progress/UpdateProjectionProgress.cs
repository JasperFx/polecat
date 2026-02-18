using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Internal;

namespace Polecat.Events.Daemon.Progress;

/// <summary>
///     Updates an existing projection progression row with optimistic concurrency.
///     Throws ProgressionProgressOutOfOrderException if the current floor doesn't match.
/// </summary>
internal class UpdateProjectionProgress : IStorageOperation
{
    private readonly EventGraph _events;
    private readonly EventRange _range;

    public UpdateProjectionProgress(EventGraph events, EventRange range)
    {
        _events = events;
        _range = range;
    }

    public Type DocumentType => typeof(ShardState);
    public OperationRole Role => OperationRole.Update;

    public void ConfigureCommand(SqlCommand command)
    {
        command.CommandText = $"""
            UPDATE {_events.ProgressionTableName}
            SET last_seq_id = @ceiling, last_updated = SYSDATETIMEOFFSET()
            WHERE name = @name AND last_seq_id = @floor;
            """;

        command.Parameters.AddWithValue("@name", _range.ShardName.Identity);
        command.Parameters.AddWithValue("@ceiling", _range.SequenceCeiling);
        command.Parameters.AddWithValue("@floor", _range.SequenceFloor);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        var rows = await command.ExecuteNonQueryAsync(token);
        if (rows == 0)
        {
            throw new ProgressionProgressOutOfOrderException(
                _range.ShardName.Identity,
                _range.SequenceFloor,
                _range.SequenceCeiling);
        }
    }
}
