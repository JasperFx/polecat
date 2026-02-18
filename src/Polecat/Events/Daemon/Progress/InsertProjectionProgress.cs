using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal;

namespace Polecat.Events.Daemon.Progress;

/// <summary>
///     Inserts an initial projection progression row into pc_event_progression.
/// </summary>
internal class InsertProjectionProgress : IStorageOperation
{
    private readonly EventGraph _events;
    private readonly EventRange _range;

    public InsertProjectionProgress(EventGraph events, EventRange range)
    {
        _events = events;
        _range = range;
    }

    public Type DocumentType => typeof(ShardState);
    public OperationRole Role => OperationRole.Insert;

    public void ConfigureCommand(SqlCommand command)
    {
        command.CommandText = $"""
            INSERT INTO {_events.ProgressionTableName} (name, last_seq_id, last_updated)
            VALUES (@name, @seq, SYSDATETIMEOFFSET());
            """;

        command.Parameters.AddWithValue("@name", _range.ShardName.Identity);
        command.Parameters.AddWithValue("@seq", _range.SequenceCeiling);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        await command.ExecuteNonQueryAsync(token);
    }
}
