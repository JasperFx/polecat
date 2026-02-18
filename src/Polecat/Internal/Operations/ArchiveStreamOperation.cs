using Microsoft.Data.SqlClient;
using Polecat.Events;

namespace Polecat.Internal.Operations;

internal class ArchiveStreamOperation : IStorageOperation
{
    private readonly EventGraph _events;
    private readonly object _streamId;
    private readonly string _tenantId;

    public ArchiveStreamOperation(EventGraph events, object streamId, string tenantId)
    {
        _events = events;
        _streamId = streamId;
        _tenantId = tenantId;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role => OperationRole.Update;

    public void ConfigureCommand(SqlCommand command)
    {
        command.CommandText = $"""
            UPDATE {_events.StreamsTableName} SET is_archived = 1
            WHERE id = @id AND tenant_id = @tenant_id;
            UPDATE {_events.EventsTableName} SET is_archived = 1
            WHERE stream_id = @id AND tenant_id = @tenant_id;
            """;
        command.Parameters.AddWithValue("@id", _streamId);
        command.Parameters.AddWithValue("@tenant_id", _tenantId);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        await command.ExecuteNonQueryAsync(token);
    }
}
