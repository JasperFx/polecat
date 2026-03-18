using System.Data.Common;
using JasperFx.Events;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Protected;

internal class OverwriteEventOperation : Polecat.Internal.IStorageOperation
{
    private readonly EventGraph _events;
    private readonly IEvent _event;
    private readonly string _serializedData;
    private readonly string? _serializedHeaders;

    public OverwriteEventOperation(EventGraph events, IEvent @event, string serializedData, string? serializedHeaders)
    {
        _events = events;
        _event = @event;
        _serializedData = serializedData;
        _serializedHeaders = serializedHeaders;
    }

    public Type DocumentType => typeof(IEvent);
    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        if (_serializedHeaders != null)
        {
            builder.Append($"UPDATE {_events.EventsTableName} SET data = ");
            builder.AppendParameter(_serializedData);
            builder.Append(", headers = ");
            builder.AppendParameter(_serializedHeaders);
            builder.Append(" WHERE seq_id = ");
            builder.AppendParameter(_event.Sequence);
            builder.Append(";");
        }
        else
        {
            builder.Append($"UPDATE {_events.EventsTableName} SET data = ");
            builder.AppendParameter(_serializedData);
            builder.Append(" WHERE seq_id = ");
            builder.AppendParameter(_event.Sequence);
            builder.Append(";");
        }
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
