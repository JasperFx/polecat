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
    private readonly byte[]? _serializedBdata;
    private readonly string? _serializedHeaders;

    public OverwriteEventOperation(EventGraph events, IEvent @event, string serializedData,
        byte[]? serializedBdata, string? serializedHeaders)
    {
        _events = events;
        _event = @event;
        _serializedData = serializedData;
        _serializedBdata = serializedBdata;
        _serializedHeaders = serializedHeaders;
    }

    public Type DocumentType => typeof(IEvent);
    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append($"UPDATE {_events.EventsTableName} SET data = ");
        builder.AppendParameter(_serializedData);

        // #388: a binary event's payload lives in bdata, so masking has to rewrite THAT — rewriting
        // only `data` would leave the original, unmasked payload readable in bdata, which for a GDPR
        // masking operation is the whole point missed. The row keeps its own format either way:
        // binary events stay binary (data holds the '{}' placeholder), JSON events stay JSON.
        builder.Append(", bdata = ");
        if (_serializedBdata is null)
        {
            builder.Append("NULL");
        }
        else
        {
            builder.AppendParameter(_serializedBdata);
        }

        if (_serializedHeaders != null)
        {
            builder.Append(", headers = ");
            builder.AppendParameter(_serializedHeaders);
        }

        builder.Append(" WHERE seq_id = ");
        builder.AppendParameter(_event.Sequence);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
