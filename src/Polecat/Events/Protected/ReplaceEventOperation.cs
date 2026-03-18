using System.Data.Common;
using JasperFx.Events;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Protected;

internal class ReplaceEventOperation : Polecat.Internal.IStorageOperation
{
    private readonly EventGraph _events;
    private readonly long _sequence;
    private readonly string _serializedData;
    private readonly string _eventTypeName;
    private readonly string _dotNetTypeName;
    private readonly Guid _newId;

    public ReplaceEventOperation(EventGraph events, long sequence, string serializedData,
        string eventTypeName, string dotNetTypeName)
    {
        _events = events;
        _sequence = sequence;
        _serializedData = serializedData;
        _eventTypeName = eventTypeName;
        _dotNetTypeName = dotNetTypeName;
        _newId = Guid.NewGuid();
    }

    public Guid Id => _newId;
    public Type DocumentType => typeof(IEvent);
    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append($"UPDATE {_events.EventsTableName} SET data = ");
        builder.AppendParameter(_serializedData);
        builder.Append(", timestamp = SYSDATETIMEOFFSET(), type = ");
        builder.AppendParameter(_eventTypeName);
        builder.Append(", dotnet_type = ");
        builder.AppendParameter(_dotNetTypeName);
        builder.Append(", id = ");
        builder.AppendParameter(_newId);

        if (_events.EventOptions.EnableHeaders)
            builder.Append(", headers = NULL");
        if (_events.EventOptions.EnableCorrelationId)
            builder.Append(", correlation_id = NULL");
        if (_events.EventOptions.EnableCausationId)
            builder.Append(", causation_id = NULL");

        builder.Append(" WHERE seq_id = ");
        builder.AppendParameter(_sequence);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
