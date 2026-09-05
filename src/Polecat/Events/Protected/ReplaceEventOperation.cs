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
    private readonly byte[]? _serializedBdata;
    private readonly string _eventTypeName;
    private readonly string _dotNetTypeName;
    private readonly Guid _newId;
    private readonly string _tenantId;

    public ReplaceEventOperation(EventGraph events, long sequence, string serializedData,
        byte[]? serializedBdata, string eventTypeName, string dotNetTypeName, string tenantId)
    {
        _events = events;
        _sequence = sequence;
        _serializedData = serializedData;
        _serializedBdata = serializedBdata;
        _eventTypeName = eventTypeName;
        _dotNetTypeName = dotNetTypeName;
        _newId = Guid.NewGuid();
        _tenantId = tenantId;
    }

    public Guid Id => _newId;
    public Type DocumentType => typeof(IEvent);
    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append($"UPDATE {_events.EventsTableName} SET data = ");
        builder.AppendParameter(_serializedData);

        // #388: the replacement body's own event type decides the row's format, so bdata is set (or
        // cleared) in the same statement — a replacement that switched a row from binary to JSON
        // without clearing bdata would keep reading the OLD payload.
        builder.Append(", bdata = ");
        if (_serializedBdata is null)
        {
            builder.Append("NULL");
        }
        else
        {
            builder.AppendParameter(_serializedBdata);
        }

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

        // marten#5234's Polecat twin: without this the Compacted<T> snapshot — the calling
        // tenant's whole aggregate state — is written into another tenant's same-numbered event
        // under UseTenantPartitionedEvents.
        builder.AppendConjoinedTenantFilter(_events, _tenantId);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
