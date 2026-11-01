using System.Data.Common;
using JasperFx.Events;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Protected;

internal class OverwriteEventOperation : Polecat.Internal.IStorageOperation, Weasel.Storage.NoDataReturnedCall
{
    private readonly EventGraph _events;
    private readonly IEvent _event;
    private readonly string _serializedData;
    private readonly byte[]? _serializedBdata;
    private readonly string? _serializedHeaders;
    private readonly string _tenantId;

    public OverwriteEventOperation(EventGraph events, IEvent @event, string serializedData,
        byte[]? serializedBdata, string? serializedHeaders, string tenantId)
    {
        _events = events;
        _event = @event;
        _serializedData = serializedData;
        _serializedBdata = serializedBdata;
        _serializedHeaders = serializedHeaders;
        _tenantId = tenantId;
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

        // #843: the payload is the event's type NOW, which for an upcast event is not the type the row
        // was stored under, so the row's identity has to move with it. Left under the source name, the
        // next read upcasts the new shape a second time and every renamed member comes back empty (or,
        // for a strict raw-JSON upcaster, the stream stops being readable). A binary target fails the
        // same way by a different route: non-null bdata switches upcasting off, so the stale
        // dotnet_type decides the read and the target's bytes meet the source type.
        //
        // Both columns, always: `type` decides whether a read upcasts, and dotnet_type decides the CLR
        // type everywhere else and is the daemon loader's allow list -- moving only `type` turns the
        // row into one no projection of the new type is ever handed. Writing the mask back in the
        // stored shape is not an alternative: an upcast is one-way, and the old CLR type may be gone.
        // The same type SerializeEventBdata resolved, so the identity and the data/bdata split agree.
        var mapping = _events.EventMappingFor(_event.EventType ?? _event.Data.GetType());
        builder.Append(", type = ");
        builder.AppendParameter(mapping.EventTypeName);
        builder.Append(", dotnet_type = ");
        builder.AppendParameter(mapping.DotNetTypeName);

        if (_serializedHeaders != null)
        {
            builder.Append(", headers = ");
            builder.AppendParameter(_serializedHeaders);
        }

        builder.Append(" WHERE seq_id = ");
        builder.AppendParameter(_event.Sequence);

        // marten#5234's Polecat twin: under UseTenantPartitionedEvents seq_id is per-tenant, so
        // without this predicate masking tenant A's stream rewrites (and discloses A's masked JSON
        // into) tenant B's same-numbered event.
        builder.AppendConjoinedTenantFilter(_events, _tenantId);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
