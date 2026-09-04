using System.Data.Common;
using JasperFx.Events;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Protected;

/// <summary>
///     jasperfx#740 (#534): records the compaction watermark on <c>pc_streams</c> after a
///     <see cref="JasperFx.Events.Protected.StreamCompactingRequest{T}"/> executes. The value is the
///     version of the last event folded into the <see cref="Compacted{T}"/> snapshot — the cutoff
///     version for a partial compaction, the stream's version for a full one — which is exactly
///     what <see cref="StreamState.CompactedVersion"/> reads back, so
///     <c>(Version - CompactedVersion)</c> measures un-compacted growth. A never-compacted stream
///     keeps the column's NOT NULL DEFAULT 0.
/// </summary>
internal class SetCompactedVersionOperation : Polecat.Internal.IStorageOperation
{
    private readonly EventGraph _events;
    private readonly object _streamId;
    private readonly long _compactedVersion;
    private readonly string _tenantId;

    public SetCompactedVersionOperation(EventGraph events, object streamId, long compactedVersion, string tenantId)
    {
        _events = events;
        _streamId = streamId;
        _compactedVersion = compactedVersion;
        _tenantId = tenantId;
    }

    public Type DocumentType => typeof(IEvent);
    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        // Tenant-qualified like every other pc_streams write: under conjoined tenancy the same
        // stream id can exist for two tenants, and only (tenant_id, id) names one row.
        builder.Append($"UPDATE {_events.StreamsTableName} SET compacted_version = ");
        builder.AppendParameter(_compactedVersion);
        builder.Append(" WHERE id = ");
        builder.AppendParameter(_streamId);
        builder.Append(" AND tenant_id = ");
        builder.AppendParameter(_tenantId, System.Data.SqlDbType.VarChar);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
