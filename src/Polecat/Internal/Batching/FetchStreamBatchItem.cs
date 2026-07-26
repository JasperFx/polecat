using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using JasperFx.Events;
using Polecat.Events;
using Polecat.Events.Internal;
using Polecat.Serialization;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

/// <summary>
///     #370: the batched half of <c>QueryEventStore.FetchStreamAsync</c>, carrying the same optional
///     <c>version</c> / <c>timestamp</c> / <c>fromVersion</c> filters. Composes its projection with
///     <see cref="PcEventsRowReader" /> and hydrates through the same readers as the standalone fetch,
///     so the two can never drift apart across a schema migration.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: hydrates IEvent batches via PcEventsRowReader (routed through ISerializer.FromJson). Event types are preserved by EventGraph registration on the caller side per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson and Event<T>.MakeGenericType are annotated RDC. AOT consumers register concrete event types ahead of time.")]
internal class FetchStreamBatchItem : IBatchQueryItem
{
    private readonly TaskCompletionSource<IReadOnlyList<IEvent>> _tcs = new();
    private readonly EventGraph _events;
    private readonly ISerializer _serializer;
    private readonly object _streamId;
    private readonly string _tenantId;
    private readonly long _version;
    private readonly DateTimeOffset? _timestamp;
    private readonly long _fromVersion;

    public FetchStreamBatchItem(EventGraph events, ISerializer serializer, object streamId, string tenantId,
        long version, DateTimeOffset? timestamp, long fromVersion)
    {
        _events = events;
        _serializer = serializer;
        _streamId = streamId;
        _tenantId = tenantId;
        _version = version;
        _timestamp = timestamp;
        _fromVersion = fromVersion;
    }

    public Task<IReadOnlyList<IEvent>> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder)
    {
        builder.Append(
            $"SELECT {PcEventsRowReader.ComposeSelectColumns(_events.EventOptions)} FROM {_events.EventsTableName} WHERE stream_id = ");
        builder.AppendParameter(_streamId, _streamId is string ? SqlDbType.VarChar : null);
        builder.Append(" AND tenant_id = ");
        builder.AppendParameter(_tenantId, SqlDbType.VarChar);
        builder.Append(" AND is_archived = 0");

        if (_version > 0)
        {
            builder.Append(" AND version <= ");
            builder.AppendParameter(_version);
        }

        if (_timestamp.HasValue)
        {
            builder.Append(" AND timestamp <= ");
            builder.AppendParameter(_timestamp.Value);
        }

        if (_fromVersion > 0)
        {
            builder.Append(" AND version >= ");
            builder.AppendParameter(_fromVersion);
        }

        builder.Append(" ORDER BY version;\n");
    }

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        var ctx = new EventHydrationContext(_events, _serializer, _streamId, defaultTenantId: _tenantId);

        // Same per-batch hoists as the standalone fetch: metadata ordinals computed once, a single-slot
        // type→mapping cache, and the StreamIdentity specialization picked once rather than per row.
        var slots = MetadataSlots.Compute(_events.EventOptions);
        var cache = new EventTypeCache();

        var results = new List<IEvent>();

        if (_events.StreamIdentity == StreamIdentity.AsGuid)
        {
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                var @event = PcEventsRowReader.ReadEventAsGuid(reader, ctx, slots, ref cache);
                if (@event != null) results.Add(@event);
            }
        }
        else
        {
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                var @event = PcEventsRowReader.ReadEventAsString(reader, ctx, slots, ref cache);
                if (@event != null) results.Add(@event);
            }
        }

        _tcs.SetResult(results);
    }
}
