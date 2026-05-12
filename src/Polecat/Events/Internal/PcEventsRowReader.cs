using System.Data.Common;
using System.Text;
using System.Text.Json;
using JasperFx.Descriptors;
using JasperFx.Events;
using Polecat.Metadata;

namespace Polecat.Events.Internal;

/// <summary>
///     Canonical SQL fragment + row reader for the <c>pc_events</c> table.
///     Shared between <see cref="QueryEventStore.FetchStreamAsync(System.Guid, long, System.DateTimeOffset?, long, System.Threading.CancellationToken)"/>
///     (full <see cref="IEvent"/> hydration) and the
///     <see cref="IEventStore"/> explorer's <c>ReadStreamAsync</c> (raw
///     <see cref="EventRecord"/> for diagnostics). Closes the pc_events
///     half of <see href="https://github.com/JasperFx/polecat/issues/57">polecat#57</see>
///     (mirrors the Marten 9 <c>ISelector&lt;IEvent&gt;</c> consolidation).
/// </summary>
/// <remarks>
///     <para>
///     <b>Canonical column order is locked here.</b> The pre-consolidation
///     <see cref="QueryEventStore"/> order (the workhorse path) is preserved
///     as the canonical order; the explorer's <c>ReadStreamAsync</c> renumbered
///     to match. Adding or renaming a <c>pc_events</c> column means updating
///     this file and exactly this file.
///     </para>
///     <para>
///     <b>Optional metadata columns</b> (<c>correlation_id</c>, <c>causation_id</c>,
///     <c>headers</c>) are projected when their <see cref="EventStoreOptions"/>
///     flag is on. Readers that don't surface a particular metadata field
///     skip it on read — the column is still on the wire but the cost is
///     negligible against the readability of one canonical projection. See
///     polecat#57 Q2 design note.
///     </para>
/// </remarks>
internal static class PcEventsRowReader
{
    /// <summary>
    ///     Mandatory columns, always projected. Ordinals 0–9.
    /// </summary>
    internal const string CoreSelectColumns =
        "seq_id, id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type, is_archived";

    /// <summary>
    ///     Compose <see cref="CoreSelectColumns"/> plus any optional metadata
    ///     columns the active <see cref="EventStoreOptions"/> enables. Both readers
    ///     consume this so the SELECT shape stays in lockstep regardless of
    ///     which output type the caller is producing.
    /// </summary>
    internal static string ComposeSelectColumns(EventStoreOptions options)
    {
        var sb = new StringBuilder(CoreSelectColumns);
        if (options.EnableCorrelationId) sb.Append(", correlation_id");
        if (options.EnableCausationId) sb.Append(", causation_id");
        if (options.EnableHeaders) sb.Append(", headers");
        return sb.ToString();
    }

    /// <summary>
    ///     Read the row the reader is positioned on into a fully-hydrated
    ///     <see cref="IEvent"/>. Returns <see langword="null"/> when the
    ///     row's <c>dotnet_type</c> doesn't resolve to a known event type
    ///     so callers can filter unresolvable rows with a single
    ///     <c>continue</c>.
    /// </summary>
    internal static IEvent? ReadEvent(DbDataReader reader, EventHydrationContext ctx)
    {
        var seqId = reader.GetInt64(0);
        var eventId = reader.GetGuid(1);
        // stream_id at ordinal 2 — known by the caller; not read here
        var eventVersion = reader.GetInt64(3);
        var json = reader.GetString(4);
        var typeName = reader.GetString(5);
        var eventTimestamp = reader.GetFieldValue<DateTimeOffset>(6);
        var tenantId = reader.IsDBNull(7) ? ctx.DefaultTenantId : reader.GetString(7);
        var dotNetTypeName = reader.IsDBNull(8) ? null : reader.GetString(8);
        var isArchived = reader.GetBoolean(9);

        var resolvedType = ctx.EventGraph.ResolveEventType(dotNetTypeName);
        if (resolvedType == null) return null;

        var data = ctx.Serializer.FromJson(resolvedType, json);
        var mapping = ctx.EventGraph.EventMappingFor(resolvedType);
        var @event = mapping.Wrap(data);

        @event.Id = eventId;
        @event.Sequence = seqId;
        @event.Version = eventVersion;
        @event.Timestamp = eventTimestamp;
        @event.TenantId = tenantId;
        @event.EventTypeName = typeName;
        @event.DotNetTypeName = dotNetTypeName!;
        @event.IsArchived = isArchived;

        var metaIndex = 10;
        if (ctx.Options.EnableCorrelationId)
        {
            @event.CorrelationId = reader.IsDBNull(metaIndex) ? null : reader.GetString(metaIndex);
            metaIndex++;
        }
        if (ctx.Options.EnableCausationId)
        {
            @event.CausationId = reader.IsDBNull(metaIndex) ? null : reader.GetString(metaIndex);
            metaIndex++;
        }
        if (ctx.Options.EnableHeaders && !reader.IsDBNull(metaIndex))
        {
            var headersJson = reader.GetString(metaIndex);
            @event.Headers = ctx.Serializer.FromJson<Dictionary<string, object>>(headersJson);
        }

        if (ctx.StreamIdentity == StreamIdentity.AsGuid)
        {
            @event.StreamId = ctx.StreamId is Guid g ? g : Guid.Empty;
        }
        else
        {
            @event.StreamKey = ctx.StreamId.ToString();
        }

        return @event;
    }

    /// <summary>
    ///     Read the row the reader is positioned on into a raw
    ///     <see cref="EventRecord"/> for the explorer surface. Unlike
    ///     <see cref="ReadEvent"/> this returns a self-contained record
    ///     (no <see cref="ISerializer"/> deserialization; <c>data</c> and
    ///     <c>headers</c> are surfaced as cloned <see cref="JsonElement"/>s).
    /// </summary>
    internal static EventRecord ReadEventRecord(DbDataReader reader, EventHydrationContext ctx)
    {
        var seqId = reader.GetInt64(0);
        var eventId = reader.GetGuid(1);
        var streamIdRaw = reader.GetValue(2);
        var streamIdString = StreamIdToString(streamIdRaw);
        var eventVersion = reader.GetInt64(3);
        var rawData = reader.GetString(4);
        var typeName = reader.GetString(5);
        var eventTimestamp = reader.GetFieldValue<DateTimeOffset>(6);
        var tenantId = reader.IsDBNull(7) ? ctx.DefaultTenantId : reader.GetString(7);
        // dotnet_type (8) + is_archived (9) intentionally not surfaced — the
        // explorer's EventRecord shape doesn't include them. They're in the
        // SELECT because the canonical projection is shared; the diagnostic
        // path just ignores them.

        // Advance metaIndex past correlation_id / causation_id when they're
        // projected, since the explorer doesn't surface them either.
        var metaIndex = 10;
        if (ctx.Options.EnableCorrelationId) metaIndex++;
        if (ctx.Options.EnableCausationId) metaIndex++;

        JsonElement? metadata = null;
        if (ctx.Options.EnableHeaders && !reader.IsDBNull(metaIndex))
        {
            using var headerDoc = JsonDocument.Parse(reader.GetString(metaIndex));
            metadata = headerDoc.RootElement.Clone();
        }

        using var doc = JsonDocument.Parse(rawData);
        var data = doc.RootElement.Clone();

        return new EventRecord(
            eventId,
            seqId,
            eventVersion,
            streamIdString,
            typeName,
            data,
            metadata,
            eventTimestamp,
            tenantId,
            Tags: null!);
    }

    private static string StreamIdToString(object value) => value switch
    {
        Guid g => g.ToString(),
        _ => value.ToString() ?? string.Empty
    };
}
