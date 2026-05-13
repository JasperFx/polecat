using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
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
///     <para>
///     <b>Per-row hot-path discipline.</b> The read methods are entered
///     once per row in <see cref="QueryEventStore.FetchStreamAsync(System.Guid, long, System.DateTimeOffset?, long, System.Threading.CancellationToken)"/>;
///     two flavors of state that are stable across a single read batch are
///     hoisted into structs the caller builds once and passes in:
///     <see cref="MetadataSlots"/> (pre-computed column ordinals for the
///     optional metadata triple) and <see cref="EventTypeCache"/> (last-seen
///     event-type → mapping, so streams with repeated event types don't
///     re-dictionary-lookup per row). The per-row <c>if</c>-on-options
///     ladder and per-row <c>EventMappingFor</c> dictionary lookup both go
///     away in the common case.
///     </para>
///     <para>
///     <b>StreamIdentity specialization.</b> <c>StreamIdentity</c> is fixed
///     at store-construction time but the pre-#57-Q2 code branched on it
///     per row. <see cref="ReadEventAsGuid"/> / <see cref="ReadEventAsString"/>
///     are specializations the caller picks once at the top of its read
///     loop — see <see cref="QueryEventStore"/>. Both delegate to
///     <see cref="ReadEventCore"/> for the shared 95% of the work.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: hydrates IEvent instances via ISerializer.FromJson on the event data column and EventGraph.Wrap. Event types are preserved by EventGraph registration on the caller side per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson and Event<T>.MakeGenericType for envelope construction are annotated RDC. AOT consumers supply a source-generator-backed ISerializer impl and register concrete event types.")]
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
    ///     <see cref="IEvent"/>, assuming the active store uses
    ///     <see cref="StreamIdentity.AsGuid"/>. Caller selects between this
    ///     and <see cref="ReadEventAsString"/> once at the top of its read
    ///     loop, eliminating the per-row branch on <c>StreamIdentity</c>.
    ///     Returns <see langword="null"/> when <c>dotnet_type</c> doesn't
    ///     resolve to a known type so callers can skip with a single
    ///     <c>continue</c>.
    /// </summary>
    internal static IEvent? ReadEventAsGuid(
        DbDataReader reader,
        EventHydrationContext ctx,
        MetadataSlots slots,
        ref EventTypeCache cache)
    {
        var @event = ReadEventCore(reader, ctx, slots, ref cache);
        if (@event == null) return null;
        @event.StreamId = ctx.StreamId is Guid g ? g : Guid.Empty;
        return @event;
    }

    /// <summary>
    ///     Read the row the reader is positioned on into a fully-hydrated
    ///     <see cref="IEvent"/>, assuming the active store uses
    ///     <see cref="StreamIdentity.AsString"/>. See <see cref="ReadEventAsGuid"/>.
    /// </summary>
    internal static IEvent? ReadEventAsString(
        DbDataReader reader,
        EventHydrationContext ctx,
        MetadataSlots slots,
        ref EventTypeCache cache)
    {
        var @event = ReadEventCore(reader, ctx, slots, ref cache);
        if (@event == null) return null;
        @event.StreamKey = ctx.StreamId.ToString();
        return @event;
    }

    /// <summary>
    ///     Shared body of <see cref="ReadEventAsGuid"/> /
    ///     <see cref="ReadEventAsString"/>. Reads every field EXCEPT
    ///     <c>StreamId</c> / <c>StreamKey</c>; the specialized wrappers
    ///     pick the right id-assignment.
    /// </summary>
    private static IEvent? ReadEventCore(
        DbDataReader reader,
        EventHydrationContext ctx,
        MetadataSlots slots,
        ref EventTypeCache cache)
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

        // Per-batch single-slot type→mapping cache. For streams with
        // repeated event types (the common shape of an aggregate stream)
        // this collapses N dictionary lookups into 1 per distinct type.
        var mapping = cache.LookupOrAdd(ctx.EventGraph, resolvedType);

        var data = ctx.Serializer.FromJson(resolvedType, json);
        var @event = mapping.Wrap(data);

        @event.Id = eventId;
        @event.Sequence = seqId;
        @event.Version = eventVersion;
        @event.Timestamp = eventTimestamp;
        @event.TenantId = tenantId;
        @event.EventTypeName = typeName;
        @event.DotNetTypeName = dotNetTypeName!;
        @event.IsArchived = isArchived;

        // Per-batch pre-computed slots. Each branch tests a sentinel and is
        // taken/not-taken the same way for every row in this batch — branch
        // predictor pegged. The per-row metaIndex++ chain is gone.
        if (slots.CorrelationIdx >= 0)
        {
            @event.CorrelationId = reader.IsDBNull(slots.CorrelationIdx)
                ? null
                : reader.GetString(slots.CorrelationIdx);
        }
        if (slots.CausationIdx >= 0)
        {
            @event.CausationId = reader.IsDBNull(slots.CausationIdx)
                ? null
                : reader.GetString(slots.CausationIdx);
        }
        if (slots.HeadersIdx >= 0 && !reader.IsDBNull(slots.HeadersIdx))
        {
            var headersJson = reader.GetString(slots.HeadersIdx);
            @event.Headers = ctx.Serializer.FromJson<Dictionary<string, object>>(headersJson);
        }

        return @event;
    }

    /// <summary>
    ///     Read the row the reader is positioned on into a raw
    ///     <see cref="EventRecord"/> for the explorer surface. Unlike
    ///     <see cref="ReadEventAsGuid"/> / <see cref="ReadEventAsString"/>
    ///     this returns a self-contained record (no <see cref="ISerializer"/>
    ///     deserialization; <c>data</c> and <c>headers</c> are surfaced as
    ///     cloned <see cref="JsonElement"/>s).
    /// </summary>
    /// <remarks>
    ///     Same per-batch <see cref="MetadataSlots"/> hoisting as the
    ///     <see cref="IEvent"/> path. The explorer doesn't need the
    ///     <see cref="EventTypeCache"/> because it doesn't call
    ///     <c>EventMappingFor</c>.
    /// </remarks>
    internal static EventRecord ReadEventRecord(
        DbDataReader reader,
        EventHydrationContext ctx,
        MetadataSlots slots)
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

        JsonElement? metadata = null;
        if (slots.HeadersIdx >= 0 && !reader.IsDBNull(slots.HeadersIdx))
        {
            using var headerDoc = JsonDocument.Parse(reader.GetString(slots.HeadersIdx));
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

/// <summary>
///     Pre-computed column ordinals for the optional metadata triple
///     (<c>correlation_id</c>, <c>causation_id</c>, <c>headers</c>). Built
///     once at the top of a read loop from <see cref="EventStoreOptions"/>
///     and passed by value into every <see cref="PcEventsRowReader"/> call
///     in the batch. <c>-1</c> means the column is not projected; the
///     reader skips it without checking the underlying flag.
/// </summary>
internal readonly record struct MetadataSlots(int CorrelationIdx, int CausationIdx, int HeadersIdx)
{
    /// <summary>
    ///     Sentinel indicating the column is not in the projected SELECT.
    /// </summary>
    public const int Disabled = -1;

    public static MetadataSlots Compute(EventStoreOptions options)
    {
        var ordinal = 10;
        var correlation = options.EnableCorrelationId ? ordinal++ : Disabled;
        var causation = options.EnableCausationId ? ordinal++ : Disabled;
        var headers = options.EnableHeaders ? ordinal : Disabled;
        return new MetadataSlots(correlation, causation, headers);
    }
}

/// <summary>
///     Per-batch single-slot LRU for the <c>resolved-Type → IEventType
///     mapping</c> lookup performed inside <see cref="PcEventsRowReader.ReadEventAsGuid"/>
///     / <see cref="PcEventsRowReader.ReadEventAsString"/>. Streams of
///     events typically have a small set of distinct event types repeated
///     many times — caching the last seen type and its mapping turns N
///     dictionary lookups into 1 per distinct type in the common case.
/// </summary>
/// <remarks>
///     Mutable struct, passed by <c>ref</c>. Live for the lifetime of a
///     single batch read; never crosses thread boundaries.
/// </remarks>
internal struct EventTypeCache
{
    private Type? _lastType;
    private IEventType? _lastMapping;

    public IEventType LookupOrAdd(EventGraph eventGraph, Type resolvedType)
    {
        if (!ReferenceEquals(resolvedType, _lastType))
        {
            _lastMapping = eventGraph.EventMappingFor(resolvedType);
            _lastType = resolvedType;
        }
        return _lastMapping!;
    }
}
