using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Weasel.Storage;

namespace Polecat.Events.Daemon;

/// <summary>
///     SQL Server implementation of IEventLoader. Loads event batches by seq_id range from pc_events.
///     Resilience (Polly) + loading metrics are layered on by the lifted
///     <see cref="ResilientEventLoader"/> decorator (jasperfx#329) that wraps this loader at
///     <c>BuildEventLoader</c> time — this class is the bare inner loader.
/// </summary>
/// <remarks>
///     <para>
///         <b>#989: the paging, skip accounting, ceiling calculation and cancellation translation are the
///         shared <see cref="EventLoaderBase" />'s</b> (weasel#566), the base Fisher's loader already
///         derives from. Polecat supplies the SQL through <see cref="SqlServerEventPagingDialect" /> and the
///         row hydration below. Adopting it also brings <c>EventPage.LastObservedSequence</c> (jasperfx#667)
///         on every page, not only on the old client-side fallback path.
///     </para>
///     <para>
///         #550: a subscription or projection that names its event types gets that allow list pushed into
///         the SQL, so non-matching rows are never read off the wire, hydrated or deserialized. The list is
///         Weasel's <see cref="EventTypeAllowList" />, which adds the framework markers (#557), the upcast
///         sources (#561), and reads everything for an interface or abstract type (weasel#845). See
///         <see cref="SqlServerEventPagingDialect" /> for how it renders, at any length (weasel#846).
///     </para>
///     <para>
///         <b>#1065: a shard that sets <c>IncludeArchivedEvents</c> reads archived events</b>, in a continuous
///         run and in a rebuild, as Marten's daemon does and as the per-identity rebuild (#1060) already did.
///         Before 6.0 this loader never passed the flag through, so every shard left archived events out.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: hydrates IEvent batches via EventGraph.Wrap (routed through ISerializer.FromJson). Event types are preserved by EventGraph registration on the caller side per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson and Event<T>.MakeGenericType are annotated RDC. AOT consumers register concrete event types ahead of time.")]
internal class PolecatEventLoader : EventLoaderBase
{
    private const int BdataOrdinal = 10;
    private const int DotNetTypeOrdinal = 8;

    private readonly EventGraph _events;
    private readonly StoreOptions _options;
    private readonly string _connectionString;

    // marten#6003 / #1053: whether the shard's projection slices by DCB tag, so its events need their tags, and the
    // shard's tenant under tenant-partitioned events
    private readonly bool _includeTags;
    private readonly string? _tenantFilter;

    // marten#6003 / #1053: the unreadable events of the load in flight when its request parks them. Set by LoadAsync
    // for the length of one load, which the base runs in a single async flow.
    private readonly AsyncLocal<List<UnreadableEvent>?> _unreadable = new();

    // #756: fixed for the loader's lifetime, computed from the same options the dialect composes the SELECT
    // from, so the two cannot disagree about where a metadata column is.
    private readonly Internal.MetadataSlots _metadataSlots;

    public PolecatEventLoader(EventGraph events, StoreOptions options, string connectionString,
        EventFilterable? filtering = null, string? tenantFilter = null)
        : base(new SqlServerEventPagingDialect(options, events), new EventLoaderOptions
        {
            // #163 Phase 2: a per-tenant rebuild/subscription shard scopes the load to one tenant so it
            // reads only that tenant's bounded sequence range, not the whole store. Null = store-global.
            TenantId = tenantFilter,
            EventTypes = EventTypeAllowList.For(events, filtering),

            // #1065: the shard's own IncludeArchivedEvents, as Marten's loader takes it. pc_events carries
            // is_archived itself, so with archived-stream partitioning (the 6.0 default) leaving the predicate
            // out reads both partitions, and there is no stream join to drop.
            IncludeArchivedEvents = filtering?.IncludeArchivedEvents ?? false
        })
    {
        _events = events;
        _options = options;
        _connectionString = connectionString;
        _metadataSlots = Internal.MetadataSlots.Compute(options.Events);
        _includeTags = filtering?.IncludeEventTags == true;
        _tenantFilter = tenantFilter;
    }

    /// <summary>
    ///     The base's load, plus two things marten#6003 asks of a store (#1053): an event the store can't deserialize
    ///     is reported on <see cref="EventPage.Unreadable" /> with its stream and tenant when the request parks such
    ///     events, and a projection that slices by DCB tag gets its events' tags.
    /// </summary>
    public override async Task<EventPage> LoadAsync(EventRequest request, CancellationToken token)
    {
        List<UnreadableEvent>? unreadable = null;
        if (request.ParksUnreadableEvents)
        {
            unreadable = new List<UnreadableEvent>();
            _unreadable.Value = unreadable;
        }

        EventPage page;
        try
        {
            page = await base.LoadAsync(request, token).ConfigureAwait(false);
        }
        finally
        {
            if (unreadable != null) _unreadable.Value = null;
        }

        if (unreadable is { Count: > 0 })
        {
            foreach (var @event in unreadable) page.AddUnreadable(@event);
        }

        if (_includeTags && page.Count > 0)
        {
            await EventTagHydrator.HydrateAsync(_connectionString, _events, page, _tenantFilter, token)
                .ConfigureAwait(false);
        }

        return page;
    }

    // marten#6003 / #1053: a request that parks unreadable events takes every one of them, whatever
    // SkipSerializationErrors says, and the base counts it as skipped so the page's ceiling still covers it
    protected override bool ShouldSkip(Exception ex, ErrorHandlingOptions options)
        => (_unreadable.Value != null && ex is EventDeserializationFailureException) || base.ShouldSkip(ex, options);

    protected override ValueTask RecordSkippedEventAsync(Exception ex, EventRequest request, CancellationToken token)
    {
        if (_unreadable.Value is { } unreadable && ex is EventDeserializationFailureException failure)
        {
            unreadable.Add(new UnreadableEvent(failure.Sequence, failure.StreamId, failure.StreamKey, failure.TenantId,
                failure.EventTypeName, failure));
        }

        return base.RecordSkippedEventAsync(ex, request, token);
    }

    protected override async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken token)
    {
        var conn = new SqlConnection(_connectionString);
        try
        {
            await conn.OpenAsync(token).ConfigureAwait(false);
            return conn;
        }
        catch
        {
            await conn.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    // The base binds each parameter's DbType but no size, and an unsized string is declared at its own
    // length, which gives the plan cache one entry per distinct value. Size them as AddVarChar does.
    protected override void ConfigureCommand(DbCommand command)
    {
        foreach (SqlParameter parameter in command.Parameters)
        {
            if (parameter.Size != 0) continue;

            parameter.Size = parameter.SqlDbType switch
            {
                System.Data.SqlDbType.VarChar => 8000,
                System.Data.SqlDbType.NVarChar => -1,
                _ => parameter.Size
            };
        }
    }

    /// <summary>
    ///     The daemon's own row hydration, for the readers that must read a row exactly as a shard would: the
    ///     per-identity rebuild's re-fold (#1060) and the stored-event rewrite (#1034). The row has to be in
    ///     <see cref="Internal.PcEventsRowReader.ComposeSelectColumns" />'s order. Null when the row's
    ///     <c>dotnet_type</c> resolves to nothing.
    /// </summary>
    internal ValueTask<IEvent?> ReadRowAsync(DbDataReader reader, CancellationToken token)
        => ReadEventAsync(reader, token);

    /// <summary>
    ///     The exception a shard raises for a row whose <c>dotnet_type</c> resolves to nothing.
    /// </summary>
    internal static UnknownEventTypeException UnknownEventTypeFor(DbDataReader reader, long sequence)
        => new(reader.IsDBNull(DotNetTypeOrdinal) ? null : reader.GetString(DotNetTypeOrdinal), sequence);

    protected override void ReportLastObservedSequence(EventPage page, long? lastObservedSequence)
        => page.LastObservedSequence = lastObservedSequence;

    // #368 / jasperfx#565: a typed exception carrying its own ShardFailureCategory, so a shard paused by an
    // unregistered event type reports UnknownEventType with the offending sequence rather than classifying
    // as Other with no detail.
    protected override Exception UnresolvedEventTypeException(DbDataReader reader, long sequence)
        => UnknownEventTypeFor(reader, sequence);

    // marten#6003 / #1053: the row's stream and tenant ride the failure, so a single-stream projection can park the
    // stream it belongs to
    private EventDeserializationFailureException unreadable(long sequence, string typeName, object rawStreamId,
        string tenantId, Exception inner)
        => new(sequence, typeName, inner)
        {
            StreamId = _events.StreamIdentity == StreamIdentity.AsGuid && rawStreamId is Guid id ? id : null,
            StreamKey = _events.StreamIdentity == StreamIdentity.AsGuid ? null : rawStreamId.ToString(),
            TenantId = tenantId
        };

    /// <summary>
    ///     One of the five hydration paths CLAUDE.md lists. Returns null for a row whose <c>dotnet_type</c>
    ///     resolves to nothing, which the base turns into a skip or an <see cref="UnknownEventTypeException" />
    ///     according to <c>SkipUnknownEvents</c>.
    /// </summary>
    protected override async ValueTask<IEvent?> ReadEventAsync(DbDataReader reader, CancellationToken token)
    {
        var seqId = reader.GetInt64(0);
        var eventId = reader.GetGuid(1);
        var rawStreamId = reader.GetValue(2);
        var eventVersion = reader.GetInt64(3);
        var json = reader.GetString(4);
        var typeName = reader.GetString(5);
        var eventTimestamp = reader.GetFieldValue<DateTimeOffset>(6);
        var tenantId = reader.GetString(7);
        var dotNetTypeName = reader.IsDBNull(DotNetTypeOrdinal) ? null : reader.GetString(DotNetTypeOrdinal);
        var isArchived = reader.GetBoolean(9);
        // #388: non-null bdata means this row's payload is binary, not JSON.
        var isBinary = !reader.IsDBNull(BdataOrdinal);

        object data;
        IEventType mapping;

        if (_events.TryFindUpcast(typeName, isBinary, out var transformation))
        {
            // Upcast FIRST, so the dotnet_type hint cannot shadow a registered transformation
            // (marten#4680). The async delegate, because the daemon's read is awaited.
            try
            {
                data = await _events
                    .UpcastAsync(transformation!, json, _options.Serializer, token)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // An upcast that throws is a payload-shape problem like any other failed deserialization, so
                // it carries the same ShardFailureCategory and obeys the same SkipSerializationErrors policy
                // (the base classifies it) rather than pausing the shard as something new.
                throw unreadable(seqId, typeName, rawStreamId, tenantId, ex);
            }

            mapping = _events.EventMappingFor(transformation!.EventType);
        }
        else
        {
            var resolvedType = _events.ResolveEventType(dotNetTypeName);
            if (resolvedType == null)
            {
                return null;
            }

            try
            {
                data = _events.DeserializeEventData(resolvedType, json, reader, BdataOrdinal, _options.Serializer);
            }
            catch (Exception ex)
            {
                // #368 / jasperfx#565: report the store's type alias (the `type` column) rather than the
                // assembly-qualified dotnet_type, matching what ShardFailure.Event.EventTypeName carries
                // everywhere else and what a client-side consumer can act on.
                throw unreadable(seqId, typeName, rawStreamId, tenantId, ex);
            }

            mapping = _events.EventMappingFor(resolvedType);
        }

        var @event = mapping.Wrap(data);

        @event.Id = eventId;
        @event.Sequence = seqId;
        @event.Version = eventVersion;
        @event.Timestamp = eventTimestamp;
        @event.TenantId = tenantId;
        @event.EventTypeName = typeName;
        @event.DotNetTypeName = dotNetTypeName!;
        @event.IsArchived = isArchived;

        // #756: the opt-in metadata columns. Before #756 this loader never selected them, so every event the
        // daemon handed to a subscription or an async projection had a null CorrelationId, CausationId,
        // Headers and UserName.
        Internal.PcEventsRowReader.ReadMetadata(reader, @event, _metadataSlots, _options.Serializer);

        if (_events.StreamIdentity == StreamIdentity.AsGuid && rawStreamId is Guid g)
        {
            @event.StreamId = g;
        }
        else
        {
            @event.StreamKey = rawStreamId.ToString();
        }

        return @event;
    }
}
