using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using JasperFx.Events;
using JasperFx.Events.Tags;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Internal;
using Polecat.Events.Linq;
using Polecat.Internal;
using Polecat.Linq;
using Polecat.Storage;

namespace Polecat.Events;

/// <summary>
///     Read-only event store implementation. Fetches events and stream state from the database.
///     All SQL execution routes through session's Polly-wrapped centralized methods.
/// </summary>
// #256: also implements JasperFx.Events.IReadOnlyEventStore (FetchStream/FetchStreamState/QueryEvents)
// so DocumentStore.OpenReadOnlyEventStore() can return this as IReadOnlyEventStore. All of its members
// are already present on the read store.
internal class QueryEventStore : IQueryEventStore, IReadOnlyEventStore
{
    private readonly QuerySession _session;
    private readonly StoreOptions _options;
    protected readonly EventGraph _events;

    // Cache types that have no Id property to avoid repeated reflection
    private static readonly ConcurrentDictionary<Type, bool> _hasIdCache = new();

    public QueryEventStore(QuerySession session, EventGraph events, StoreOptions options)
    {
        _session = session;
        _events = events;
        _options = options;
    }

    public IPolecatQueryable<T> QueryRawEventDataOnly<T>() where T : class
    {
        _events.AddEventType(typeof(T));
        var eventTypeName = _events.EventMappingFor(typeof(T)).EventTypeName;
        var provider = new EventLinqQueryProvider(_session, _events, eventTypeName, typeof(T), _options);
        return new PolecatLinqQueryable<T>(provider);
    }

    public IPolecatQueryable<IEvent> QueryAllRawEvents()
    {
        var provider = new EventLinqQueryProvider(_session, _events);
        return new PolecatLinqQueryable<IEvent>(provider);
    }

    /// <summary>
    ///     jasperfx#740 (#534): the streams table as a real <see cref="IQueryable{T}"/> of
    ///     <see cref="StreamState"/> — the read surface behind the Stream Compaction Policies
    ///     (<c>AggregateType == typeof(X) &amp;&amp; Version - CompactedVersion &gt; N &amp;&amp;
    ///     !IsArchived</c>). Every public get member of <see cref="StreamState"/> translates in
    ///     <c>Where()</c>/<c>OrderBy()</c>; an untranslatable member throws naming it. Executed
    ///     through the shared JasperFx.Events.Documents terminators (the provider implements
    ///     <see cref="JasperFx.Events.Documents.IDocumentQueryExecutor"/>) or Polecat's own.
    /// </summary>
    /// <param name="tenantId">
    ///     Optional tenant scope. Refused with <see cref="NotSupportedException"/> on a store
    ///     without conjoined tenancy — a tenant filter this store cannot honor must never
    ///     silently return the unscoped global set (the jasperfx#737 rule).
    /// </param>
    public IQueryable<StreamState> QueryStreamStates(string? tenantId = null)
    {
        if (tenantId != null && _events.TenancyStyle != TenancyStyle.Conjoined)
        {
            throw new NotSupportedException(
                $"A tenantId was supplied to {nameof(QueryStreamStates)}, but this event store has no tenant " +
                "dimension (Events.TenancyStyle is not Conjoined). A tenant filter must be honored or refused, " +
                "never silently ignored — unscoped results would read as tenant-scoped. See jasperfx#740.");
        }

        var provider = new StreamStateLinqQueryProvider(_session, _events, tenantId);
        return new PolecatLinqQueryable<StreamState>(provider);
    }

    /// <summary>
    ///     #256 / #532 (jasperfx#737): query events across all streams with metadata filters,
    ///     inclusive timestamp/sequence windows, multi-type union, DCB tag conditions, and
    ///     pagination — the full JasperFx <see cref="EventQuery" /> surface. All supplied filters
    ///     AND-combine; results are ordered by the store-global sequence ascending and
    ///     <see cref="PagedEvents.TotalCount" /> is the match count across every page.
    ///     The correlation/causation/user-name filters are honored only when the event store
    ///     actually captures that metadata column (the <c>Enable*</c> flag), since a disabled
    ///     column isn't populated — a query supplying one of those against a store that does not
    ///     capture it is REFUSED by <see cref="EventQuery.AssertFiltersAreSupported" /> (never
    ///     silently ignored; unfiltered results would read as filtered).
    /// </summary>
    public async Task<PagedEvents> QueryEventsAsync(EventQuery query, CancellationToken token = default)
    {
        query.AssertFiltersAreSupported(SupportedEventQueryFilters());

        IQueryable<IEvent> queryable = QueryAllRawEvents();

        // #353 / jasperfx#555 — honour the tenant scope the Event Explorer sets. On a conjoined
        // multi-tenant store the same event can exist under two tenants, so the Explorer sets
        // EventQuery.TenantId to isolate one; TenantIsOneOf overrides the implicit session-tenant filter
        // the event LINQ provider adds for conjoined stores. A null TenantId is left untouched, preserving
        // the pre-existing per-session-tenant paging contract.
        if (query.TenantId != null)
        {
            queryable = queryable.TenantIsOneOf(query.TenantId);
        }

        // jasperfx#737: EventTypeName and EventTypeNames union through CombinedEventTypeNames(),
        // so both spellings share one code path and the single/plural semantics stay upstream.
        var eventTypeNames = query.CombinedEventTypeNames();
        if (eventTypeNames.Count == 1)
        {
            var single = eventTypeNames[0];
            queryable = queryable.Where(e => e.EventTypeName == single);
        }
        else if (eventTypeNames.Count > 1)
        {
            var names = eventTypeNames.ToArray();
            queryable = queryable.Where(e => e.EventTypeName.IsOneOf(names));
        }

        if (query.StreamId != null)
        {
            if (_events.StreamIdentity == StreamIdentity.AsGuid && Guid.TryParse(query.StreamId, out var streamGuid))
            {
                queryable = queryable.Where(e => e.StreamId == streamGuid);
            }
            else
            {
                queryable = queryable.Where(e => e.StreamKey == query.StreamId);
            }
        }

        // The Enable* capture guards live in SupportedEventQueryFilters(): a supplied metadata
        // filter against a store that doesn't capture the column was already refused above, so
        // reaching here means the column exists and the filter is applied unconditionally.
        if (query.CorrelationId != null)
        {
            queryable = queryable.Where(e => e.CorrelationId == query.CorrelationId);
        }

        if (query.CausationId != null)
        {
            queryable = queryable.Where(e => e.CausationId == query.CausationId);
        }

        if (query.UserName != null)
        {
            queryable = queryable.Where(e => e.UserName == query.UserName);
        }

        // jasperfx#737: inclusive at both ends; a half-open window (one bound null) is valid, and
        // an inverted window is a well-formed range containing nothing — the SQL comparisons
        // produce exactly that (empty page, TotalCount 0), never an error.
        if (query.TimestampFrom.HasValue)
        {
            var timestampFrom = query.TimestampFrom.Value;
            queryable = queryable.Where(e => e.Timestamp >= timestampFrom);
        }

        if (query.TimestampTo.HasValue)
        {
            var timestampTo = query.TimestampTo.Value;
            queryable = queryable.Where(e => e.Timestamp <= timestampTo);
        }

        if (query.SequenceFloor.HasValue)
        {
            var sequenceFloor = query.SequenceFloor.Value;
            queryable = queryable.Where(e => e.Sequence >= sequenceFloor);
        }

        if (query.SequenceCeiling.HasValue)
        {
            var sequenceCeiling = query.SequenceCeiling.Value;
            queryable = queryable.Where(e => e.Sequence <= sequenceCeiling);
        }

        if (query.TagConditions != null)
        {
            queryable = ApplyTagConditions(queryable, query.TagConditions);
        }

        var pageNumber = query.PageNumber <= 0 ? 1 : query.PageNumber;
        var pageSize = query.PageSize <= 0 ? 25 : query.PageSize;
        var offset = (pageNumber - 1) * pageSize;

        var total = await queryable.CountAsync(token);
        var events = await queryable
            .OrderBy(e => e.Sequence)
            .Skip(offset)
            .Take(pageSize)
            .ToListAsync(token);

        return new PagedEvents
        {
            Events = events,
            TotalCount = total,
            PageNumber = pageNumber,
            PageSize = pageSize
        };
    }

    /// <summary>
    ///     The jasperfx#737 declaration: every <see cref="EventQuery" /> filter Polecat honors.
    ///     Structural filters (types, stream, tenant, both windows, tag conditions) are always
    ///     supported; the metadata filters are supported exactly when the store captures the
    ///     column, because a disabled column is never populated and filtering on it would return
    ///     truthful-looking garbage (everything or nothing depending on NULL semantics).
    /// </summary>
    private EventQueryFilters SupportedEventQueryFilters()
    {
        var supported = EventQueryFilters.EventTypeName
                        | EventQueryFilters.EventTypeNames
                        | EventQueryFilters.StreamId
                        | EventQueryFilters.TenantId
                        | EventQueryFilters.TimestampWindow
                        | EventQueryFilters.SequenceWindow
                        | EventQueryFilters.TagConditions;

        var options = _events.EventOptions;
        if (options.EnableCorrelationId) supported |= EventQueryFilters.CorrelationId;
        if (options.EnableCausationId) supported |= EventQueryFilters.CausationId;
        if (options.EnableUserName) supported |= EventQueryFilters.UserName;

        return supported;
    }

    private static readonly MethodInfo _hasTagMethod =
        typeof(LinqExtensions).GetMethod(nameof(LinqExtensions.HasTag))!;

    /// <summary>
    ///     jasperfx#737: fold the wire-form <see cref="EventTagQuerySpec" /> into the event LINQ
    ///     query as an OR of <see cref="LinqExtensions.HasTag{TTag}" /> marker calls — each
    ///     compiling to the same correlated <c>seq_id IN (SELECT seq_id FROM pc_event_tag_*)</c>
    ///     subquery Polecat's DCB tag path emits (see <see cref="HasTagParser" />), so an event
    ///     matching several conditions still reads back once and the whole selection AND-combines
    ///     with every other filter on the query. A type-scoped condition
    ///     (<c>EventTagQueryConditionSpec.EventType</c>) ANDs an <c>e.type</c> match into its own
    ///     OR branch, distinct from the query-level event type filter.
    /// </summary>
    private IQueryable<IEvent> ApplyTagConditions(IQueryable<IEvent> queryable, EventTagQuerySpec spec)
    {
        // Resolve the wire descriptors back to CLR types against the registered tag/event graph;
        // an unknown type raises UnknownTagQueryTypeException naming the descriptor.
        var knownTypes = _events.TagTypes.Select(x => x.TagType)
            .Concat(_events.AllKnownEventTypes().Select(x => x.EventType));
        var tagQuery = spec.Resolve(EventTagQuerySpec.ResolverFor(knownTypes));

        if (tagQuery.Conditions.Count == 0)
        {
            return queryable;
        }

        var e = Expression.Parameter(typeof(IEvent), "e");

        Expression? body = null;
        foreach (var condition in tagQuery.Conditions)
        {
            Expression branch = Expression.Call(
                _hasTagMethod.MakeGenericMethod(condition.TagType),
                e,
                Expression.Constant(condition.TagValue, condition.TagType));

            if (condition.EventType != null)
            {
                var eventTypeName = _events.EventMappingFor(condition.EventType).EventTypeName;
                branch = Expression.AndAlso(
                    branch,
                    Expression.Equal(
                        Expression.Property(e, typeof(IEvent).GetProperty(nameof(IEvent.EventTypeName))!),
                        Expression.Constant(eventTypeName)));
            }

            body = body == null ? branch : Expression.OrElse(body, branch);
        }

        return queryable.Where(Expression.Lambda<Func<IEvent, bool>>(body!, e));
    }

    public async Task<IReadOnlyList<IEvent>> FetchStreamAsync(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0, CancellationToken token = default)
    {
        return await FetchStreamInternalAsync(streamId, version, timestamp, fromVersion, token);
    }

    public async Task<IReadOnlyList<IEvent>> FetchStreamAsync(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0, CancellationToken token = default)
    {
        return await FetchStreamInternalAsync(streamKey, version, timestamp, fromVersion, token);
    }

    private async Task<IReadOnlyList<IEvent>> FetchStreamInternalAsync(object streamId, long version,
        DateTimeOffset? timestamp, long fromVersion, CancellationToken token)
    {
        await _session.EnsureEventStoreSchemaAsync(token); // #219: create event store on first use
        // #57 pc_events half: column projection + per-row hydration live in
        // PcEventsRowReader, shared with the IEventStore explorer's
        // ReadStreamAsync path. This method only composes WHERE / ORDER BY.
        await using var cmd = new SqlCommand();

        var sql = $"""
            SELECT {PcEventsRowReader.ComposeSelectColumns(_events.EventOptions)}
            FROM {_events.EventsTableName}
            WHERE stream_id = @stream_id AND tenant_id = @tenant_id AND is_archived = 0
            """;

        cmd.Parameters.AddIdParameter("@stream_id", streamId);
        cmd.Parameters.AddVarChar("@tenant_id", _session.TenantId);

        if (version > 0)
        {
            sql += " AND version <= @version";
            cmd.Parameters.AddWithValue("@version", version);
        }

        if (timestamp.HasValue)
        {
            sql += " AND timestamp <= @timestamp";
            cmd.Parameters.AddWithValue("@timestamp", timestamp.Value);
        }

        if (fromVersion > 0)
        {
            sql += " AND version >= @from_version";
            cmd.Parameters.AddWithValue("@from_version", fromVersion);
        }

        sql += " ORDER BY version;";
        cmd.CommandText = sql;

        var ctx = new EventHydrationContext(
            _events,
            _session.Serializer,
            streamId,
            defaultTenantId: _session.TenantId);

        // Per-batch hoists: compute the optional-metadata column ordinals
        // once, declare a single-slot type→mapping cache, pick the
        // StreamIdentity specialization once. Per-row reads have zero
        // option-flag branches and ~1 EventMappingFor lookup per distinct
        // event-type-in-stream.
        var slots = MetadataSlots.Compute(_events.EventOptions);
        var cache = new EventTypeCache();

        var results = new List<IEvent>();
        await using var reader = await _session.ExecuteReaderAsync(cmd, token);

        if (_events.StreamIdentity == StreamIdentity.AsGuid)
        {
            while (await reader.ReadAsync(token))
            {
                var @event = PcEventsRowReader.ReadEventAsGuid(reader, ctx, slots, ref cache);
                if (@event != null) results.Add(@event);
            }
        }
        else
        {
            while (await reader.ReadAsync(token))
            {
                var @event = PcEventsRowReader.ReadEventAsString(reader, ctx, slots, ref cache);
                if (@event != null) results.Add(@event);
            }
        }

        return results;
    }

    public async Task<IEvent<T>?> LoadAsync<T>(Guid id, CancellationToken token = default) where T : class
    {
        var @event = await LoadInternalAsync(id, token);
        return @event as IEvent<T>;
    }

    public Task<IEvent?> LoadAsync(Guid id, CancellationToken token = default)
        => LoadInternalAsync(id, token);

    private async Task<IEvent?> LoadInternalAsync(Guid id, CancellationToken token)
    {
        await _session.EnsureEventStoreSchemaAsync(token); // #219: create event store on first use
        // Mirrors FetchStreamInternalAsync but filters by the event UUID
        // rather than stream id, and reads the row's stream_id column to
        // assemble the context (since the caller doesn't know the stream
        // up-front for a load-by-event-id lookup).
        await using var cmd = new SqlCommand();
        cmd.CommandText = $"""
            SELECT {PcEventsRowReader.ComposeSelectColumns(_events.EventOptions)}
            FROM {_events.EventsTableName}
            WHERE id = @id AND tenant_id = @tenant_id AND is_archived = 0;
            """;
        cmd.Parameters.AddWithValue("@id", id); // event id — uniqueidentifier
        cmd.Parameters.AddVarChar("@tenant_id", _session.TenantId);

        await using var reader = await _session.ExecuteReaderAsync(cmd, token);
        if (!await reader.ReadAsync(token)) return null;

        // Pull stream_id off the row so PcEventsRowReader's stream-id
        // assignment (driven by ctx.StreamId) gets the right value. Ordinal 2
        // matches PcEventsRowReader.ComposeSelectColumns(...).
        object streamId = _events.StreamIdentity == StreamIdentity.AsGuid
            ? reader.GetGuid(2)
            : reader.GetString(2);

        var ctx = new EventHydrationContext(
            _events,
            _session.Serializer,
            streamId,
            defaultTenantId: _session.TenantId);

        var slots = MetadataSlots.Compute(_events.EventOptions);
        var cache = new EventTypeCache();

        return _events.StreamIdentity == StreamIdentity.AsGuid
            ? PcEventsRowReader.ReadEventAsGuid(reader, ctx, slots, ref cache)
            : PcEventsRowReader.ReadEventAsString(reader, ctx, slots, ref cache);
    }

    public async Task<StreamState?> FetchStreamStateAsync(Guid streamId, CancellationToken token = default)
    {
        return await FetchStreamStateInternalAsync(streamId, token);
    }

    public async Task<StreamState?> FetchStreamStateAsync(string streamKey, CancellationToken token = default)
    {
        return await FetchStreamStateInternalAsync(streamKey, token);
    }

    private async Task<StreamState?> FetchStreamStateInternalAsync(object streamId, CancellationToken token)
    {
        await _session.EnsureEventStoreSchemaAsync(token); // #219: create event store on first use
        // #57: column projection + row read live in PcStreamsRowReader so this
        // method, GetRecentStreamsAsync, and GetStreamMetadataAsync all read
        // pc_streams with the same shape. Note the canonical column order
        // (created before timestamp) differs from the historical order this
        // method used (timestamp before created) — the typed reader normalizes.
        await using var cmd = new SqlCommand();
        cmd.CommandText = $"""
            SELECT {PcStreamsRowReader.SelectColumns}
            FROM {_events.StreamsTableName}
            WHERE id = @id AND tenant_id = @tenant_id;
            """;
        cmd.Parameters.AddIdParameter("@id", streamId);
        cmd.Parameters.AddVarChar("@tenant_id", _session.TenantId);

        await using var reader = await _session.ExecuteReaderAsync(cmd, token);
        if (await reader.ReadAsync(token))
        {
            return PcStreamsRowReader.ReadStreamState(reader, _events.StreamIdentity, _events);
        }

        return null;
    }

    public async Task<T?> AggregateStreamAsync<T>(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, T? state = null, long fromVersion = 0,
        CancellationToken token = default) where T : class
    {
        return await AggregateStreamInternalAsync<T>(streamId, version, timestamp, state, fromVersion, token);
    }

    public async Task<T?> AggregateStreamAsync<T>(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, T? state = null, long fromVersion = 0,
        CancellationToken token = default) where T : class
    {
        return await AggregateStreamInternalAsync<T>(streamKey, version, timestamp, state, fromVersion, token);
    }

    private async Task<T?> AggregateStreamInternalAsync<T>(object streamId, long version,
        DateTimeOffset? timestamp, T? state, long fromVersion,
        CancellationToken token) where T : class
    {
        IReadOnlyList<IEvent> events;
        if (streamId is Guid guid)
            events = await FetchStreamAsync(guid, version, timestamp, fromVersion, token);
        else
            events = await FetchStreamAsync((string)streamId, version, timestamp, fromVersion, token);

        if (events.Count == 0) return state;

        var aggregator = _options.Projections.AggregatorFor<T>();
        var aggregate = await aggregator.BuildAsync(events, _session, state, token);
        if (aggregate == null) return null;

        TrySetIdentity(aggregate, streamId);
        return aggregate;
    }

    public async Task<T?> AggregateStreamToLastKnownAsync<T>(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, CancellationToken token = default) where T : class
    {
        return await AggregateStreamToLastKnownInternalAsync<T>(streamId, version, timestamp, token);
    }

    public async Task<T?> AggregateStreamToLastKnownAsync<T>(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, CancellationToken token = default) where T : class
    {
        return await AggregateStreamToLastKnownInternalAsync<T>(streamKey, version, timestamp, token);
    }

    private async Task<T?> AggregateStreamToLastKnownInternalAsync<T>(object streamId, long version,
        DateTimeOffset? timestamp, CancellationToken token) where T : class
    {
        IReadOnlyList<IEvent> events;
        if (streamId is Guid guid)
            events = await FetchStreamAsync(guid, version, timestamp, 0, token);
        else
            events = await FetchStreamAsync((string)streamId, version, timestamp, 0, token);

        if (events.Count == 0) return null;

        var aggregator = _options.Projections.AggregatorFor<T>();
        var eventList = events.ToList();

        T? aggregate = null;
        while (aggregate == null && eventList.Count > 0)
        {
            aggregate = await aggregator.BuildAsync(eventList, _session, default, token);
            eventList = eventList.SkipLast(1).ToList();
        }

        if (aggregate != null)
        {
            TrySetIdentity(aggregate, streamId);
        }

        return aggregate;
    }

    public async ValueTask<T?> FetchLatest<T>(Guid id, CancellationToken cancellation = default)
        where T : class
    {
        if (_session.TryGetAggregateFromIdentityMap<T, Guid>(id, out var cached))
        {
            return cached;
        }

        // #463: an Inline-projected aggregate is READ from its projected document rather than
        // re-aggregated off the stream. See CanReadInlineDocument.
        if (CanReadInlineDocument<T>(typeof(Guid)))
        {
            return await _session.LoadAsync<T>(id, cancellation);
        }

        return await AggregateStreamAsync<T>(id, token: cancellation);
    }

    public async ValueTask<T?> FetchLatest<T>(string key, CancellationToken cancellation = default)
        where T : class
    {
        if (_session.TryGetAggregateFromIdentityMap<T, string>(key, out var cached))
        {
            return cached;
        }

        // #463: see CanReadInlineDocument.
        if (CanReadInlineDocument<T>(typeof(string)))
        {
            return await _session.LoadAsync<T>(key, cancellation);
        }

        return await AggregateStreamAsync<T>(key, token: cancellation);
    }

    /// <summary>
    ///     #463: is <typeparamref name="T" /> the subject of an <c>Inline</c> projection whose
    ///     projected document can be loaded by a <paramref name="keyType" />-typed identity?
    /// </summary>
    /// <remarks>
    ///     Polecat used to live-aggregate the stream for <em>every</em> <c>FetchLatest&lt;T&gt;</c>,
    ///     whatever <c>T</c>'s lifecycle. Marten does not: its fetch planner routes an Inline
    ///     aggregate to <c>FetchInlinedPlan</c>, which simply loads the projected document. The
    ///     visible difference is on a stream that exists but holds nothing <c>T</c> owns -- Marten
    ///     finds no document and returns <c>null</c>, while Polecat aggregated the foreign events
    ///     and handed back whatever the aggregator constructed.
    ///     <para>
    ///     For an aggregate whose handlers are conventional <c>Create</c>/<c>Apply</c> methods the
    ///     old path came out <c>null</c> anyway, because nothing built an instance. The shape that
    ///     surfaced this is a single catch-all <c>Evolve(IEvent)</c>, which accepts every event type
    ///     by construction: the aggregator default-constructed an instance, the switch inside matched
    ///     nothing, and the default came back as though it were state. Since
    ///     <c>FetchLatest&lt;T&gt;(key) is null</c> is the idiomatic "does this aggregate exist?"
    ///     probe that code branching between <c>StartStream</c> and <c>Append</c> depends on, that
    ///     probe was satisfied by any stream key holding events at all -- so the answer depended on
    ///     whether some other aggregate happened to share the key space.
    ///     </para>
    ///     <para>
    ///     Reading the document is also what the persistence side already believed: the inline
    ///     projection screens streams it does not own out of the apply pass, so no row was ever
    ///     written for them. The two halves now agree.
    ///     </para>
    ///     <para>
    ///     Deliberately Inline only, mirroring <c>InlineFetchPlanner</c>. Marten routes Async
    ///     aggregates to the document only when the mapping is revisioned, and falls back to live
    ///     aggregation otherwise; Live aggregates have no document to read.
    ///     </para>
    ///     <para>
    ///     The <paramref name="keyType" /> check matters because the stream identity and the
    ///     aggregate's document id are not always the same type. A natural key resolves to a stream
    ///     <em>key</em> (string) for an aggregate whose document id is a Guid, and that key cannot
    ///     address the document at all -- so those fall back to live aggregation, exactly as before.
    ///     <c>InnerIdType</c> rather than <c>IdType</c> so a strongly-typed id still matches on the
    ///     value it wraps; the storage layer re-wraps it on the way through.
    ///     </para>
    /// </remarks>
    private bool CanReadInlineDocument<T>(Type keyType) where T : class
        => _options.Projections.TryFindAggregate(typeof(T), out var projection)
           && projection.Lifecycle == ProjectionLifecycle.Inline
           && _session.Providers.GetProvider<T>().Mapping.InnerIdType == keyType;

    internal static void TrySetIdentity<T>(T aggregate, object streamId) where T : class
    {
        var hasId = _hasIdCache.GetOrAdd(typeof(T), static t =>
            DocumentMapping.FindIdProperty(t) != null);

        if (!hasId) return;

        var idProp = DocumentMapping.FindIdProperty(typeof(T))!;
        if (idProp.PropertyType.IsInstanceOfType(streamId))
        {
            idProp.SetValue(aggregate, streamId);
        }
    }
}
