using System.Diagnostics.CodeAnalysis;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;

using Polecat.Exceptions;
using Polecat.Internal;
namespace Polecat.Events.Daemon;

/// <summary>
///     SQL Server implementation of IEventLoader.
///     Loads event batches by seq_id range from pc_events.
///     Resilience (Polly) + loading metrics are layered on by the lifted
///     <see cref="ResilientEventLoader"/> decorator (jasperfx#329) that wraps
///     this loader at <c>BuildEventLoader</c> time — this class is the bare
///     inner loader.
/// </summary>
/// <remarks>
///     #550: a subscription or projection that names its event types gets that allow-list pushed into
///     the SQL as <c>dotnet_type in (...)</c>, so non-matching rows are never read off the wire,
///     hydrated or deserialized — the same strategy as Marten's <c>EventTypeFilter</c> fragment and
///     fisher#153. The whole command text is composed once at construction too, because every part of
///     it (table name, tenant predicate, type filter) is fixed for the loader's lifetime. See the
///     constructor for why the page's ceiling accounting is <em>more</em> correct with the filter
///     server-side, not less.
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: hydrates IEvent batches via EventGraph.Wrap (routed through ISerializer.FromJson). Event types are preserved by EventGraph registration on the caller side per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson and Event<T>.MakeGenericType are annotated RDC. AOT consumers register concrete event types ahead of time.")]
internal class PolecatEventLoader : IEventLoader
{
    /// <summary>
    ///     SQL Server rejects a command with more than 2100 parameters outright. An allow-list anywhere
    ///     near this size is not a real subscription, but a shard that dies on a SqlException would be a
    ///     far worse outcome than one that filters client-side, so the push-down bows out above this and
    ///     the client-side check below carries the filtering on its own.
    /// </summary>
    private const int MaximumPushedDownTypeNames = 2000;

    private readonly EventGraph _events;
    private readonly StoreOptions _options;
    private readonly string _connectionString;
    private readonly HashSet<string>? _allowedDotNetTypes;
    private readonly string? _tenantFilter;
    private readonly string _commandText;
    private readonly string[]? _pushedDownTypeNames;

    public PolecatEventLoader(EventGraph events, StoreOptions options, string connectionString,
        EventFilterable? filtering = null, string? tenantFilter = null)
    {
        _events = events;
        _options = options;
        _connectionString = connectionString;
        // #163 Phase 2: a per-tenant rebuild/subscription shard scopes the load to one tenant so it
        // reads only that tenant's bounded sequence range, not the whole store. Null = store-global.
        _tenantFilter = tenantFilter;

        // Build an allow list of dotnet_type names from the included event types
        if (filtering?.IncludedEventTypes is { Count: > 0 } includedTypes)
        {
            _allowedDotNetTypes = new HashSet<string>(StringComparer.Ordinal);
            foreach (var type in includedTypes)
            {
                var mapping = events.EventMappingFor(type);
                _allowedDotNetTypes.Add(mapping.DotNetTypeName);
            }

            if (_allowedDotNetTypes.Count <= MaximumPushedDownTypeNames)
            {
                // Materialized into a stable array because a HashSet's enumeration order is
                // unspecified, and the parameter names rendered into the SQL have to line up with the
                // values bound on each load.
                _pushedDownTypeNames = _allowedDotNetTypes.ToArray();
            }
        }

        _commandText = BuildCommandText();
    }

    /// <summary>
    ///     Compose the one SELECT this loader will ever issue. Every input is fixed for the loader's
    ///     lifetime, so rebuilding the string per page — as this used to — bought nothing.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>Why pushing the type filter down keeps the ceiling honest.</b>
    ///         <c>EventPage.CalculateCeiling</c>'s contract is "a page that did not fill the batch
    ///         exhausted everything up to the bound it was given". With the filter in SQL, a query that
    ///         comes back with fewer than <c>BatchSize</c> <em>matching</em> rows really has scanned the
    ///         whole <c>(floor, high-water]</c> range, so claiming the high-water mark as the ceiling
    ///         steps the floor past every non-matching sequence in that range in a single page — which
    ///         is exactly what stops a shard stalling behind a long run of events it will never apply. A
    ///         page that does fill the batch claims only its last matched sequence, as before; the rows
    ///         above it are re-scanned by the next request and still match nothing.
    ///     </para>
    ///     <para>
    ///         Filtering client-side, as this loader used to, made that same claim <em>dishonestly</em>:
    ///         <c>TOP(@batchSize)</c> applied before the filter, so a page whose rows were all discarded
    ///         came back empty, failed the "did not fill the batch" test, and reported the high-water
    ///         mark as its ceiling — after looking at only the first <c>BatchSize</c> rows of the range.
    ///         Any matching event further up that range was stepped over and never delivered. Pushing
    ///         the filter down is therefore a correctness fix as much as a performance one.
    ///     </para>
    ///     <para>
    ///         The <c>dotnet_type is null</c> disjunct preserves today's behaviour precisely: the
    ///         client-side check only ever excluded a row that <em>had</em> a dotnet_type, so a null one
    ///         still reaches hydration (where it fails or is skipped per the shard's error options)
    ///         rather than being silently dropped by a filtered shard. The column is nullable — see
    ///         <c>EventsTable</c>.
    ///     </para>
    /// </remarks>
    private string BuildCommandText()
    {
        var tenantPredicate = _tenantFilter != null ? " AND tenant_id = @tenant" : "";

        var typePredicate = "";
        if (_pushedDownTypeNames is { Length: > 0 })
        {
            var markers = string.Join(", ", Enumerable.Range(0, _pushedDownTypeNames.Length).Select(i => $"@t{i}"));
            typePredicate = $" AND (dotnet_type IS NULL OR dotnet_type IN ({markers}))";
        }

        return $"""
            SELECT TOP(@batchSize) seq_id, id, stream_id, version, data, type, timestamp,
                tenant_id, dotnet_type, is_archived, bdata
            FROM {_events.EventsTableName}
            WHERE seq_id > @floor AND seq_id <= @ceiling AND is_archived = 0{tenantPredicate}{typePredicate}
            ORDER BY seq_id;
            """;
    }

    public async Task<EventPage> LoadAsync(EventRequest request, CancellationToken token)
    {
        try
        {
            return await LoadInternalAsync(request, token);
        }
        catch (Exception ex) when (token.IsCancellationRequested && ex is not OperationCanceledException)
        {
            // The daemon cancelled this load mid-flight — e.g. shutting the shard down after a
            // CatchUpAsync reaches the high-water mark. SqlClient surfaces that cancellation as a
            // SqlException ("Operation cancelled by user" / "the batch is aborted ... the session is
            // busy") rather than a clean OperationCanceledException, and the async daemon's recorder
            // would otherwise treat it as a real shard error. Translate it to a cooperative
            // cancellation so the daemon ignores it as the benign shutdown it is.
            throw new OperationCanceledException("Event loading was cancelled.", ex, token);
        }
    }

    private async Task<EventPage> LoadInternalAsync(EventRequest request, CancellationToken token)
    {
        var page = new EventPage(request.Floor);

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = _commandText;

        cmd.Parameters.AddWithValue("@batchSize", request.BatchSize);
        cmd.Parameters.AddWithValue("@floor", request.Floor);
        cmd.Parameters.AddWithValue("@ceiling", request.HighWater);
        if (_tenantFilter != null) cmd.Parameters.AddVarChar("@tenant", _tenantFilter);

        if (_pushedDownTypeNames is not null)
        {
            // AddVarChar rather than AddWithValue (#363): dotnet_type is varchar, and binding these as
            // nvarchar would put CONVERT_IMPLICIT on the column side of the IN.
            for (var i = 0; i < _pushedDownTypeNames.Length; i++)
            {
                cmd.Parameters.AddVarChar($"@t{i}", _pushedDownTypeNames[i]);
            }
        }

        var skippedEvents = 0;

        await using var reader = await cmd.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            var seqId = reader.GetInt64(0);
            var eventId = reader.GetGuid(1);
            var rawStreamId = reader.GetValue(2);
            var eventVersion = reader.GetInt64(3);
            var json = reader.GetString(4);
            var typeName = reader.GetString(5);
            var eventTimestamp = reader.GetDateTimeOffset(6);
            var tenantId = reader.GetString(7);
            var dotNetTypeName = reader.IsDBNull(8) ? null : reader.GetString(8);
            var isArchived = reader.GetBoolean(9);
            // #388: non-null bdata means this row's payload is binary, not JSON.
            var bdata = reader.IsDBNull(10) ? null : reader.GetFieldValue<byte[]>(10);

            // #550: how far this query actually got, whether or not the row survives into the page.
            // EventPage.CalculateCeiling uses it to avoid claiming progress past rows nobody read when a
            // full batch is entirely skipped (jasperfx#667) — the case the fallback path below can
            // produce on an allow-list too large to push into SQL.
            page.LastObservedSequence = seqId;

            // Apply event type allow-list filter (skip events not in the subscription's filter).
            // #550: normally dead code — the SQL above already excluded these rows — and kept as belt
            // and braces in case the rendered filter and the set ever disagree. It is the only filter
            // when the allow-list was too large to push down. Counting a discarded row as skipped is
            // what keeps the ceiling honest in that fallback: the row genuinely consumed a slot of this
            // page's range, so the batch was saturated even though the page came back short.
            if (_allowedDotNetTypes != null && dotNetTypeName != null &&
                !_allowedDotNetTypes.Contains(dotNetTypeName))
            {
                skippedEvents++;
                continue;
            }

            var resolvedType = _events.ResolveEventType(dotNetTypeName);
            if (resolvedType == null)
            {
                if (request.ErrorOptions.SkipUnknownEvents)
                {
                    skippedEvents++;
                    continue;
                }

                // #368 / jasperfx#565: a typed exception carrying its own ShardFailureCategory, so a shard
                // paused by an unregistered event type reports UnknownEventType with the offending
                // sequence rather than classifying as Other with no detail. The daemon does not sniff
                // exception type names — the exception has to declare its own kind.
                throw new UnknownEventTypeException(dotNetTypeName, seqId);
            }

            object data;
            try
            {
                data = _events.DeserializeEventData(resolvedType, json, bdata, _options.Serializer);
            }
            catch (Exception ex)
            {
                if (request.ErrorOptions.SkipSerializationErrors)
                {
                    skippedEvents++;
                    continue;
                }

                // #368 / jasperfx#565: report the store's type alias (the `type` column) rather than the
                // assembly-qualified dotnet_type, matching what ShardFailure.Event.EventTypeName carries
                // everywhere else and what a client-side consumer can act on.
                throw new EventDeserializationFailureException(seqId, typeName, ex);
            }

            var mapping = _events.EventMappingFor(resolvedType);
            var @event = mapping.Wrap(data);

            @event.Id = eventId;
            @event.Sequence = seqId;
            @event.Version = eventVersion;
            @event.Timestamp = eventTimestamp;
            @event.TenantId = tenantId;
            @event.EventTypeName = typeName;
            @event.DotNetTypeName = dotNetTypeName!;
            @event.IsArchived = isArchived;

            if (_events.StreamIdentity == StreamIdentity.AsGuid && rawStreamId is Guid g)
            {
                @event.StreamId = g;
            }
            else
            {
                @event.StreamKey = rawStreamId.ToString();
            }

            page.Add(@event);
        }

        page.CalculateCeiling(request.BatchSize, request.HighWater, skippedEvents);
        return page;
    }
}
