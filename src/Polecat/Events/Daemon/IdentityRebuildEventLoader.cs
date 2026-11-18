using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Storage;
using Weasel.Storage;

namespace Polecat.Events.Daemon;

/// <summary>
///     Reads one stream's events for a per-identity rebuild's re-fold (#1060, jasperfx#1201): the store side of
///     <c>IEventStore.LoadEventsForIdentityRebuildAsync</c>. The port of Marten's loader (marten#6071).
/// </summary>
/// <remarks>
///     <para>
///         The same rules as <see cref="PolecatEventLoader" />, so the re-fold writes what the shard would have:
///         the same <see cref="EventTypeAllowList" /> (the projection's event types with <see cref="Archived" />
///         and <c>Compacted&lt;T&gt;</c>, the upcast sources, an interface or abstract type reading every event),
///         rendered by the same <see cref="SqlServerEventPagingDialect" />, the tenant under conjoined tenancy,
///         archived events only when the projection reads them, and the daemon loader's own row hydration, so
///         deserialization and upcasting are the daemon's.
///     </para>
///     <para>
///         A short answer means "no more". JasperFx stops paging when it gets fewer events than it asked for, so
///         a row skipped under <c>SkipUnknownEvents</c> or <c>SkipSerializationErrors</c> does not shorten the
///         answer: the read goes on until it has the limit or runs out of rows. A row the shard would not skip
///         throws, which faults the rebuild request and leaves the shard running.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: hydrates IEvent rows through the daemon loader, as PolecatEventLoader does. Event types are preserved by EventGraph registration per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: event deserialization, as in PolecatEventLoader. AOT consumers register concrete event types ahead of time.")]
internal sealed class IdentityRebuildEventLoader
{
    private readonly DocumentStore _store;
    private readonly PolecatDatabase _database;

    public IdentityRebuildEventLoader(DocumentStore store, PolecatDatabase database)
    {
        _store = store;
        _database = database;
    }

    public async Task<IReadOnlyList<IEvent>> LoadAsync(IdentityRebuildEventRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);

        var events = _store.Options.EventGraph;
        var streamId = StreamIdFor(request.StreamIdentity, events.StreamIdentity);
        var limit = Math.Max(request.Limit, 1);
        var errors = _store.Options.Projections.Errors;
        var allowList = EventTypeAllowList.For(events, eventTypesFor(request));

        // The daemon's loader, used only for its row hydration
        var hydrator = new PolecatEventLoader(events, _store.Options, _database.ConnectionString);

        await using var conn = new SqlConnection(_database.ConnectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);

        var loaded = new List<IEvent>();
        var floor = request.AfterSequence;

        while (loaded.Count < limit)
        {
            var wanted = limit - loaded.Count;
            await using var cmd = buildCommand(conn, allowList, request, streamId, floor, wanted);

            var rows = 0;
            await using (var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    rows++;
                    var sequence = reader.GetInt64(0);
                    floor = sequence;

                    IEvent? @event;
                    try
                    {
                        @event = await hydrator.ReadRowAsync(reader, token).ConfigureAwait(false);
                    }
                    catch (EventDeserializationFailureException) when (errors.SkipSerializationErrors)
                    {
                        continue;
                    }

                    if (@event == null)
                    {
                        if (errors.SkipUnknownEvents) continue;
                        throw PolecatEventLoader.UnknownEventTypeFor(reader, sequence);
                    }

                    loaded.Add(@event);
                }
            }

            // Fewer rows than asked for means the stream has nothing more in the range
            if (rows < wanted) break;
        }

        return loaded;
    }

    private SqlCommand buildCommand(SqlConnection conn, EventTypeAllowList allowList,
        IdentityRebuildEventRequest request, object streamId, long floor, int limit)
    {
        var events = _store.Options.EventGraph;
        var cmd = conn.CreateCommand();

        var sql = new StringBuilder("SELECT TOP(@limit) ")
            .Append(Internal.PcEventsRowReader.ComposeSelectColumns(_store.Options.Events))
            .Append(" FROM ").Append(events.EventsTableName)
            .Append(" WHERE seq_id > @floor AND seq_id <= @ceiling AND stream_id = @stream");

        cmd.Parameters.Add(new SqlParameter("@limit", SqlDbType.Int) { Value = limit });
        cmd.Parameters.Add(new SqlParameter("@floor", SqlDbType.BigInt) { Value = floor });
        cmd.Parameters.Add(new SqlParameter("@ceiling", SqlDbType.BigInt) { Value = request.Ceiling });
        cmd.Parameters.AddUncheckedKey("@stream", streamId, events.UnicodeKeys);

        if (events.TenancyStyle == TenancyStyle.Conjoined)
        {
            sql.Append(" AND tenant_id = @tenant");
            cmd.Parameters.AddKey("@tenant", request.TenantId, events.UnicodeKeys);
        }

        if (!request.IncludeArchivedEvents)
        {
            sql.Append(" AND is_archived = 0");
        }

        if (!allowList.IsEmpty)
        {
            var parameters = new List<EventPageParameter>();
            sql.Append(SqlServerEventPagingDialect.RenderTypeFilter(allowList, "", parameters));
            foreach (var parameter in parameters)
            {
                cmd.Parameters.Add(toSqlParameter(parameter));
            }
        }

        sql.Append(" ORDER BY seq_id;");
        cmd.CommandText = sql.ToString();
        return cmd;
    }

    // Sized as PolecatEventLoader.ConfigureCommand sizes the daemon's, so the plan cache gets one entry
    private static SqlParameter toSqlParameter(EventPageParameter parameter)
    {
        var sql = new SqlParameter(parameter.Name, parameter.Value ?? DBNull.Value);
        if (parameter.DbType is { } dbType) sql.DbType = dbType;

        sql.Size = sql.SqlDbType switch
        {
            SqlDbType.VarChar => 8000,
            SqlDbType.NVarChar => -1,
            _ => sql.Size
        };

        return sql;
    }

    /// <summary>
    ///     The request's event types plus the framework markers the daemon adds for the projection. The request
    ///     carries the event types but not the projection, so the single-stream aggregations it can be are where
    ///     <c>Compacted&lt;T&gt;</c> comes from, as in Marten's loader.
    /// </summary>
    private IReadOnlyList<Type> eventTypesFor(IdentityRebuildEventRequest request)
    {
        if (request.EventTypes.Count == 0) return [];

        var types = new List<Type>(request.EventTypes);

        var candidates = _store.Options.Projections.All
            .Where(x => x is IAggregateProjection { Scope: AggregationScope.SingleStream })
            .OfType<EventFilterable>()
            .Where(x => x.IncludeArchivedEvents == request.IncludeArchivedEvents
                        && x.IncludedEventTypes.ToHashSet().SetEquals(request.EventTypes));

        foreach (var candidate in candidates)
        {
            foreach (var marker in EventTypeAllowList.FrameworkMarkerTypes(candidate))
            {
                if (!types.Contains(marker)) types.Add(marker);
            }
        }

        return types;
    }

    /// <summary>
    ///     The stream id behind the projected identity: the identity itself when it is the store's Guid or
    ///     string, or the value a strong-typed id wraps.
    /// </summary>
    internal static object StreamIdFor(object identity, StreamIdentity streamIdentity)
    {
        var streamIdType = streamIdentity == StreamIdentity.AsGuid ? typeof(Guid) : typeof(string);

        if (identity.GetType() == streamIdType) return identity;

        if (StrongTypedIdResolver.TryResolve(identity.GetType(), out var info) && info.SimpleType == streamIdType)
        {
            return StrongTypedIdResolver.Unwrap(info, identity);
        }

        throw new ArgumentOutOfRangeException(nameof(identity),
            $"A per-identity rebuild was asked for '{identity}' ({identity.GetType().FullNameInCode()}), but this store's "
            + $"stream identity is {streamIdType.Name}. Pass a {streamIdType.Name} or a strong-typed id wrapping one.");
    }
}
