using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Schema;
using Polecat.Internal;
using Polecat.Storage;
using Weasel.Storage;

namespace Polecat.Events.Daemon;

/// <summary>
///     marten#6003 / #1053: one identity's events from one source, the store side of
///     <c>IEventStore.LoadEventsForIdentityAsync</c>: a stream, a DCB tag, a projection's slice index, or a scan of
///     the projection's event types. Used to replay, discard and rebuild one identity of a multi-stream projection and
///     by the single-identity stepper.
/// </summary>
/// <remarks>
///     The same rules as <see cref="IdentityRebuildEventLoader" />: the projection's event types (with its framework
///     markers), archived events only when it reads them, the daemon loader's row hydration, and a row skipped under
///     <c>SkipUnknownEvents</c> or the request's <c>SkipSerializationErrors</c> doesn't shorten the answer.
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: hydrates IEvent rows through the daemon loader, as PolecatEventLoader does.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: event deserialization, as in PolecatEventLoader.")]
internal sealed class IdentityEventSourceLoader
{
    private readonly DocumentStore _store;
    private readonly PolecatDatabase _database;

    public IdentityEventSourceLoader(DocumentStore store, PolecatDatabase database)
    {
        _store = store;
        _database = database;
    }

    public async Task<IReadOnlyList<IEvent>> LoadAsync(IdentityEventLoadRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);

        var events = _store.Options.EventGraph;
        IReadOnlyList<IEvent> loaded;

        if (request.Source == IdentityEventSource.Stream)
        {
            if (request.Identity == null || request.TenantId == null)
            {
                throw new ArgumentOutOfRangeException(nameof(request),
                    "Reading one stream's events needs the stream's identity and its tenant");
            }

            loaded = await new IdentityRebuildEventLoader(_store, _database).LoadAsync(
                new IdentityRebuildEventRequest(request.TenantId, request.Identity, request.AfterSequence,
                    request.Ceiling, request.Limit)
                {
                    EventTypes = request.EventTypes,
                    IncludeArchivedEvents = request.IncludeArchivedEvents,
                    SkipSerializationErrors = request.SkipSerializationErrors
                }, token).ConfigureAwait(false);
        }
        else
        {
            loaded = await loadAsync(request, token).ConfigureAwait(false);
        }

        if (request.IncludeTags || request.Source == IdentityEventSource.Tag)
        {
            await EventTagHydrator.HydrateAsync(_database.ConnectionString, events, loaded, null, token)
                .ConfigureAwait(false);
        }

        return loaded;
    }

    private async Task<IReadOnlyList<IEvent>> loadAsync(IdentityEventLoadRequest request, CancellationToken token)
    {
        var events = _store.Options.EventGraph;
        var limit = Math.Max(request.Limit, 1);
        var errors = _store.Options.Projections.Errors;
        var allowList = allowListFor(request);

        var hydrator = new PolecatEventLoader(events, _store.Options, _database.ConnectionString);

        await using var conn = new SqlConnection(_database.ConnectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);

        var loaded = new List<IEvent>();
        var floor = request.AfterSequence;

        while (loaded.Count < limit)
        {
            var wanted = limit - loaded.Count;
            await using var cmd = buildCommand(conn, allowList, request, floor, wanted);

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
                    catch (EventDeserializationFailureException) when (request.SkipSerializationErrors)
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

            if (rows < wanted) break;
        }

        return loaded;
    }

    // The projection's own allow list, framework markers and all, when the request names a registered projection
    private EventTypeAllowList allowListFor(IdentityEventLoadRequest request)
    {
        var events = _store.Options.EventGraph;

        if (request.ProjectionName != null
            && _store.Options.Projections.TryFindProjection(request.ProjectionName, out var source)
            && source is EventFilterable filterable
            && filterable.IncludedEventTypes.ToHashSet().SetEquals(request.EventTypes))
        {
            return EventTypeAllowList.For(events, filterable);
        }

        return EventTypeAllowList.For(events, request.EventTypes);
    }

    private SqlCommand buildCommand(SqlConnection conn, EventTypeAllowList allowList, IdentityEventLoadRequest request,
        long floor, int limit)
    {
        var events = _store.Options.EventGraph;
        var unicode = events.UnicodeKeys;
        var conjoined = events.TenancyStyle == TenancyStyle.Conjoined;
        var cmd = conn.CreateCommand();

        var sql = new StringBuilder("SELECT TOP(@limit) ")
            .Append(Internal.PcEventsRowReader.ComposeSelectColumns(_store.Options.Events))
            .Append(" FROM ").Append(events.EventsTableName)
            .Append(" WHERE seq_id > @floor AND seq_id <= @ceiling");

        cmd.Parameters.Add(new SqlParameter("@limit", SqlDbType.Int) { Value = limit });
        cmd.Parameters.Add(new SqlParameter("@floor", SqlDbType.BigInt) { Value = floor });
        cmd.Parameters.Add(new SqlParameter("@ceiling", SqlDbType.BigInt) { Value = request.Ceiling });

        switch (request.Source)
        {
            case IdentityEventSource.Tag:
            {
                var registration = events.TagTypes.FirstOrDefault(x => x.TagType == request.TagType)
                                   ?? throw new ArgumentOutOfRangeException(nameof(request),
                                       $"{request.TagType?.FullName ?? "(no tag type)"} is not a registered tag type");
                var table = SqlEscaping.QualifiedName(events.DatabaseSchemaName, EventGraph.TagTableNameFor(registration));

                sql.Append($" AND seq_id IN (SELECT seq_id FROM {table} WHERE value = @value");
                if (conjoined) sql.Append(" AND tenant_id = @tenant");
                sql.Append(')');
                if (conjoined) sql.Append(" AND tenant_id = @tenant");

                addValue(cmd, request.IdentityValue!, unicode);
                if (conjoined) cmd.Parameters.AddKey("@tenant", request.TenantId, unicode);
                break;
            }

            case IdentityEventSource.SliceIndex:
            {
                var table = SqlEscaping.QualifiedName(events.DatabaseSchemaName,
                    SliceIndexTable.TableNameFor(request.ProjectionName!, request.ProjectionVersion));

                // Whatever tenant the event itself is in, except that tenant-partitioned events number each tenant's
                // events on their own
                sql.Append($" AND seq_id IN (SELECT seq_id FROM {table} WHERE tenant_id = @tenant AND identity_value = @value)");
                if (events.UseTenantPartitionedEvents) sql.Append(" AND tenant_id = @tenant");

                addValue(cmd, request.IdentityValue!, unicode);
                cmd.Parameters.AddKey("@tenant", request.TenantId, unicode);
                break;
            }

            case IdentityEventSource.Scan:
                if (conjoined && request.TenantId != null)
                {
                    sql.Append(" AND tenant_id = @tenant");
                    cmd.Parameters.AddKey("@tenant", request.TenantId, unicode);
                }

                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(request), $"Unknown identity event source {request.Source}");
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
                var sqlParameter = new SqlParameter(parameter.Name, parameter.Value ?? DBNull.Value);
                if (parameter.DbType is { } dbType) sqlParameter.DbType = dbType;
                sqlParameter.Size = sqlParameter.SqlDbType switch
                {
                    SqlDbType.VarChar => 8000,
                    SqlDbType.NVarChar => -1,
                    _ => sqlParameter.Size
                };
                cmd.Parameters.Add(sqlParameter);
            }
        }

        sql.Append(" ORDER BY seq_id;");
        cmd.CommandText = sql.ToString();
        return cmd;
    }

    private static void addValue(SqlCommand cmd, object value, bool unicode)
    {
        switch (value)
        {
            case string text:
                cmd.Parameters.AddKey("@value", text, unicode);
                break;
            case Guid guid:
                cmd.Parameters.Add("@value", SqlDbType.UniqueIdentifier).Value = guid;
                break;
            case int number:
                cmd.Parameters.Add("@value", SqlDbType.Int).Value = number;
                break;
            case long number:
                cmd.Parameters.Add("@value", SqlDbType.BigInt).Value = number;
                break;
            default:
                cmd.Parameters.AddWithValue("@value", value);
                break;
        }
    }
}
