using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Internal;

namespace Polecat.Events.Fetching;

/// <summary>
///     Resolves a stream identity from a natural key value using the pc_natural_key_{type} table,
///     then delegates to the standard FetchForWriting flow.
/// </summary>
internal static class NaturalKeyFetchPlanner
{
    /// <summary>
    ///     Fetch a stream for writing by natural key. Resolves the natural key to a stream id
    ///     via the pc_natural_key_{type} table, then fetches the stream with standard locking.
    /// </summary>
    public static async Task<IEventStream<T>> FetchForWritingByNaturalKey<T, TId>(
        DocumentSessionBase session,
        EventGraph events,
        WorkTracker workTracker,
        NaturalKeyDefinition naturalKey,
        TId naturalKeyValue,
        string tenantId,
        CancellationToken cancellation) where T : class where TId : notnull
    {
        var isGuidStream = events.StreamIdentity == StreamIdentity.AsGuid;
        var schema = events.DatabaseSchemaName;
        var tableName = $"pc_natural_key_{naturalKey.AggregateType.Name.ToLowerInvariant()}";
        var streamColumn = isGuidStream ? "stream_id" : "stream_key";

        // Unwrap strong-typed id to primitive
        var unwrapped = naturalKey.Unwrap(naturalKeyValue) ?? throw new ArgumentNullException(
            nameof(naturalKeyValue), "Natural key value cannot be null.");

        // Single-round-trip: look up stream id from natural key table, join to streams for locking
        await session.BeginTransactionAsync(cancellation);

        await using var cmd = new SqlCommand();

        var tenantFilter = events.TenancyStyle == TenancyStyle.Conjoined
            ? " AND nk.tenant_id = @tenantId"
            : "";

        cmd.CommandText = $"""
            SELECT s.version, s.id
            FROM [{schema}].[{tableName}] nk WITH (NOLOCK)
            INNER JOIN {events.StreamsTableName} s WITH (UPDLOCK, HOLDLOCK) ON s.id = nk.{streamColumn}
            WHERE nk.natural_key_value = @naturalKey AND nk.is_archived = 0{tenantFilter}
            AND s.tenant_id = @tenantId;
            """;

        cmd.Parameters.AddWithValue("@naturalKey", unwrapped);
        cmd.Parameters.AddWithValue("@tenantId", tenantId);

        long version = 0;
        object? streamId = null;
        bool streamExists = false;

        await using (var reader = (SqlDataReader)await session.ExecuteReaderAsync(cmd, cancellation))
        {
            if (await reader.ReadAsync(cancellation))
            {
                version = reader.GetInt64(0);
                streamId = isGuidStream ? (object)reader.GetGuid(1) : reader.GetString(1);
                streamExists = true;
            }
        }

        if (!streamExists)
        {
            throw new InvalidOperationException(
                $"No active stream found for natural key '{naturalKeyValue}' on aggregate type '{typeof(T).Name}'.");
        }

        // Build aggregate from events
        T? aggregate = null;
        if (version > 0)
        {
            var queryEventStore = new QueryEventStore(session, events, session.Options);
            if (isGuidStream)
            {
                var guidId = (Guid)streamId!;
                var streamEvents = await queryEventStore.FetchStreamAsync(guidId, token: cancellation);
                if (streamEvents.Count > 0)
                {
                    var aggregator = session.Options.Projections.AggregatorFor<T>();
                    aggregate = await aggregator.BuildAsync(streamEvents, session, null, cancellation);
                    if (aggregate != null) QueryEventStore.TrySetIdentity(aggregate, guidId);
                }
            }
            else
            {
                var key = (string)streamId!;
                var streamEvents = await queryEventStore.FetchStreamAsync(key, token: cancellation);
                if (streamEvents.Count > 0)
                {
                    var aggregator = session.Options.Projections.AggregatorFor<T>();
                    aggregate = await aggregator.BuildAsync(streamEvents, session, null, cancellation);
                    if (aggregate != null) QueryEventStore.TrySetIdentity(aggregate, key);
                }
            }
        }

        // Create the StreamAction
        StreamAction action;
        if (isGuidStream)
        {
            var guidId = (Guid)streamId!;
            action = new StreamAction(guidId, StreamActionType.Append);
        }
        else
        {
            action = new StreamAction((string)streamId!, StreamActionType.Append);
        }

        action.ExpectedVersionOnServer = version;
        action.TenantId = tenantId;
        action.AggregateType = typeof(T);
        workTracker.AddStream(action);

        // Return the appropriate EventStream variant
        if (isGuidStream)
        {
            return new EventStream<T>(session, events, (Guid)streamId!, aggregate, cancellation, action);
        }
        else
        {
            return new EventStream<T>(session, events, (string)streamId!, aggregate, cancellation, action);
        }
    }
}
