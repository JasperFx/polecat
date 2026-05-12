using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Events.Internal;
using Polecat.Storage;

namespace Polecat;

/// <summary>
///     IEventStore explorer / diagnostic methods (CritterWatch #143).
///     Implements the eight methods added in JasperFx.Events 1.36 against
///     Polecat's pc_streams / pc_events / pc_event_progression tables.
/// </summary>
public partial class DocumentStore
{
    async Task<IReadOnlyList<StreamSummary>> IEventStore.GetRecentStreamsAsync(
        int count, CancellationToken ct)
    {
        if (count <= 0) return Array.Empty<StreamSummary>();

        var results = new List<StreamSummary>(capacity: count);

        await using var conn = new SqlConnection(Options.ConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();
        // #57: share the column projection + row read with FetchStreamStateAsync
        // and GetStreamMetadataAsync via PcStreamsRowReader so all three sites
        // stay aligned when pc_streams' shape evolves.
        cmd.CommandText = $"""
            SELECT TOP (@count) {PcStreamsRowReader.SelectColumns}
            FROM {Events.StreamsTableName}
            ORDER BY timestamp DESC;
            """;
        cmd.Parameters.AddWithValue("@count", count);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add(PcStreamsRowReader.ReadStreamSummary(reader, Tenancy.DefaultTenantId));
        }

        return results;
    }

    async IAsyncEnumerable<EventRecord> IEventStore.ReadStreamAsync(
        string streamId, [EnumeratorCancellation] CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);

        // Resolve to the configured identity type
        object resolvedStreamId = Events.StreamIdentity == StreamIdentity.AsGuid
            ? Guid.Parse(streamId)
            : streamId;

        await using var conn = new SqlConnection(Options.ConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();

        var eventOptions = Events.EventOptions;
        var selectColumns = "id, seq_id, version, stream_id, type, data, timestamp, tenant_id";
        if (eventOptions.EnableHeaders) selectColumns += ", headers";

        cmd.CommandText = $"""
            SELECT {selectColumns}
            FROM {Events.EventsTableName}
            WHERE stream_id = @stream_id
            ORDER BY version;
            """;
        cmd.Parameters.AddWithValue("@stream_id", resolvedStreamId);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var eventId = reader.GetGuid(0);
            var sequence = reader.GetInt64(1);
            var version = reader.GetInt64(2);
            var streamIdRaw = reader.GetValue(3);
            var streamIdString = streamIdRaw switch
            {
                Guid g => g.ToString(),
                _ => streamIdRaw.ToString() ?? string.Empty
            };
            var typeName = reader.GetString(4);
            var rawData = reader.GetString(5);
            var timestamp = reader.GetDateTimeOffset(6);
            var tenantId = reader.IsDBNull(7) ? Tenancy.DefaultTenantId : reader.GetString(7);

            JsonElement? metadata = null;
            if (eventOptions.EnableHeaders && !reader.IsDBNull(8))
            {
                using var headerDoc = JsonDocument.Parse(reader.GetString(8));
                metadata = headerDoc.RootElement.Clone();
            }

            using var doc = JsonDocument.Parse(rawData);
            var data = doc.RootElement.Clone();

            yield return new EventRecord(
                eventId,
                sequence,
                version,
                streamIdString,
                typeName,
                data,
                metadata,
                timestamp,
                tenantId,
                Tags: null!);
        }
    }

    async Task<StreamMetadata?> IEventStore.GetStreamMetadataAsync(
        string streamId, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);

        object resolvedStreamId = Events.StreamIdentity == StreamIdentity.AsGuid
            ? Guid.Parse(streamId)
            : streamId;

        await using var conn = new SqlConnection(Options.ConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();
        // #57: share the pc_streams column projection + row read with the
        // other two stream-reading sites via PcStreamsRowReader. The JOIN'd
        // first_event_at column is appended after the canonical projection
        // (index 7) and threaded into ReadStreamMetadata explicitly.
        cmd.CommandText = $"""
            SELECT {PcStreamsRowReader.SelectColumnsWithAlias("s")},
                   MIN(e.timestamp) AS first_event_at
            FROM {Events.StreamsTableName} s
            LEFT JOIN {Events.EventsTableName} e ON e.stream_id = s.id AND e.tenant_id = s.tenant_id
            WHERE s.id = @id
            GROUP BY s.id, s.type, s.version, s.created, s.timestamp, s.tenant_id, s.is_archived;
            """;
        cmd.Parameters.AddWithValue("@id", resolvedStreamId);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        var firstEventAt = reader.IsDBNull(7)
            ? (DateTimeOffset?)null
            : reader.GetFieldValue<DateTimeOffset>(7);

        return PcStreamsRowReader.ReadStreamMetadata(reader, Tenancy.DefaultTenantId, firstEventAt);
    }

    IAsyncEnumerable<EventRecord> IEventStore.QueryByTagsAsync(
        IReadOnlyDictionary<string, string> tags, CancellationToken ct)
    {
        throw new NotSupportedException(
            "DCB tag-set queries are not yet supported on Polecat. " +
            "Tracked at https://github.com/JasperFx/polecat/issues — see the CritterWatch master plan for the DCB roadmap.");
    }

    Task<DcbProjectedState?> IEventStore.GetProjectedStateForTagsAsync(
        string projectionName, IReadOnlyDictionary<string, string> tags, CancellationToken ct)
    {
        throw new NotSupportedException(
            "DCB tag-set projection rehydration is not yet supported on Polecat. " +
            "Tracked at https://github.com/JasperFx/polecat/issues — see the CritterWatch master plan for the DCB roadmap.");
    }

    async Task<AggregateAtVersion<TAggregate>> IEventStore.RehydrateAtVersionAsync<TAggregate>(
        object identity, long version, CancellationToken ct) where TAggregate : class
    {
        ArgumentNullException.ThrowIfNull(identity);

        await using var session = QuerySession();

        IReadOnlyList<IEvent> events;
        if (Events.StreamIdentity == StreamIdentity.AsGuid)
        {
            var guid = identity is Guid g ? g : Guid.Parse(identity.ToString()!);
            events = await session.Events.FetchStreamAsync(guid, version: version, token: ct);
        }
        else
        {
            events = await session.Events.FetchStreamAsync(identity.ToString()!, version: version, token: ct);
        }

        TAggregate? state = default;
        long applied = 0;
        if (events.Count > 0)
        {
            var aggregator = Options.Projections.AggregatorFor<TAggregate>();
            state = await aggregator.BuildAsync(events, session, default!, ct);
            applied = events.Count;
        }

        return new AggregateAtVersion<TAggregate>(state!, version, applied);
    }

    async Task<AggregateAtVersion?> IEventStore.RehydrateAtVersionByNameAsync(
        string aggregateTypeName, object identity, long version, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(aggregateTypeName);
        ArgumentNullException.ThrowIfNull(identity);

        var aggregateType = ResolveAggregateType(aggregateTypeName)
            ?? throw new ArgumentException(
                $"Unknown aggregate type '{aggregateTypeName}'. Polecat resolves aggregate types from registered projections — register the projection or use the strong-typed RehydrateAtVersionAsync overload.",
                nameof(aggregateTypeName));

        var method = typeof(IEventStore)
            .GetMethod(nameof(IEventStore.RehydrateAtVersionAsync))!
            .MakeGenericMethod(aggregateType);

        var typedTask = (Task)method.Invoke(this, new[] { identity, version, (object)ct })!;
        await typedTask.ConfigureAwait(false);

        var resultProperty = typedTask.GetType().GetProperty("Result")!;
        var result = resultProperty.GetValue(typedTask)!;
        var stateValue = result.GetType().GetProperty("State")!.GetValue(result);
        var resolvedVersion = (long)result.GetType().GetProperty("Version")!.GetValue(result)!;
        var eventsApplied = (long)result.GetType().GetProperty("EventsApplied")!.GetValue(result)!;

        var stateJson = stateValue is null
            ? default
            : JsonDocument.Parse(Options.Serializer.ToJson(stateValue)).RootElement.Clone();

        return new AggregateAtVersion(aggregateType.FullName ?? aggregateTypeName, stateJson, resolvedVersion, eventsApplied);
    }

    async Task<IReadOnlyList<ProjectionStatus>> IEventStore.GetProjectionStatusesAsync(CancellationToken ct)
    {
        // Pull progression rows so we can attach processed sequence numbers to each shard
        var progress = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        long highWater = 0;

        await using (var conn = new SqlConnection(Options.ConnectionString))
        {
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT name, last_seq_id FROM {Events.ProgressionTableName};";
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = reader.GetString(0);
                var seq = reader.GetInt64(1);
                if (string.Equals(name, "HighWaterMark", StringComparison.OrdinalIgnoreCase))
                {
                    highWater = seq;
                }
                else
                {
                    progress[name] = seq;
                }
            }
        }

        var allShards = Options.Projections.AllShards();
        var statuses = new List<ProjectionStatus>();
        foreach (var projectionName in Options.Projections.AllProjectionNames())
        {
            var lifecycle = "Unknown";
            var shards = allShards
                .Where(s => s.Name.Name.Equals(projectionName, StringComparison.Ordinal))
                .Select(shard =>
                {
                    var shardName = shard.Name.Identity;
                    progress.TryGetValue(shardName, out var processed);
                    return new ShardStatus(shardName, "Stopped", processed, highWater, Error: null!);
                })
                .ToList();

            if (shards.Count == 0)
            {
                shards = new List<ShardStatus>
                {
                    new(projectionName, lifecycle, 0, highWater, Error: null!)
                };
            }

            statuses.Add(new ProjectionStatus(projectionName, lifecycle, shards));
        }

        return statuses;
    }

    private Type? ResolveAggregateType(string aggregateTypeName)
    {
        foreach (var source in Options.Projections.All)
        {
            foreach (var published in source.PublishedTypes())
            {
                if (string.Equals(published.FullName, aggregateTypeName, StringComparison.Ordinal)
                    || string.Equals(published.Name, aggregateTypeName, StringComparison.Ordinal))
                {
                    return published;
                }
            }
        }

        try
        {
            return Events.AggregateTypeFor(aggregateTypeName);
        }
        catch
        {
            return null;
        }
    }
}
