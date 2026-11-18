using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Metadata;
using Polecat.Storage;

namespace Polecat.Internal;

/// <summary>
///     What a fetch learned about a stream it read as missing, from its Async snapshot document.
/// </summary>
internal enum MissingStreamCheck
{
    /// <summary>No snapshot and no stream: a new stream.</summary>
    NewStream,

    /// <summary>A snapshot whose stream does not exist, as <c>DeleteAllEventDataAsync</c> leaves it.</summary>
    SnapshotWithoutStream,

    /// <summary>The stream was started after the fetch read it as missing. The fetch is redone.</summary>
    StartedSince
}

internal partial class QuerySession
{
    /// <summary>
    ///     Test seam: runs before every command or batch this session sends, with its SQL. The fetch race
    ///     tests (polecat#1049) use it to change the database between a fetch's reads. Null otherwise.
    /// </summary>
    internal Func<string, CancellationToken, Task>? BeforeExecuteAsync { get; set; }

    /// <summary>
    ///     The storage of <typeparamref name="T" />'s Async snapshot, when <typeparamref name="T" /> is an
    ///     aggregate projected with <c>ProjectionLifecycle.Async</c> into a Polecat document that a
    ///     <paramref name="streamIdType" /> stream id can address. Null for every other aggregate.
    /// </summary>
    /// <remarks>
    ///     <c>InnerIdType</c> rather than <c>IdType</c>, so an aggregate with a strong-typed id is matched on
    ///     the value it wraps, which is what its table's id column holds. A type with custom projection
    ///     storage (an EF Core projection) has no Polecat document to look at.
    /// </remarks>
    internal DocumentProvider? AsyncSnapshotProviderFor<T>(Type streamIdType) where T : class
    {
        if (Options.CustomProjectionStorageProviders.ContainsKey(typeof(T))) return null;
        if (!Options.Projections.TryFindAggregate(typeof(T), out var projection)
            || projection.Lifecycle != ProjectionLifecycle.Async)
        {
            return null;
        }

        var provider = _providers.GetProvider<T>();
        return provider.Mapping.InnerIdType == streamIdType ? provider : null;
    }

    /// <summary>
    ///     polecat#1049 (marten#5799): for streams a fetch read as missing, whether the Async snapshot
    ///     document exists without its stream, or the stream has been started since, in one round trip.
    /// </summary>
    /// <remarks>
    ///     Both questions are answered by one statement, so a stream that is started and projected after
    ///     the fetch's first read is seen as started, never as a snapshot without its stream.
    /// </remarks>
    internal async Task<MissingStreamCheck[]> CheckMissingStreamsAsync(DocumentProvider snapshots,
        IReadOnlyList<object> streamIds, string tenantId, CancellationToken token)
    {
        var results = new MissingStreamCheck[streamIds.Count];
        if (streamIds.Count == 0) return results;

        await _tableEnsurer.EnsureTableAsync(snapshots, HasOpenTransaction, token).ConfigureAwait(false);

        var mapping = snapshots.Mapping;
        await using var cmd = new SqlCommand();

        var rows = new List<string>(streamIds.Count);
        for (var i = 0; i < streamIds.Count; i++)
        {
            cmd.Parameters.AddIdParameter($"@id{i}", streamIds[i], _eventGraph.UnicodeKeys);
            rows.Add($"({i}, @id{i})");
        }

        cmd.Parameters.AddKey("@stream_tenant", tenantId, _eventGraph.UnicodeKeys);

        var snapshotFilter = "";
        if (mapping.TenancyStyle == TenancyStyle.Conjoined)
        {
            snapshotFilter += " AND d.tenant_id = @doc_tenant";
            cmd.Parameters.AddKey("@doc_tenant", tenantId, mapping.UnicodeKeys);
        }

        if (mapping.DeleteStyle == DeleteStyle.SoftDelete) snapshotFilter += " AND d.is_deleted = 0";

        cmd.CommandText = $"""
            SELECT i.n,
                CASE WHEN EXISTS (SELECT 1 FROM {mapping.QualifiedTableName} AS d WHERE d.id = i.id{snapshotFilter}) THEN 1 ELSE 0 END,
                CASE WHEN EXISTS (SELECT 1 FROM {_eventGraph.StreamsTableName} AS s WHERE s.id = i.id AND s.tenant_id = @stream_tenant) THEN 1 ELSE 0 END
            FROM (VALUES {string.Join(", ", rows)}) AS i(n, id);
            """;

        await using var reader = await ExecuteReaderAsync(cmd, token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            var index = reader.GetInt32(0);
            var snapshotExists = reader.GetInt32(1) == 1;
            var streamExists = reader.GetInt32(2) == 1;

            results[index] = streamExists
                ? MissingStreamCheck.StartedSince
                : snapshotExists
                    ? MissingStreamCheck.SnapshotWithoutStream
                    : MissingStreamCheck.NewStream;
        }

        return results;
    }
}
