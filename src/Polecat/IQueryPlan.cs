using JasperFx.Events;
using Polecat.Batching;
using Polecat.Internal.Batching;
using Polecat.Linq;

namespace Polecat;

/// <summary>
///     Polecat's concept of the "Specification" pattern for reusable queries.
///     Encapsulates a query that can be executed against an IQuerySession.
/// </summary>
/// <typeparam name="T">The result type of the query</typeparam>
public interface IQueryPlan<T>
{
    Task<T> Fetch(IQuerySession session, CancellationToken token);
}

/// <summary>
///     Polecat's concept of the "Specification" pattern for reusable queries
///     within batched queries. Encapsulates a query that can be executed as part
///     of an IBatchedQuery.
/// </summary>
/// <typeparam name="T">The result type of the query</typeparam>
public interface IBatchQueryPlan<T>
{
    Task<T> Fetch(IBatchedQuery query);
}

/// <summary>
///     Base class for query plans that return a list of items. Implements both
///     IQueryPlan and IBatchQueryPlan so it can be used with QueryByPlanAsync()
///     and batch.QueryByPlan().
/// </summary>
/// <typeparam name="T">The document type to query</typeparam>
public abstract class QueryListPlan<T> : IQueryPlan<IReadOnlyList<T>>, IBatchQueryPlan<IReadOnlyList<T>>
    where T : class
{
    /// <summary>
    ///     Define the query by returning an IQueryable from the session.
    /// </summary>
    public abstract IQueryable<T> Query(IQuerySession session);

    async Task<IReadOnlyList<T>> IQueryPlan<IReadOnlyList<T>>.Fetch(IQuerySession session, CancellationToken token)
    {
        return await Query(session).ToListAsync(token);
    }

    Task<IReadOnlyList<T>> IBatchQueryPlan<IReadOnlyList<T>>.Fetch(IBatchedQuery query)
    {
        if (query is BatchedQuery batch)
        {
            return batch.AddQueryableList(Query(query.Parent));
        }

        // Fallback for non-Polecat batch implementations
        return Query(query.Parent).ToListAsync();
    }
}

/// <summary>
///     Query plan for the high level metadata of a single event stream, identified by either a Guid
///     stream id or a string stream key. Yields null if the stream does not exist.
/// </summary>
/// <remarks>
///     #370 (parity with marten#5053). Implements <b>both</b> <see cref="IQueryPlan{T}" /> and
///     <see cref="IBatchQueryPlan{T}" />, so the same plan instance works with
///     <c>session.QueryByPlanAsync()</c> and <c>batch.QueryByPlan()</c>. Implementing only the batched
///     half matters beyond convenience: through Wolverine's fetch-specification feature, a plan that is
///     only an <see cref="IBatchQueryPlan{T}" /> produces uncompilable generated code.
/// </remarks>
public class FetchStreamStatePlan : IQueryPlan<StreamState?>, IBatchQueryPlan<StreamState?>
{
    private readonly Guid _streamId;
    private readonly string? _streamKey;

    /// <summary>
    ///     Fetch the stream state for the stream identified by <paramref name="streamId" />.
    /// </summary>
    public FetchStreamStatePlan(Guid streamId)
    {
        _streamId = streamId;
    }

    /// <summary>
    ///     Fetch the stream state for the stream identified by <paramref name="streamKey" />.
    /// </summary>
    public FetchStreamStatePlan(string streamKey)
    {
        _streamKey = streamKey ?? throw new ArgumentNullException(nameof(streamKey));
    }

    public Task<StreamState?> Fetch(IQuerySession session, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(session);

        return _streamKey is not null
            ? session.Events.FetchStreamStateAsync(_streamKey, token)
            : session.Events.FetchStreamStateAsync(_streamId, token);
    }

    public Task<StreamState?> Fetch(IBatchedQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);

        return _streamKey is not null
            ? query.Events.FetchStreamState(_streamKey)
            : query.Events.FetchStreamState(_streamId);
    }
}

/// <summary>
///     Query plan for the raw events of a single event stream, identified by either a Guid stream id or
///     a string stream key, carrying <c>FetchStream</c>'s optional <c>version</c> / <c>timestamp</c> /
///     <c>fromVersion</c> filters. Yields an empty list if the stream does not exist.
/// </summary>
/// <remarks>
///     #370 (parity with marten#5053). Implements both <see cref="IQueryPlan{T}" /> and
///     <see cref="IBatchQueryPlan{T}" /> — see <see cref="FetchStreamStatePlan" /> for why the pair
///     matters.
/// </remarks>
public class FetchStreamPlan : IQueryPlan<IReadOnlyList<IEvent>>, IBatchQueryPlan<IReadOnlyList<IEvent>>
{
    private readonly Guid _streamId;
    private readonly string? _streamKey;
    private readonly long _version;
    private readonly DateTimeOffset? _timestamp;
    private readonly long _fromVersion;

    /// <summary>
    ///     Fetch the events for the stream identified by <paramref name="streamId" />.
    /// </summary>
    /// <param name="streamId"></param>
    /// <param name="version">If set, queries for events up to and including this version</param>
    /// <param name="timestamp">If set, queries for events captured on or before this timestamp</param>
    /// <param name="fromVersion">If set, queries for events on or from this version</param>
    public FetchStreamPlan(Guid streamId, long version = 0, DateTimeOffset? timestamp = null,
        long fromVersion = 0)
    {
        _streamId = streamId;
        _version = version;
        _timestamp = timestamp;
        _fromVersion = fromVersion;
    }

    /// <summary>
    ///     Fetch the events for the stream identified by <paramref name="streamKey" />.
    /// </summary>
    /// <param name="streamKey"></param>
    /// <param name="version">If set, queries for events up to and including this version</param>
    /// <param name="timestamp">If set, queries for events captured on or before this timestamp</param>
    /// <param name="fromVersion">If set, queries for events on or from this version</param>
    public FetchStreamPlan(string streamKey, long version = 0, DateTimeOffset? timestamp = null,
        long fromVersion = 0)
    {
        _streamKey = streamKey ?? throw new ArgumentNullException(nameof(streamKey));
        _version = version;
        _timestamp = timestamp;
        _fromVersion = fromVersion;
    }

    public Task<IReadOnlyList<IEvent>> Fetch(IQuerySession session, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(session);

        return _streamKey is not null
            ? session.Events.FetchStreamAsync(_streamKey, _version, _timestamp, _fromVersion, token)
            : session.Events.FetchStreamAsync(_streamId, _version, _timestamp, _fromVersion, token);
    }

    public Task<IReadOnlyList<IEvent>> Fetch(IBatchedQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);

        return _streamKey is not null
            ? query.Events.FetchStream(_streamKey, _version, _timestamp, _fromVersion)
            : query.Events.FetchStream(_streamId, _version, _timestamp, _fromVersion);
    }
}
