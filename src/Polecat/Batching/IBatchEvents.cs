using JasperFx.Events;

namespace Polecat.Batching;

/// <summary>
///     The event-store surface of a batched query — the batched counterparts of
///     <see cref="IQueryEventStore.FetchStreamStateAsync(Guid, CancellationToken)" /> and
///     <see cref="IQueryEventStore.FetchStreamAsync(Guid, long, DateTimeOffset?, long, CancellationToken)" />.
/// </summary>
/// <remarks>
///     #370 (parity with marten#5053). Reached through <see cref="IBatchedQuery.Events" />. Every method
///     returns immediately with an unresolved <see cref="Task{T}" />; the task completes when
///     <see cref="IBatchedQuery.Execute" /> runs the whole batch as a single round trip and walks its
///     result sets in order.
/// </remarks>
public interface IBatchEvents
{
    /// <summary>
    ///     Fetch the high level metadata about the stream identified by <paramref name="streamId" />.
    ///     Yields null if the stream does not exist.
    /// </summary>
    Task<StreamState?> FetchStreamState(Guid streamId);

    /// <summary>
    ///     Fetch the high level metadata about the stream identified by <paramref name="streamKey" />.
    ///     Yields null if the stream does not exist.
    /// </summary>
    Task<StreamState?> FetchStreamState(string streamKey);

    /// <summary>
    ///     Fetch the raw events of the stream identified by <paramref name="streamId" />. Yields an empty
    ///     list if the stream does not exist.
    /// </summary>
    /// <param name="streamId"></param>
    /// <param name="version">If set, fetches events up to and including this version</param>
    /// <param name="timestamp">If set, fetches events captured on or before this timestamp</param>
    /// <param name="fromVersion">If set, fetches events on or from this version</param>
    Task<IReadOnlyList<IEvent>> FetchStream(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0);

    /// <summary>
    ///     Fetch the raw events of the stream identified by <paramref name="streamKey" />. Yields an empty
    ///     list if the stream does not exist.
    /// </summary>
    /// <param name="streamKey"></param>
    /// <param name="version">If set, fetches events up to and including this version</param>
    /// <param name="timestamp">If set, fetches events captured on or before this timestamp</param>
    /// <param name="fromVersion">If set, fetches events on or from this version</param>
    Task<IReadOnlyList<IEvent>> FetchStream(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0);
}
