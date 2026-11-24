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

    // ---- #1131 / #1050 (marten#6150): aggregates, by stream id and by natural key ----
    // Default interface members, so that an implementation compiled against Polecat 5 still loads. Polecat's own
    // batch implements every one of them.

    /// <summary>
    ///     The batched form of <c>FetchLatest&lt;T&gt;(Guid)</c>: the aggregate's latest state, or null when the
    ///     stream does not exist.
    /// </summary>
    Task<T?> FetchLatest<T>(Guid id) where T : class => throw notSupported();

    /// <summary>The batched form of <c>FetchLatest&lt;T&gt;(string)</c>.</summary>
    Task<T?> FetchLatest<T>(string key) where T : class => throw notSupported();

    /// <summary>
    ///     The batched form of <c>FetchLatest&lt;T, TId&gt;</c>: by a natural key, owned or mapped
    ///     (<c>[MappedNaturalKey&lt;TSource&gt;]</c>), by a strong-typed stream id, or by the stream id. A null key
    ///     finds nothing, without a query.
    /// </summary>
    Task<T?> FetchLatest<T, TId>(TId id) where T : class where TId : notnull => throw notSupported();

    /// <summary>
    ///     The batched form of <c>FetchForWriting&lt;T&gt;(Guid)</c>. Needs a document session.
    /// </summary>
    Task<IEventStream<T>> FetchForWriting<T>(Guid id) where T : class => throw notSupported();

    /// <summary>The batched form of <c>FetchForWriting&lt;T&gt;(string)</c>. Needs a document session.</summary>
    Task<IEventStream<T>> FetchForWriting<T>(string key) where T : class => throw notSupported();

    /// <summary>
    ///     The batched form of <c>FetchForWriting&lt;T&gt;(Guid, long)</c>: fails with a concurrency exception when
    ///     the stream is not at <paramref name="expectedVersion" />. Needs a document session.
    /// </summary>
    Task<IEventStream<T>> FetchForWriting<T>(Guid id, long expectedVersion) where T : class
        => throw notSupported();

    /// <summary>The batched form of <c>FetchForWriting&lt;T&gt;(string, long)</c>. Needs a document session.</summary>
    Task<IEventStream<T>> FetchForWriting<T>(string key, long expectedVersion) where T : class
        => throw notSupported();

    /// <summary>
    ///     The batched form of <c>FetchForExclusiveWriting&lt;T&gt;(Guid)</c>. The session's transaction is opened
    ///     before the batch runs, and the stream stays locked until it ends. Needs a document session.
    /// </summary>
    Task<IEventStream<T>> FetchForExclusiveWriting<T>(Guid id) where T : class => throw notSupported();

    /// <summary>The batched form of <c>FetchForExclusiveWriting&lt;T&gt;(string)</c>. Needs a document session.</summary>
    Task<IEventStream<T>> FetchForExclusiveWriting<T>(string key) where T : class => throw notSupported();

    /// <summary>
    ///     The batched form of <c>FetchForWriting&lt;T, TId&gt;</c>: by a natural key, owned or mapped, by a
    ///     strong-typed stream id, or by the stream id. A key no stream has gets a handle on a new stream. Needs a
    ///     document session.
    /// </summary>
    Task<IEventStream<T>> FetchForWriting<T, TId>(TId id) where T : class where TId : notnull
        => throw notSupported();

    /// <summary>
    ///     The batched form of <c>FetchForWriting&lt;T, TId&gt;(TId, long)</c>. A stale version fails before the
    ///     session is touched. Needs a document session.
    /// </summary>
    Task<IEventStream<T>> FetchForWriting<T, TId>(TId id, long expectedVersion) where T : class where TId : notnull
        => throw notSupported();

    /// <summary>
    ///     The batched form of <c>FetchForExclusiveWriting&lt;T, TId&gt;</c>. Needs a document session.
    /// </summary>
    Task<IEventStream<T>> FetchForExclusiveWriting<T, TId>(TId id) where T : class where TId : notnull
        => throw notSupported();

    private static NotSupportedException notSupported()
        => new("This IBatchEvents implementation does not fetch aggregates. Polecat's own batched query does.");
}
