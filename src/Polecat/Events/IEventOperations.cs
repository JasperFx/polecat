using JasperFx.Events;

namespace Polecat.Events;

/// <summary>
///     Write-side event operations. Extends IQueryEventStore with append/start capabilities.
///     Operations are queued and flushed on SaveChangesAsync.
/// </summary>
public interface IEventOperations : IQueryEventStore
{
    /// <summary>
    ///     Append events to an existing stream (or create it) by Guid id.
    /// </summary>
    StreamAction Append(Guid stream, params object[] events);

    /// <summary>
    ///     Append events to an existing stream (or create it) by string key.
    /// </summary>
    StreamAction Append(string stream, params object[] events);

    /// <summary>
    ///     Append events with an expected version for optimistic concurrency.
    /// </summary>
    StreamAction Append(Guid stream, long expectedVersion, params object[] events);

    /// <summary>
    ///     Append events with an expected version for optimistic concurrency.
    /// </summary>
    StreamAction Append(string stream, long expectedVersion, params object[] events);

    /// <summary>
    ///     Start a new stream with a Guid id. Throws if the stream already exists.
    /// </summary>
    StreamAction StartStream(Guid id, params object[] events);

    /// <summary>
    ///     Start a new stream with a string key. Throws if the stream already exists.
    /// </summary>
    StreamAction StartStream(string streamKey, params object[] events);

    /// <summary>
    ///     Start a new stream with a Guid id and aggregate type. Throws if the stream already exists.
    /// </summary>
    StreamAction StartStream<TAggregate>(Guid id, params object[] events) where TAggregate : class;

    /// <summary>
    ///     Start a new stream with a string key and aggregate type. Throws if the stream already exists.
    /// </summary>
    StreamAction StartStream<TAggregate>(string streamKey, params object[] events) where TAggregate : class;

    /// <summary>
    ///     Start a new stream with an auto-generated Guid id. Returns the StreamAction with the assigned id.
    /// </summary>
    StreamAction StartStream(params object[] events);

    /// <summary>
    ///     Start a new stream with an auto-generated Guid id and aggregate type.
    /// </summary>
    StreamAction StartStream<TAggregate>(params object[] events) where TAggregate : class;

    /// <summary>
    ///     Fetch the aggregate state and return a writable handle for optimistic concurrency.
    /// </summary>
    Task<IEventStream<T>> FetchForWriting<T>(Guid id, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate state and return a writable handle for optimistic concurrency.
    /// </summary>
    Task<IEventStream<T>> FetchForWriting<T>(string key, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate state with an expected version. Throws if the version does not match.
    /// </summary>
    Task<IEventStream<T>> FetchForWriting<T>(Guid id, long expectedVersion, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate state with an expected version. Throws if the version does not match.
    /// </summary>
    Task<IEventStream<T>> FetchForWriting<T>(string key, long expectedVersion, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate state with a pessimistic lock (UPDLOCK, HOLDLOCK) for exclusive writing.
    /// </summary>
    Task<IEventStream<T>> FetchForExclusiveWriting<T>(Guid id, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate state with a pessimistic lock (UPDLOCK, HOLDLOCK) for exclusive writing.
    /// </summary>
    Task<IEventStream<T>> FetchForExclusiveWriting<T>(string key, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate, apply events via callback, and save changes in one step.
    /// </summary>
    Task WriteToAggregate<T>(Guid id, Action<IEventStream<T>> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate, apply events via callback, and save changes in one step.
    /// </summary>
    Task WriteToAggregate<T>(string key, Action<IEventStream<T>> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate, apply events via async callback, and save changes in one step.
    /// </summary>
    Task WriteToAggregate<T>(Guid id, Func<IEventStream<T>, Task> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate, apply events via async callback, and save changes in one step.
    /// </summary>
    Task WriteToAggregate<T>(string key, Func<IEventStream<T>, Task> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Mark a stream and all its events as archived by Guid id.
    /// </summary>
    void ArchiveStream(Guid streamId);

    /// <summary>
    ///     Mark a stream and all its events as archived by string key.
    /// </summary>
    void ArchiveStream(string streamKey);
}
