using System.Linq.Expressions;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Events.Protected;

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
    ///     Fetch the aggregate at a specific version, apply events via callback, and save changes in one step.
    /// </summary>
    Task WriteToAggregate<T>(Guid id, int initialVersion, Action<IEventStream<T>> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate at a specific version, apply events via async callback, and save changes in one step.
    /// </summary>
    Task WriteToAggregate<T>(Guid id, int initialVersion, Func<IEventStream<T>, Task> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate at a specific version, apply events via callback, and save changes in one step.
    /// </summary>
    Task WriteToAggregate<T>(string key, int initialVersion, Action<IEventStream<T>> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate at a specific version, apply events via async callback, and save changes in one step.
    /// </summary>
    Task WriteToAggregate<T>(string key, int initialVersion, Func<IEventStream<T>, Task> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate with an exclusive lock, apply events via callback, and save changes in one step.
    /// </summary>
    Task WriteExclusivelyToAggregate<T>(Guid id, Action<IEventStream<T>> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate with an exclusive lock, apply events via callback, and save changes in one step.
    /// </summary>
    Task WriteExclusivelyToAggregate<T>(string key, Action<IEventStream<T>> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate with an exclusive lock, apply events via async callback, and save changes in one step.
    /// </summary>
    Task WriteExclusivelyToAggregate<T>(Guid id, Func<IEventStream<T>, Task> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the aggregate with an exclusive lock, apply events via async callback, and save changes in one step.
    /// </summary>
    Task WriteExclusivelyToAggregate<T>(string key, Func<IEventStream<T>, Task> writing, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Append events to an existing stream with optimistic concurrency. Reads the current
    ///     version and sets ExpectedVersionOnServer. Throws NonExistentStreamException if the
    ///     stream does not exist.
    /// </summary>
    Task AppendOptimistic(Guid streamId, CancellationToken token, params object[] events);

    /// <summary>
    ///     Append events to an existing stream with optimistic concurrency.
    /// </summary>
    Task AppendOptimistic(Guid streamId, params object[] events);

    /// <summary>
    ///     Append events to an existing stream with optimistic concurrency.
    /// </summary>
    Task AppendOptimistic(string streamKey, CancellationToken token, params object[] events);

    /// <summary>
    ///     Append events to an existing stream with optimistic concurrency.
    /// </summary>
    Task AppendOptimistic(string streamKey, params object[] events);

    /// <summary>
    ///     Append events to an existing stream with exclusive row locking. Begins a transaction
    ///     and holds an exclusive lock until SaveChangesAsync. Throws StreamLockedException
    ///     if the lock cannot be acquired.
    /// </summary>
    Task AppendExclusive(Guid streamId, CancellationToken token, params object[] events);

    /// <summary>
    ///     Append events to an existing stream with exclusive row locking.
    /// </summary>
    Task AppendExclusive(Guid streamId, params object[] events);

    /// <summary>
    ///     Append events to an existing stream with exclusive row locking.
    /// </summary>
    Task AppendExclusive(string streamKey, CancellationToken token, params object[] events);

    /// <summary>
    ///     Append events to an existing stream with exclusive row locking.
    /// </summary>
    Task AppendExclusive(string streamKey, params object[] events);

    /// <summary>
    ///     Mark a stream and all its events as archived by Guid id.
    /// </summary>
    void ArchiveStream(Guid streamId);

    /// <summary>
    ///     Mark a stream and all its events as archived by string key.
    /// </summary>
    void ArchiveStream(string streamKey);

    /// <summary>
    ///     Remove the archived flag from a stream and all its events by Guid id.
    /// </summary>
    void UnArchiveStream(Guid streamId);

    /// <summary>
    ///     Remove the archived flag from a stream and all its events by string key.
    /// </summary>
    void UnArchiveStream(string streamKey);

    /// <summary>
    ///     Permanently delete a stream and all its events (hard DELETE) by Guid id.
    /// </summary>
    void TombstoneStream(Guid streamId);

    /// <summary>
    ///     Permanently delete a stream and all its events (hard DELETE) by string key.
    /// </summary>
    void TombstoneStream(string streamKey);

    /// <summary>
    ///     Retroactively assign a tag to all events matching the given LINQ predicate.
    ///     The tag must be of a registered tag type. The operation is queued and applied at SaveChangesAsync time.
    /// </summary>
    /// <param name="expression">LINQ predicate against IEvent properties (e.g. EventTypeName, StreamId, Timestamp)</param>
    /// <param name="tag">Tag value whose type must be registered via RegisterTagType</param>
    void AssignTagWhere(Expression<Func<IEvent, bool>> expression, object tag);

    /// <summary>
    ///     Overwrite the data and optionally headers of an existing event. Used for GDPR
    ///     data masking operations.
    /// </summary>
    void OverwriteEvent(IEvent @event);

    /// <summary>
    ///     Check whether any events exist that match the given tag query, without loading the events.
    ///     This is a lightweight existence check useful for DCB guard clauses.
    /// </summary>
    Task<bool> EventsExistAsync(EventTagQuery query, CancellationToken cancellation = default);

    /// <summary>
    ///     Query events across streams by tag conditions (DCB support).
    /// </summary>
    Task<IReadOnlyList<IEvent>> QueryByTagsAsync(EventTagQuery query, CancellationToken cancellation = default);

    /// <summary>
    ///     Aggregate events matching tag conditions into a live aggregate (DCB support).
    /// </summary>
    Task<T?> AggregateByTagsAsync<T>(EventTagQuery query, CancellationToken cancellation = default) where T : class;

    /// <summary>
    ///     Fetch events by tags and return a writable boundary with DCB consistency checking.
    /// </summary>
    Task<IEventBoundary<T>> FetchForWritingByTags<T>(EventTagQuery query, CancellationToken cancellation = default) where T : class;

    /// <summary>
    ///     Compact a stream by replacing its events with a single Compacted&lt;T&gt; snapshot event.
    ///     Use this when you do not care about older stream data but want to keep database size down.
    /// </summary>
    Task CompactStreamAsync<T>(Guid streamId, Action<StreamCompactingRequest<T>>? configure = null) where T : class;

    /// <summary>
    ///     Compact a stream by replacing its events with a single Compacted&lt;T&gt; snapshot event.
    ///     Use this when you do not care about older stream data but want to keep database size down.
    /// </summary>
    Task CompactStreamAsync<T>(string streamKey, Action<StreamCompactingRequest<T>>? configure = null) where T : class;

    /// <summary>
    ///     Build an IEvent wrapper for raw event data. Useful for setting tags before appending.
    /// </summary>
    IEvent BuildEvent(object data);

    /// <summary>
    ///     Fetch the aggregate state for writing by a natural key or any registered identifier type.
    /// </summary>
    Task<IEventStream<T>> FetchForWriting<T, TId>(TId id, CancellationToken cancellation = default)
        where T : class, new() where TId : notnull;

    /// <summary>
    ///     Fetch the aggregate state by natural key for exclusive writing with row-level locking.
    /// </summary>
    Task<IEventStream<T>> FetchForExclusiveWriting<T, TId>(TId id, CancellationToken cancellation = default)
        where T : class, new() where TId : notnull;

    /// <summary>
    ///     Fetch the projected aggregate T by a natural key or any registered identifier type.
    ///     This is a lightweight, read-only version of FetchForWriting.
    /// </summary>
    ValueTask<T?> FetchLatest<T, TId>(TId id, CancellationToken cancellation = default)
        where T : class, new() where TId : notnull;
}
