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
}
