using JasperFx.Events;

namespace Polecat.Events;

/// <summary>
///     Read-only event store queries. Available from both query sessions and document sessions.
/// </summary>
public interface IQueryEventStore
{
    /// <summary>
    ///     Fetch all events for a stream by Guid id.
    /// </summary>
    Task<IReadOnlyList<IEvent>> FetchStreamAsync(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0, CancellationToken token = default);

    /// <summary>
    ///     Fetch all events for a stream by string key.
    /// </summary>
    Task<IReadOnlyList<IEvent>> FetchStreamAsync(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0, CancellationToken token = default);

    /// <summary>
    ///     Fetch stream metadata by Guid id.
    /// </summary>
    Task<StreamState?> FetchStreamStateAsync(Guid streamId, CancellationToken token = default);

    /// <summary>
    ///     Fetch stream metadata by string key.
    /// </summary>
    Task<StreamState?> FetchStreamStateAsync(string streamKey, CancellationToken token = default);

    /// <summary>
    ///     Aggregate events from a stream into a transient document of type T (not persisted).
    /// </summary>
    Task<T?> AggregateStreamAsync<T>(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, T? state = null, long fromVersion = 0,
        CancellationToken token = default) where T : class, new();

    /// <summary>
    ///     Aggregate events from a stream into a transient document of type T (not persisted).
    /// </summary>
    Task<T?> AggregateStreamAsync<T>(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, T? state = null, long fromVersion = 0,
        CancellationToken token = default) where T : class, new();

    /// <summary>
    ///     Fetch the latest aggregate state for a stream by Guid id.
    ///     Convenience wrapper around AggregateStreamAsync.
    /// </summary>
    ValueTask<T?> FetchLatest<T>(Guid id, CancellationToken cancellation = default) where T : class, new();

    /// <summary>
    ///     Fetch the latest aggregate state for a stream by string key.
    ///     Convenience wrapper around AggregateStreamAsync.
    /// </summary>
    ValueTask<T?> FetchLatest<T>(string key, CancellationToken cancellation = default) where T : class, new();
}
