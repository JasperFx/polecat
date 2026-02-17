using Polecat.Events;
using Polecat.Serialization;

namespace Polecat;

/// <summary>
///     Read-only session for loading documents by id.
/// </summary>
public interface IQuerySession : IAsyncDisposable
{
    /// <summary>
    ///     The tenant id for this session.
    /// </summary>
    string TenantId { get; }

    /// <summary>
    ///     The serializer used by this session.
    /// </summary>
    IPolecatSerializer Serializer { get; }

    /// <summary>
    ///     Read-only access to event store queries.
    /// </summary>
    IQueryEventStore Events { get; }

    /// <summary>
    ///     Load a document by its id. Returns null if not found.
    /// </summary>
    Task<T?> LoadAsync<T>(Guid id, CancellationToken token = default) where T : class;

    /// <summary>
    ///     Load a document by its string id. Returns null if not found.
    /// </summary>
    Task<T?> LoadAsync<T>(string id, CancellationToken token = default) where T : class;

    /// <summary>
    ///     Load multiple documents by their ids.
    /// </summary>
    Task<IReadOnlyList<T>> LoadManyAsync<T>(IEnumerable<Guid> ids, CancellationToken token = default) where T : class;

    /// <summary>
    ///     Load multiple documents by their string ids.
    /// </summary>
    Task<IReadOnlyList<T>> LoadManyAsync<T>(IEnumerable<string> ids, CancellationToken token = default) where T : class;
}
