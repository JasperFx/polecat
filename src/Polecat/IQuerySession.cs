using Polecat.Batching;
using Polecat.Events;
using Polecat.Linq;
using Polecat.Logging;
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
    ISerializer Serializer { get; }

    /// <summary>
    ///     Correlation id to be set on ITracked documents on save.
    /// </summary>
    string? CorrelationId { get; set; }

    /// <summary>
    ///     Causation id to be set on ITracked documents on save.
    /// </summary>
    string? CausationId { get; set; }

    /// <summary>
    ///     Last modified by value to be set on ITracked documents on save.
    /// </summary>
    string? LastModifiedBy { get; set; }

    /// <summary>
    ///     The number of database requests executed by this session.
    /// </summary>
    int RequestCount { get; }

    /// <summary>
    ///     The session-level logger for SQL command diagnostics. Can be replaced per-session.
    /// </summary>
    IPolecatSessionLogger Logger { get; set; }

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

    /// <summary>
    ///     Load a document by its int id. Returns null if not found.
    /// </summary>
    Task<T?> LoadAsync<T>(int id, CancellationToken token = default) where T : class;

    /// <summary>
    ///     Load a document by its long id. Returns null if not found.
    /// </summary>
    Task<T?> LoadAsync<T>(long id, CancellationToken token = default) where T : class;

    /// <summary>
    ///     Start a LINQ query against documents of type T.
    /// </summary>
    IPolecatQueryable<T> Query<T>() where T : class;

    /// <summary>
    ///     Create a batch query to execute multiple Load/Query operations in a single roundtrip.
    /// </summary>
    IBatchedQuery CreateBatchQuery();

    /// <summary>
    ///     Preview the SQL that would be generated for a LINQ query without executing it.
    /// </summary>
    string ToSql<T>(IQueryable<T> queryable) where T : class;

    /// <summary>
    ///     Load the raw JSON for a document by its Guid id, without deserializing.
    /// </summary>
    Task<string?> LoadJsonAsync<T>(Guid id, CancellationToken token = default) where T : class;

    /// <summary>
    ///     Load the raw JSON for a document by its string id, without deserializing.
    /// </summary>
    Task<string?> LoadJsonAsync<T>(string id, CancellationToken token = default) where T : class;

    /// <summary>
    ///     Load the raw JSON for a document by its int id, without deserializing.
    /// </summary>
    Task<string?> LoadJsonAsync<T>(int id, CancellationToken token = default) where T : class;

    /// <summary>
    ///     Load the raw JSON for a document by its long id, without deserializing.
    /// </summary>
    Task<string?> LoadJsonAsync<T>(long id, CancellationToken token = default) where T : class;
}
