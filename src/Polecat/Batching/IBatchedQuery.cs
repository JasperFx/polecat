using JasperFx.Events.Tags;
using Polecat.Events.Dcb;

namespace Polecat.Batching;

/// <summary>
///     Batches multiple Load/Query operations into a single database roundtrip
///     using SQL Server's multiple result sets.
/// </summary>
public interface IBatchedQuery
{
    /// <summary>
    ///     The parent query session that created this batch.
    /// </summary>
    IQuerySession Parent { get; }

    /// <summary>
    ///     Check if a document of type T with the given Guid id exists in the database
    ///     without loading or deserializing the document.
    /// </summary>
    Task<bool> CheckExists<T>(Guid id) where T : class;

    /// <summary>
    ///     Check if a document of type T with the given string id exists in the database
    ///     without loading or deserializing the document.
    /// </summary>
    Task<bool> CheckExists<T>(string id) where T : class;

    /// <summary>
    ///     Check if a document of type T with the given int id exists in the database
    ///     without loading or deserializing the document.
    /// </summary>
    Task<bool> CheckExists<T>(int id) where T : class;

    /// <summary>
    ///     Check if a document of type T with the given long id exists in the database
    ///     without loading or deserializing the document.
    /// </summary>
    Task<bool> CheckExists<T>(long id) where T : class;

    Task<T?> Load<T>(Guid id) where T : class;
    Task<T?> Load<T>(string id) where T : class;
    Task<T?> Load<T>(int id) where T : class;
    Task<T?> Load<T>(long id) where T : class;

    Task<IReadOnlyList<T>> LoadMany<T>(params Guid[] ids) where T : class;
    Task<IReadOnlyList<T>> LoadMany<T>(params string[] ids) where T : class;

    IBatchedQueryable<T> Query<T>() where T : class;

    /// <summary>
    ///     Execute a batch query plan (specification pattern).
    /// </summary>
    Task<T> QueryByPlan<T>(IBatchQueryPlan<T> plan);

    /// <summary>
    ///     Check whether any events exist that match the given tag query, without loading the events.
    ///     This is a lightweight existence check useful for DCB guard clauses.
    /// </summary>
    Task<bool> EventsExist(EventTagQuery query);

    /// <summary>
    ///     Fetch events matching a tag query and aggregate them into type T with a DCB consistency boundary.
    ///     At SaveChangesAsync time, will throw DcbConcurrencyException if new matching events were appended.
    /// </summary>
    Task<IEventBoundary<T>> FetchForWritingByTags<T>(EventTagQuery query) where T : class;

    Task Execute(CancellationToken token = default);
}
