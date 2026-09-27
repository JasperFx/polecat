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
    ///     The batched event store fetches — <c>FetchStreamState</c> and <c>FetchStream</c> — so a raw
    ///     stream read can share the batch's single round trip with document loads and LINQ queries.
    ///     See <see cref="IBatchEvents" /> (#370).
    /// </summary>
    IBatchEvents Events { get; }

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
    ///     Enlist a raw SQL query in this batch, so it shares the batch's single round trip with the
    ///     loads, LINQ queries and event fetches around it. Parameters are substituted for the first
    ///     occurrences of <c>?</c>, in order. <typeparamref name="T" /> may be a scalar, a
    ///     JSON-deserializable class, or a registered document type — the same rules
    ///     <see cref="IAdvancedSql.QueryAsync{T}(string, CancellationToken, object[])" /> applies, and
    ///     the same reader materializes the rows.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>Raw SQL is NOT tenant-scoped</b>, unlike every other member of this interface. The
    ///         caller wrote the SQL, so the caller owns its <c>tenant_id</c> filter; nothing is appended
    ///         to it. Nor is any table ensured to exist first, for the same reason — matching
    ///         <see cref="IAdvancedSql" /> rather than the document members.
    ///     </para>
    ///     <para>
    ///         #676. The gap this closes had a concrete caller: Wolverine's Polecat deduplication paid
    ///         a round trip of its own for an existence check that could have gone out with the
    ///         <c>FetchForWriting</c> the handler was already doing — one extra round trip per
    ///         deduplicated message, on exactly the high-traffic endpoints.
    ///     </para>
    /// </remarks>
    Task<IReadOnlyList<T>> Query<T>(string sql, params object[] parameters);

    /// <summary>
    ///     <see cref="Query{T}(string, object[])" /> with an explicit placeholder character, for SQL
    ///     that contains a literal <c>?</c> of its own — a JSON path, say. Only the first
    ///     <c>parameters.Length</c> occurrences of <paramref name="placeholder" /> are substituted;
    ///     anything after that is left in the SQL untouched.
    /// </summary>
    Task<IReadOnlyList<T>> Query<T>(char placeholder, string sql, params object[] parameters);

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
