using System.Runtime.CompilerServices;

namespace Polecat;

/// <summary>
///     Provides raw SQL query capabilities against the Polecat document store.
///     The type parameter can be a scalar, a JSON-serializable class, or a document class.
///     For document types, the SQL must select the required columns in the correct order
///     (at minimum: id, data).
/// </summary>
public interface IAdvancedSql
{
    /// <summary>
    ///     Execute a raw SQL query and map results to type T.
    ///     T can be a scalar (string, int, etc.), a JSON-deserializable object, or a document.
    ///     For documents, SELECT must include at least id, data columns in order.
    ///     Parameters use @p0, @p1, etc. as placeholders (or specify a custom placeholder character).
    /// </summary>
    Task<IReadOnlyList<T>> QueryAsync<T>(string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Execute a raw SQL query and map results to type T, using a custom placeholder character
    ///     that will be replaced with @p0, @p1, etc.
    /// </summary>
    Task<IReadOnlyList<T>> QueryAsync<T>(char placeholder, string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Execute a raw SQL query returning two result columns mapped to a tuple.
    ///     Each type reads from sequential columns. Scalars consume 1 column each,
    ///     JSON objects consume 1 column each, documents consume multiple columns (id, data, ...).
    /// </summary>
    Task<IReadOnlyList<(T1, T2)>> QueryAsync<T1, T2>(string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Execute a raw SQL query returning two result columns mapped to a tuple,
    ///     using a custom placeholder character.
    /// </summary>
    Task<IReadOnlyList<(T1, T2)>> QueryAsync<T1, T2>(char placeholder, string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Execute a raw SQL query returning three result columns mapped to a tuple.
    /// </summary>
    Task<IReadOnlyList<(T1, T2, T3)>> QueryAsync<T1, T2, T3>(string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Execute a raw SQL query returning three result columns mapped to a tuple,
    ///     using a custom placeholder character.
    /// </summary>
    Task<IReadOnlyList<(T1, T2, T3)>> QueryAsync<T1, T2, T3>(char placeholder, string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Stream raw SQL query results as an async enumerable of type T.
    /// </summary>
    IAsyncEnumerable<T> StreamAsync<T>(string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Stream raw SQL query results as an async enumerable of type T,
    ///     using a custom placeholder character.
    /// </summary>
    IAsyncEnumerable<T> StreamAsync<T>(char placeholder, string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Stream raw SQL query results as an async enumerable of tuples.
    /// </summary>
    IAsyncEnumerable<(T1, T2)> StreamAsync<T1, T2>(string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Stream raw SQL query results as an async enumerable of tuples,
    ///     using a custom placeholder character.
    /// </summary>
    IAsyncEnumerable<(T1, T2)> StreamAsync<T1, T2>(char placeholder, string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Stream raw SQL query results as an async enumerable of 3-tuples.
    /// </summary>
    IAsyncEnumerable<(T1, T2, T3)> StreamAsync<T1, T2, T3>(string sql, CancellationToken token, params object[] parameters);

    /// <summary>
    ///     Stream raw SQL query results as an async enumerable of 3-tuples,
    ///     using a custom placeholder character.
    /// </summary>
    IAsyncEnumerable<(T1, T2, T3)> StreamAsync<T1, T2, T3>(char placeholder, string sql, CancellationToken token, params object[] parameters);
}
