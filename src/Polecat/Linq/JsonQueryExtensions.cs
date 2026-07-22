using System.Text;

namespace Polecat.Linq;

/// <summary>
///     The raw persisted JSON of the first matching document plus its <c>version</c>, read in a
///     single round trip. Used for HTTP conditional-request (ETag) support.
/// </summary>
public sealed record DocumentJsonWithVersion(string Json, long Version);

/// <summary>
///     Extension methods for streaming raw JSON from LINQ queries.
/// </summary>
public static class JsonQueryExtensions
{
    /// <summary>
    ///     Execute the query and return the results as a raw JSON array string
    ///     without deserializing to .NET objects.
    /// </summary>
    public static async Task<string> ToJsonArrayAsync<T>(
        this IQueryable<T> queryable, CancellationToken token = default)
    {
        if (queryable.Provider is not PolecatLinqQueryProvider provider)
        {
            throw new InvalidOperationException(
                "ToJsonArrayAsync can only be used with Polecat IQueryable instances.");
        }

        return await provider.ExecuteJsonArrayAsync(queryable.Expression, token);
    }

    /// <summary>
    ///     Executes the query and returns the first matching document's raw JSON plus its
    ///     <c>version</c> in a single database round trip, or <c>null</c> if no document matches.
    ///     Intended for HTTP conditional-request (ETag / If-None-Match → 304) support.
    /// </summary>
    public static async Task<DocumentJsonWithVersion?> ToJsonFirstWithVersionAsync<T>(
        this IQueryable<T> queryable, CancellationToken token = default)
    {
        if (queryable.Provider is not PolecatLinqQueryProvider provider)
        {
            throw new InvalidOperationException(
                "ToJsonFirstWithVersionAsync can only be used with Polecat IQueryable instances.");
        }

        var result = await provider.ExecuteJsonFirstWithVersionAsync(queryable.Expression, token);
        return result is { } r ? new DocumentJsonWithVersion(r.Json, r.Version) : null;
    }
}
