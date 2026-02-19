using System.Text;

namespace Polecat.Linq;

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
}
