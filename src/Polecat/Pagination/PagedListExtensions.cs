using Polecat.Linq;

namespace Polecat.Pagination;

/// <summary>
///     Extension methods for IQueryable to support pagination.
/// </summary>
public static class PagedListExtensions
{
    /// <summary>
    ///     Executes the query with pagination and returns a paged list with metadata.
    /// </summary>
    /// <param name="queryable">The queryable to paginate.</param>
    /// <param name="pageNumber">The 1-based page number.</param>
    /// <param name="pageSize">The number of items per page.</param>
    /// <param name="token">Cancellation token.</param>
    public static async Task<IPagedList<T>> ToPagedListAsync<T>(
        this IQueryable<T> queryable, int pageNumber, int pageSize,
        CancellationToken token = default)
    {
        if (pageNumber < 1)
            throw new ArgumentOutOfRangeException(nameof(pageNumber), "Page number must be >= 1.");
        if (pageSize < 1)
            throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be >= 1.");

        var totalCount = await queryable.CountAsync(token);
        var items = await queryable
            .Skip((pageNumber - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync(token);

        return new PagedList<T>(items, totalCount, pageNumber, pageSize);
    }
}
