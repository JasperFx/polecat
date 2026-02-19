namespace Polecat.Pagination;

/// <summary>
///     Represents a paged subset of a query result with pagination metadata.
///     Mirrors Marten's IPagedList interface.
/// </summary>
public interface IPagedList<out T> : IReadOnlyList<T>
{
    /// <summary>
    ///     Total number of items across all pages.
    /// </summary>
    long TotalItemCount { get; }

    /// <summary>
    ///     Total number of pages.
    /// </summary>
    int PageCount { get; }

    /// <summary>
    ///     The current page number (1-based).
    /// </summary>
    int PageNumber { get; }

    /// <summary>
    ///     The maximum number of items per page.
    /// </summary>
    int PageSize { get; }

    /// <summary>
    ///     Whether there is a previous page.
    /// </summary>
    bool HasPreviousPage { get; }

    /// <summary>
    ///     Whether there is a next page.
    /// </summary>
    bool HasNextPage { get; }

    /// <summary>
    ///     Whether this is the first page.
    /// </summary>
    bool IsFirstPage { get; }

    /// <summary>
    ///     Whether this is the last page.
    /// </summary>
    bool IsLastPage { get; }

    /// <summary>
    ///     1-based index of the first item on this page within the full result set.
    /// </summary>
    int FirstItemOnPage { get; }

    /// <summary>
    ///     1-based index of the last item on this page within the full result set.
    /// </summary>
    int LastItemOnPage { get; }
}
