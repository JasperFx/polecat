using System.Collections;

namespace Polecat.Pagination;

/// <summary>
///     Default implementation of IPagedList that wraps a page of items with pagination metadata.
/// </summary>
public class PagedList<T> : IPagedList<T>
{
    private readonly IReadOnlyList<T> _items;

    public PagedList(IReadOnlyList<T> items, long totalItemCount, int pageNumber, int pageSize)
    {
        _items = items;
        TotalItemCount = totalItemCount;
        PageNumber = pageNumber;
        PageSize = pageSize;
        PageCount = totalItemCount > 0 ? (int)Math.Ceiling((double)totalItemCount / pageSize) : 0;
    }

    public long TotalItemCount { get; }
    public int PageCount { get; }
    public int PageNumber { get; }
    public int PageSize { get; }

    public bool HasPreviousPage => PageNumber > 1;
    public bool HasNextPage => PageNumber < PageCount;
    public bool IsFirstPage => PageNumber == 1;
    public bool IsLastPage => PageNumber >= PageCount;

    public int FirstItemOnPage => TotalItemCount > 0 ? (PageNumber - 1) * PageSize + 1 : 0;

    public int LastItemOnPage => TotalItemCount > 0
        ? (int)Math.Min((long)PageNumber * PageSize, TotalItemCount)
        : 0;

    public int Count => _items.Count;
    public T this[int index] => _items[index];

    public IEnumerator<T> GetEnumerator() => _items.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
