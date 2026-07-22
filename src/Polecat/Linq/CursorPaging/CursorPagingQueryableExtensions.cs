namespace Polecat.Linq.CursorPaging;

/// <summary>
///     Keyset (cursor / seek) pagination entry points on Polecat <see cref="IQueryable{T}"/>.
///     Constant cost at any depth — ideal for infinite scroll, "load more", and export feeds.
/// </summary>
public static class CursorPagingQueryableExtensions
{
    /// <summary>
    ///     Fetches one keyset page as raw JSON plus a continuation cursor. On the first request pass
    ///     <paramref name="cursor"/> = <c>null</c>; on subsequent requests pass the previous page's
    ///     <see cref="CursorPageResult.NextCursor"/>. When the page returns fewer than
    ///     <paramref name="pageSize"/> items the returned <see cref="CursorPageResult.NextCursor"/> is
    ///     <c>null</c> (end of set).
    /// </summary>
    /// <param name="queryable">
    ///     A Polecat query ordered so its terminal key is the document identity (e.g.
    ///     <c>OrderBy(x =&gt; x.Foo).ThenBy(x =&gt; x.Id)</c>). Ordering that lacks an OrderBy, or whose
    ///     terminal key is not the identity member, is rejected.
    /// </param>
    /// <param name="cursor">The opaque continuation cursor, or <c>null</c> for the first page.</param>
    /// <param name="pageSize">Page size. Must be &gt;= 1.</param>
    /// <param name="token">Cancellation token.</param>
    /// <exception cref="ArgumentOutOfRangeException">If <paramref name="pageSize"/> is below 1.</exception>
    /// <exception cref="InvalidOperationException">If the query has no OrderBy or a non-identity terminal key.</exception>
    public static async Task<CursorPageResult> ToJsonPageByCursorAsync<T>(
        this IQueryable<T> queryable, string? cursor, int pageSize, CancellationToken token = default)
    {
        if (queryable.Provider is not PolecatLinqQueryProvider provider)
        {
            throw new InvalidOperationException(
                "ToJsonPageByCursorAsync can only be used with Polecat IQueryable instances.");
        }

        return await provider.ExecuteCursorPageJsonAsync(queryable.Expression, cursor, pageSize, token);
    }
}
