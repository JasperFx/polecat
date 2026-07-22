namespace Polecat.Linq.CursorPaging;

/// <summary>
///     The result of a keyset (cursor / seek) page: the raw JSON array of the page's documents,
///     the number of items returned, and an opaque continuation cursor for the next page
///     (<c>null</c> when the end of the set has been reached).
/// </summary>
/// <param name="ItemsJson">A JSON array (<c>[...]</c>) of the raw persisted document JSON for the page.</param>
/// <param name="Count">The number of items on this page (0..pageSize).</param>
/// <param name="NextCursor">The opaque cursor to fetch the next page, or <c>null</c> at end of set.</param>
public sealed record CursorPageResult(string ItemsJson, int Count, string? NextCursor);
