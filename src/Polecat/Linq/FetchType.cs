namespace Polecat.Linq;

/// <summary>
///     In basic terms, how is the IQueryable going to be executed? Chooses which statement
///     <see cref="IQuerySession.ToSql{T}(IQueryable{T}, FetchType)" /> previews.
/// </summary>
/// <remarks>
///     gh-1018 (marten#5916): the same four members as Marten's <c>FetchType</c>. A terminal these do not
///     name -- <c>LongCount()</c>, <c>Single()</c>, <c>Last()</c>, <c>Sum()</c>, <c>Min()</c>, <c>Max()</c>,
///     <c>Average()</c>, or any operator with a predicate -- is previewed by handing the operator itself to
///     <see cref="IQuerySession.ToSql{T, TResult}(IQueryable{T}, System.Linq.Expressions.Expression{Func{IQueryable{T}, TResult}})" />.
/// </remarks>
public enum FetchType
{
    /// <summary>
    ///     First/FirstOrDefault/Single/SingleOrDefault. A preview shows what <c>FirstAsync()</c> and
    ///     <c>FirstOrDefaultAsync()</c> send; <c>SingleAsync()</c> and <c>SingleOrDefaultAsync()</c> read up to
    ///     two rows instead of one, so that they can tell a second match apart, and <c>LastAsync()</c> reads
    ///     the last row of the reversed ordering.
    /// </summary>
    FetchOne,

    /// <summary>
    ///     Any execution that returns an IEnumerable (ToListAsync()/ToAsyncEnumerable()/etc.)
    /// </summary>
    FetchMany,

    /// <summary>
    ///     Using <c>CountAsync()</c>. <c>LongCountAsync()</c> reads the same count as a <c>bigint</c>, so its
    ///     statement differs by that cast.
    /// </summary>
    Count,

    /// <summary>
    ///     Using <c>AnyAsync()</c>
    /// </summary>
    Any
}
