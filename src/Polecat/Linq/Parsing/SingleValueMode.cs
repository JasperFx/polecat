namespace Polecat.Linq.Parsing;

/// <summary>
///     Determines how a LINQ query produces a single value result.
/// </summary>
internal enum SingleValueMode
{
    First,
    FirstOrDefault,
    Single,
    SingleOrDefault,
    Last,
    LastOrDefault,
    Count,
    LongCount,
    Any,
    Sum,
    Min,
    Max,
    Average
}

internal static class SingleValueModeExtensions
{
    /// <summary>Whether this mode hands back one row of the query, rather than a value over all of them.</summary>
    internal static bool TakesOneRow(this SingleValueMode mode)
        => mode is SingleValueMode.First or SingleValueMode.FirstOrDefault or SingleValueMode.Single
            or SingleValueMode.SingleOrDefault or SingleValueMode.Last or SingleValueMode.LastOrDefault;

    /// <summary>
    ///     The answer to <paramref name="mode" /> from <paramref name="rows" />, which the statement has
    ///     already limited: to two for Single, so that a second one can be noticed.
    /// </summary>
    internal static T OneOf<T>(this SingleValueMode mode, IReadOnlyList<T> rows)
    {
        if (rows.Count == 0)
        {
            return mode is SingleValueMode.First or SingleValueMode.Single or SingleValueMode.Last
                ? throw new InvalidOperationException("Sequence contains no elements")
                : default!;
        }

        if (mode is SingleValueMode.Single or SingleValueMode.SingleOrDefault && rows.Count > 1)
        {
            throw new InvalidOperationException("Sequence contains more than one element");
        }

        return rows[0];
    }
}
