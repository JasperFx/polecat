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
