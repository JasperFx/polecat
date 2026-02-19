namespace Polecat.Linq;

/// <summary>
///     Polecat-specific queryable interface, supporting LINQ queries against document tables.
/// </summary>
public interface IPolecatQueryable<out T> : IOrderedQueryable<T>
{
}
