using System.Collections;
using System.Linq.Expressions;

namespace Polecat.Linq;

/// <summary>
///     IQueryable implementation that builds a LINQ expression tree for Polecat queries.
/// </summary>
internal class PolecatLinqQueryable<T> : IPolecatQueryable<T>
{
    public PolecatLinqQueryable(PolecatLinqQueryProvider provider)
    {
        Provider = provider;
        Expression = Expression.Constant(this);
    }

    public PolecatLinqQueryable(PolecatLinqQueryProvider provider, Expression expression)
    {
        Provider = provider;
        Expression = expression;
    }

    internal PolecatLinqQueryProvider PolecatProvider => (PolecatLinqQueryProvider)Provider;

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
    {
        throw new NotSupportedException(
            "Polecat does not support synchronous LINQ enumeration. Use ToListAsync() instead.");
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
