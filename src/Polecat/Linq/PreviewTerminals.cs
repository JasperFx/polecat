using System.Linq.Expressions;
using JasperFx;

namespace Polecat.Linq;

/// <summary>
///     gh-1018 (marten#5916): the expression a <c>ToSql()</c> preview renders for a terminal operator. It is
///     the expression the async operator executes -- <c>CountAsync()</c> runs
///     <c>Queryable.Count(source)</c> through the provider -- so handing it to the same statement builder
///     execution uses leaves no second code path for the preview to drift along.
/// </summary>
internal static class PreviewTerminals
{
    /// <summary>
    ///     <paramref name="source" /> ending in the operator <paramref name="fetchType" /> stands for, as
    ///     <c>PolecatQueryableExtensions</c> composes it for execution.
    /// </summary>
    public static Expression Compose(Expression source, Type elementType, FetchType fetchType)
    {
        var name = fetchType switch
        {
            FetchType.FetchMany => null,
            FetchType.FetchOne => nameof(Queryable.First),
            FetchType.Count => nameof(Queryable.Count),
            FetchType.Any => nameof(Queryable.Any),
            _ => throw new ArgumentOutOfRangeException(nameof(fetchType), fetchType, null)
        };

        return name == null ? source : Expression.Call(typeof(Queryable), name, [elementType], source);
    }

    /// <summary>
    ///     <paramref name="terminal" />'s body with its parameter replaced by <paramref name="source" />.
    /// </summary>
    /// <exception cref="BadLinqExpressionException">
    ///     The body is not a <see cref="Queryable" /> operator, or it does not read its parameter.
    /// </exception>
    public static Expression Compose<T, TResult>(Expression source,
        Expression<Func<IQueryable<T>, TResult>> terminal)
    {
        if (terminal.Body is not MethodCallExpression call || call.Method.DeclaringType != typeof(Queryable))
        {
            throw new BadLinqExpressionException(
                $"ToSql() previews a Queryable operator applied to the query, such as q => q.Count() or "
                + $"q => q.Sum(x => x.Amount), and {terminal.Body} is not one. Use the synchronous operator the "
                + "async one executes: q.Count() for CountAsync().");
        }

        var replacer = new ParameterReplacer(terminal.Parameters[0], source);
        var composed = replacer.Visit(terminal.Body);

        if (!replacer.Replaced)
        {
            throw new BadLinqExpressionException(
                $"ToSql() previews an operator applied to the query it is given, and {terminal.Body} does not "
                + $"read the parameter {terminal.Parameters[0].Name}.");
        }

        return composed;
    }

    private sealed class ParameterReplacer(ParameterExpression parameter, Expression replacement)
        : ExpressionVisitor
    {
        public bool Replaced { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node != parameter) return base.VisitParameter(node);

            Replaced = true;
            return replacement;
        }
    }
}
