using JasperFx;
using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using Polecat.Linq.Parsing;

namespace Polecat.Linq.Members;

/// <summary>
///     gh-1014 (marten#5949, marten#5943): an index into an array or a list the document holds,
///     <c>x.Values[1]</c>, <c>x.Grid[1][0]</c>, <c>x.Items[0].Name</c> or <c>x.Values.ElementAt(2)</c>, read as
///     the JSON path <c>$.values[1]</c>, <c>$.grid[1][0]</c>, <c>$.items[0].name</c>.
/// </summary>
/// <remarks>
///     <para>
///         The index has to be known before the query runs: a literal, a captured local or a static. It is
///         written into the path rather than bound, because a JSON path is a literal. A negative index is
///         refused, because <c>[-1]</c> is not a path SQL Server reads, and an index computed from the
///         document would need a path built per row.
///     </para>
///     <para>
///         An index past the end reads SQL NULL, the way every missing path does: a comparison against it
///         matches nothing, and a scalar <c>Select</c> of it reads the type's default, as Marten does. LINQ to
///         Objects would throw.
///     </para>
/// </remarks>
internal static class DocumentPaths
{
    /// <summary>
    ///     Whether <paramref name="node" /> indexes into a collection: <c>a[i]</c> over an array, the indexer
    ///     of a list, or <c>Enumerable.ElementAt(a, i)</c>.
    /// </summary>
    public static bool TryGetIndex(Expression node, out Expression collection, out Expression index)
    {
        switch (node)
        {
            case BinaryExpression { NodeType: ExpressionType.ArrayIndex } arrayIndex:
                collection = arrayIndex.Left;
                index = arrayIndex.Right;
                return true;

            case MethodCallExpression
            {
                Method.Name: "get_Item", Object: { } list, Arguments: [{ Type: var indexType } listIndex]
            } when indexType == typeof(int) && IsList(list.Type):
                collection = list;
                index = listIndex;
                return true;

            // marten#5943: a dictionary's indexer is a key of the JSON object it is stored as
            case MethodCallExpression
            {
                Method.Name: "get_Item", Object: { } dictionary, Arguments: [{ } key]
            } when IsDictionary(dictionary.Type):
                collection = dictionary;
                index = key;
                return true;

            case MethodCallExpression { Method.Name: nameof(Enumerable.ElementAt), Object: null, Arguments: [var source, var at] }
                when node is MethodCallExpression call && call.Method.DeclaringType == typeof(Enumerable)
                                                       && at.Type == typeof(int):
                collection = source;
                index = at;
                return true;
        }

        collection = null!;
        index = null!;
        return false;
    }

    /// <summary>
    ///     Whether <paramref name="expression" /> is a chain of members and indexes that starts at the queried
    ///     row and has at least one index along it.
    /// </summary>
    public static bool IsIndexPath(Expression expression)
    {
        var indexed = false;
        var current = Unwrapped(expression);

        while (true)
        {
            switch (current)
            {
                case ParameterExpression:
                    return indexed;

                case MemberExpression member:
                    current = Unwrapped(member.Expression);
                    continue;
            }

            if (TryGetIndex(current!, out var collection, out _))
            {
                indexed = true;
                current = Unwrapped(collection);
                continue;
            }

            return false;
        }
    }

    /// <summary>
    ///     Whether the member chain <paramref name="expression" /> starts at the queried row, stepping over
    ///     indexes and union case casts on the way.
    /// </summary>
    public static bool IsRooted(MemberExpression expression)
    {
        Expression? current = expression;

        while (current is MemberExpression member)
        {
            if (Unwrapped(member.Expression) is ParameterExpression) return true;

            var owner = UnionMembers.Owner(member);
            if (owner != null)
            {
                current = owner;
                continue;
            }

            current = Unwrapped(member.Expression);
            while (current != null && TryGetIndex(current, out var collection, out _))
            {
                current = Unwrapped(collection);
            }

            if (current is ParameterExpression) return true;
        }

        return false;
    }

    /// <summary>
    ///     The value of an index, which has to be known before the query runs and not be negative.
    /// </summary>
    public static int IndexValue(Expression index, Expression whole)
    {
        if (ReadsTheRow.In(index))
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate '{whole}' to SQL: its index is computed from the document, and a JSON "
                + "path is written before the query runs. Index with a constant, a local or a static.");
        }

        var value = (int)WhereClauseParser.ExtractValue(index)!;
        if (value < 0)
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate '{whole}' to SQL: the index {value} is negative, and a JSON path has no "
                + "negative index. LINQ to Objects would throw for it too.");
        }

        return value;
    }

    /// <summary>Whether <paramref name="type" /> is a dictionary, whose indexer is a key rather than a position.</summary>
    public static bool IsDictionary(Type type)
        => typeof(IDictionary).IsAssignableFrom(type)
           || type.GetInterfaces().Concat([type]).Any(i =>
               i.IsGenericType && (i.GetGenericTypeDefinition() == typeof(IDictionary<,>)
                                   || i.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)));

    /// <summary>
    ///     The key of a dictionary's indexer, which has to be known before the query runs.
    /// </summary>
    public static object KeyValue(Expression key, Expression whole)
    {
        if (ReadsTheRow.In(key))
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate '{whole}' to SQL: its key is computed from the document, and a JSON path is "
                + "written before the query runs. Use a constant, a local or a static as the key.");
        }

        return WhereClauseParser.ExtractValue(key)
               ?? throw new BadLinqExpressionException($"Polecat cannot translate '{whole}' to SQL: its key is null.");
    }

    /// <summary>
    ///     A collection System.Text.Json writes as a JSON array, so its indexer is a position in it. A
    ///     dictionary's indexer is a key.
    /// </summary>
    private static bool IsList(Type type)
        => type != typeof(string)
           && typeof(IEnumerable).IsAssignableFrom(type)
           && !typeof(IDictionary).IsAssignableFrom(type)
           && !type.GetInterfaces().Concat([type]).Any(i =>
               i.IsGenericType && (i.GetGenericTypeDefinition() == typeof(IDictionary<,>)
                                   || i.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)));

    private static Expression? Unwrapped(Expression? expression)
    {
        while (expression is UnaryExpression
               {
                   NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs
               } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }
}

/// <summary>
///     One segment of a JSON path: a stored member, or, where <see cref="Member" /> is null, a position in an
///     array.
/// </summary>
internal readonly record struct PathSegment(MemberInfo? Member, int Index, object? Key = null);

/// <summary>
///     Whether an expression reads the queried row:whether it has a parameter in it that no lambda inside
///     it binds. One that does not can be worked out before the query runs.
/// </summary>
internal sealed class ReadsTheRow : ExpressionVisitor
{
    private readonly HashSet<ParameterExpression> _bound = [];
    private bool _found;

    public static bool In(Expression expression)
    {
        var finder = new ReadsTheRow();
        finder.Visit(expression);
        return finder._found;
    }

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        _bound.UnionWith(node.Parameters);
        return base.VisitLambda(node);
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        _found |= !_bound.Contains(node);
        return node;
    }

    // A member read off a static has no row in it, and should not be walked as though it did.
    protected override Expression VisitMember(MemberExpression node)
        => node.Expression == null ? node : base.VisitMember(node);
}
