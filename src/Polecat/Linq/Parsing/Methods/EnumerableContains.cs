using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses list.Contains(x.Prop) → JSON_VALUE locator IN (...).
///     Also handles x.Tags.Contains("value") → OPENJSON subquery.
/// </summary>
internal class EnumerableContains : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        // Enumerable.Contains(source, value) - static
        if (expression.Method.Name == "Contains" && expression.Method.DeclaringType == typeof(Enumerable))
            return true;

        // MemoryExtensions.Contains (used by .NET for array.Contains() in .NET 8+)
        if (expression.Method.Name == "Contains" && expression.Method.DeclaringType == typeof(MemoryExtensions))
            return true;

        // List<T>.Contains(value) or ICollection<T>.Contains(value) - instance
        if (expression.Method.Name == "Contains" && expression.Arguments.Count == 1
            && expression.Object != null)
        {
            var objType = expression.Object.Type;
            if (objType.IsGenericType && (
                objType.GetGenericTypeDefinition() == typeof(List<>) ||
                objType.GetGenericTypeDefinition() == typeof(IList<>) ||
                objType.GetGenericTypeDefinition() == typeof(ICollection<>) ||
                objType.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
            {
                return true;
            }

            // Array
            if (objType.IsArray) return true;
        }

        return false;
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
        => Parse(memberFactory, expression, positive: false);

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression, bool positive)
    {
        // Check if this is a collection property contains: x.Tags.Contains("value")
        if (IsCollectionPropertyContains(expression))
        {
            return ParseCollectionPropertyContains(memberFactory, expression);
        }

        // Otherwise it's list.Contains(x.Prop) → IN clause
        return ParseListContainsMember(memberFactory, expression, positive);
    }

    private static bool IsCollectionPropertyContains(MethodCallExpression expression)
    {
        // Instance method: x.Tags.Contains("value")
        if (expression.Object is MemberExpression memberExpr)
        {
            return IsDocumentMember(memberExpr);
        }

        // Static Enumerable.Contains: first arg is the source
        if (expression.Method.DeclaringType == typeof(Enumerable) && expression.Arguments.Count == 2)
        {
            return expression.Arguments[0] is MemberExpression me && IsDocumentMember(me);
        }

        // #840: C# 14 binds x.Arr.Contains("a") on an array to MemoryExtensions, with the array inside
        // an implicit conversion to ReadOnlySpan<T>. .NET 10 adds an optional comparer, which a plain
        // call leaves null. A comparer that is really there has no SQL.
        if (expression.Method.DeclaringType == typeof(MemoryExtensions) && expression.Arguments.Count >= 2
            && (expression.Arguments.Count == 2 || expression.Arguments[2] is ConstantExpression { Value: null }))
        {
            return UnwrapToMember(expression.Arguments[0]) is MemberExpression me && IsDocumentMember(me);
        }

        return false;
    }

    private static ISqlFragment ParseCollectionPropertyContains(
        IMemberResolver memberFactory, MethodCallExpression expression)
    {
        MemberExpression collectionMember;
        Expression valueExpr;

        if (expression.Object is MemberExpression instanceMember)
        {
            // Instance: x.Tags.Contains("value")
            collectionMember = instanceMember;
            valueExpr = expression.Arguments[0];
        }
        else
        {
            // Static: Enumerable.Contains(x.Tags, "value") or MemoryExtensions.Contains(x.Tags, "value")
            collectionMember = (MemberExpression)UnwrapToMember(expression.Arguments[0]);
            valueExpr = expression.Arguments[1];
        }

        var member = memberFactory.ResolveMember(collectionMember);
        var value = WhereClauseParser.ExtractValue(valueExpr);

        // Use OPENJSON subquery: @p0 IN (SELECT [value] FROM OPENJSON(data, '$.tags'))
        // Extract the JSON path from the raw locator
        var jsonPath = ExtractJsonPath(member.RawLocator);
        return new OpenjsonContainsFilter(jsonPath, value!);
    }

    private static ISqlFragment ParseListContainsMember(
        IMemberResolver memberFactory, MethodCallExpression expression, bool positive)
    {
        Expression memberExpr;
        Expression listExpr;

        if (expression.Method.DeclaringType == typeof(Enumerable)
            || expression.Method.DeclaringType == typeof(MemoryExtensions))
        {
            // Enumerable.Contains(list, x.Prop) or MemoryExtensions.Contains(array, x.Prop)
            listExpr = expression.Arguments[0];
            memberExpr = expression.Arguments[1];
        }
        else
        {
            // list.Contains(x.Prop)
            listExpr = expression.Object!;
            memberExpr = expression.Arguments[0];
        }

        var stripped = StripConvert(memberExpr);
        if (stripped is not MemberExpression me)
            throw new BadLinqExpressionException(
                $"Contains() requires a member expression, got: {stripped}");

        var member = memberFactory.ResolveMember(me);
        var values = ExtractListValue(listExpr);

        if (values is not IEnumerable enumerable)
            throw new BadLinqExpressionException(
                "Polecat cannot translate this Contains(): the source has to be an enumerable known "
                + "before the query runs — an array, a collection, or a variable holding one.");

        var list = new List<object?>();
        foreach (var item in enumerable) list.Add(item);

        return new InFilter(member.ListLocator(positive, list), member, list);
    }

    /// <summary>
    ///     Extract the list/array value from a list expression.
    ///     Handles MemoryExtensions conversion wrapping (ReadOnlySpan) that can't
    ///     be compiled via Expression.Lambda().
    /// </summary>
    private static object? ExtractListValue(Expression expression)
    {
        // For MemoryExtensions.Contains, the array may be wrapped in a conversion
        // to ReadOnlySpan<T> which can't be compiled. Unwrap to the underlying member.
        var unwrapped = UnwrapToMember(expression);
        return WhereClauseParser.ExtractValue(unwrapped);
    }

    private static Expression UnwrapToMember(Expression expression)
    {
        // Convert and op_Implicit can come in either order, and C# 14 emits both for an array:
        // op_Implicit(Convert(x.Tags, String[]))
        while (true)
        {
            switch (expression)
            {
                case UnaryExpression { NodeType: ExpressionType.Convert } unary:
                    expression = unary.Operand;
                    break;

                // ReadOnlySpan<T>.op_Implicit and friends
                case MethodCallExpression { Method.Name: "op_Implicit" or "op_Explicit", Arguments.Count: 1 } call:
                    expression = call.Arguments[0];
                    break;

                default:
                    return expression;
            }
        }
    }

    private static string ExtractJsonPath(string rawLocator)
    {
        // rawLocator is like: JSON_VALUE(data, '$.tags')
        // Extract the path: $.tags
        var start = rawLocator.IndexOf("'", StringComparison.Ordinal) + 1;
        var end = rawLocator.LastIndexOf("'", StringComparison.Ordinal);
        return rawLocator[start..end];
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            expression = unary.Operand;
        return expression;
    }

    private static bool IsDocumentMember(MemberExpression expression)
    {
        var current = expression;
        while (current != null)
        {
            if (current.Expression is ParameterExpression) return true;
            current = current.Expression as MemberExpression;
        }

        return false;
    }
}

internal class OpenjsonContainsFilter : ISqlFragment
{
    private readonly string _jsonPath;
    private readonly object _value;

    public OpenjsonContainsFilter(string jsonPath, object value)
    {
        _jsonPath = jsonPath;
        _value = value;
    }

    public void Apply(ICommandBuilder builder)
    {
        builder.AppendParameter(_value);
        builder.Append($" IN (SELECT [value] FROM OPENJSON(data, '{_jsonPath}'))");
    }
}
