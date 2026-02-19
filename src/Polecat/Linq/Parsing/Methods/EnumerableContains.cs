using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

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

    public ISqlFragment Parse(MemberFactory memberFactory, MethodCallExpression expression)
    {
        // Check if this is a collection property contains: x.Tags.Contains("value")
        if (IsCollectionPropertyContains(expression))
        {
            return ParseCollectionPropertyContains(memberFactory, expression);
        }

        // Otherwise it's list.Contains(x.Prop) → IN clause
        return ParseListContainsMember(memberFactory, expression);
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

        return false;
    }

    private static ISqlFragment ParseCollectionPropertyContains(
        MemberFactory memberFactory, MethodCallExpression expression)
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
            // Static: Enumerable.Contains(x.Tags, "value")
            collectionMember = (MemberExpression)expression.Arguments[0];
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
        MemberFactory memberFactory, MethodCallExpression expression)
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
            throw new NotSupportedException($"Contains requires a member expression");

        var member = memberFactory.ResolveMember(me);
        var values = ExtractListValue(listExpr);

        if (values is not IEnumerable enumerable)
            throw new NotSupportedException($"Contains source must be enumerable");

        var list = new List<object?>();
        foreach (var item in enumerable) list.Add(item);

        return new InFilter(member.TypedLocator, member, list);
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
        // Strip Convert (including implicit span conversions)
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            expression = unary.Operand;

        // Strip method call conversions (e.g., ReadOnlySpan<T>.op_Implicit)
        if (expression is MethodCallExpression call && call.Arguments.Count == 1
            && call.Method.Name is "op_Implicit" or "op_Explicit")
        {
            expression = call.Arguments[0];
        }

        return expression;
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

    public void Apply(CommandBuilder builder)
    {
        builder.AppendParameter(_value);
        builder.Append($" IN (SELECT [value] FROM OPENJSON(data, '{_jsonPath}'))");
    }
}
