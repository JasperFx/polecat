using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;

namespace Polecat.Patching;

/// <summary>
///     Extracts JSON paths (e.g., "$.camelCase.path") from lambda expressions,
///     using the serializer's JsonNamingPolicy.
/// </summary>
internal static class JsonPathHelper
{
    public static string ToPath(Expression expression, JsonNamingPolicy? namingPolicy)
    {
        var members = ExtractMembers(expression);
        var segments = members.Select(m => FormatName(m, namingPolicy));
        return string.Join(".", segments);
    }

    private static List<string> ExtractMembers(Expression expression)
    {
        // Unwrap lambda
        if (expression is LambdaExpression lambda)
        {
            expression = lambda.Body;
        }

        // Unwrap Convert (for boxing scenarios like Func<T, object>)
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            expression = unary.Operand;
        }

        var segments = new List<string>();
        CollectSegments(expression, segments);
        return segments;
    }

    private static void CollectSegments(Expression expression, List<string> segments)
    {
        switch (expression)
        {
            case MemberExpression member:
                if (member.Expression is not ParameterExpression)
                {
                    CollectSegments(member.Expression!, segments);
                }

                segments.Add(member.Member.Name);
                break;

            case MethodCallExpression methodCall when IsDictionaryIndexer(methodCall):
                // Dictionary indexer: x.Dict["key"] or x.Dict[variable]
                CollectSegments(methodCall.Object!, segments);
                var key = EvaluateExpression(methodCall.Arguments[0]);
                segments.Add(key?.ToString() ?? throw new InvalidOperationException("Dictionary key cannot be null."));
                break;

            case ParameterExpression:
                // Root parameter, nothing to add
                break;

            default:
                throw new NotSupportedException(
                    $"Unsupported expression type in patch path: {expression.NodeType} ({expression.GetType().Name})");
        }
    }

    private static bool IsDictionaryIndexer(MethodCallExpression expression)
    {
        return expression.Method.Name == "get_Item"
               && expression.Object != null
               && expression.Arguments.Count == 1;
    }

    private static object? EvaluateExpression(Expression expression)
    {
        // Unwrap Convert
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            expression = unary.Operand;
        }

        if (expression is ConstantExpression constant)
        {
            return constant.Value;
        }

        // Closure variable: field on a captured closure object
        if (expression is MemberExpression { Expression: ConstantExpression closureObj } memberExpr)
        {
            return memberExpr.Member switch
            {
                FieldInfo field => field.GetValue(closureObj.Value),
                PropertyInfo prop => prop.GetValue(closureObj.Value),
                _ => CompileAndInvoke(expression)
            };
        }

        return CompileAndInvoke(expression);
    }

    private static object? CompileAndInvoke(Expression expression)
    {
        var lambda = Expression.Lambda(expression);
        var compiled = lambda.Compile();
        return compiled.DynamicInvoke();
    }

    private static string FormatName(string clrName, JsonNamingPolicy? namingPolicy)
    {
        return namingPolicy?.ConvertName(clrName) ?? clrName;
    }
}
