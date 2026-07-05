using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Polecat.Patching;

/// <summary>
///     Extracts JSON paths (e.g., "$.camelCase.path") from lambda expressions,
///     using the serializer's JsonNamingPolicy.
/// </summary>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: builds Expression.Lambda delegates for evaluating constant sub-expressions in patch paths — runtime code generation. The targeted lambdas reference document properties whose owning types are preserved by registration on the caller side.")]
internal static class JsonPathHelper
{
    public static string ToPath(Expression expression, JsonNamingPolicy? namingPolicy)
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
        CollectSegments(expression, segments, namingPolicy);
        return string.Join(".", segments);
    }

    private static void CollectSegments(Expression expression, List<string> segments,
        JsonNamingPolicy? namingPolicy)
    {
        switch (expression)
        {
            case MemberExpression member:
                if (member.Expression is not ParameterExpression)
                {
                    CollectSegments(member.Expression!, segments, namingPolicy);
                }

                segments.Add(FormatMember(member.Member, namingPolicy));
                break;

            case MethodCallExpression methodCall when IsDictionaryIndexer(methodCall):
                // Dictionary indexer: x.Dict["key"] or x.Dict[variable]
                CollectSegments(methodCall.Object!, segments, namingPolicy);
                var key = EvaluateExpression(methodCall.Arguments[0]);
                segments.Add(FormatName(
                    key?.ToString() ?? throw new InvalidOperationException("Dictionary key cannot be null."),
                    namingPolicy));
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

    /// <summary>
    ///     #270: resolves the JSON key for a member exactly as System.Text.Json serializes it — an
    ///     explicit [JsonPropertyName] wins verbatim over the naming policy, otherwise the configured
    ///     naming policy is applied — so a patch targets the same key the document was written with.
    /// </summary>
    private static string FormatMember(MemberInfo member, JsonNamingPolicy? namingPolicy)
    {
        var attribute = member.GetCustomAttribute<JsonPropertyNameAttribute>();
        if (attribute != null)
        {
            return attribute.Name;
        }

        return FormatName(member.Name, namingPolicy);
    }

    private static string FormatName(string clrName, JsonNamingPolicy? namingPolicy)
    {
        return namingPolicy?.ConvertName(clrName) ?? clrName;
    }
}
