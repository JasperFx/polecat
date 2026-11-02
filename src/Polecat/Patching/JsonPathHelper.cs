using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Polecat.Serialization;
using Weasel.Core;

namespace Polecat.Patching;

/// <summary>
///     Extracts JSON paths (e.g., "$.camelCase.path") from lambda expressions,
///     using the serializer's JsonNamingPolicy.
/// </summary>
/// <remarks>
///     <para>
///         ⚠️ <b>A path is matched against the text of the stored key, not against the key.</b> That
///         goes for native <c>json</c> as well as <c>nvarchar(max)</c>, and it decides how a segment has
///         to be written. System.Text.Json stores <c>clé</c> as <c>cl\u00E9</c> by default, so
///         <c>$."clé"</c> finds nothing, and a <c>JSON_MODIFY</c> that finds nothing adds a second member
///         beside the first, with no error. See <see cref="Segment" />.
///     </para>
///     <para>
///         ⚠️ <b>A segment can contain a dot once it is quoted</b>, so a path has to be taken apart
///         with <see cref="SplitPath" /> and never by splitting on the dot.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: builds Expression.Lambda delegates for evaluating constant sub-expressions in patch paths — runtime code generation. The targeted lambdas reference document properties whose owning types are preserved by registration on the caller side.")]
internal static class JsonPathHelper
{
    /// <param name="expression">The member or dictionary entry.</param>
    /// <param name="namingPolicy">The serializer's policy for property names.</param>
    /// <param name="serializer">
    ///     The store's serializer, which decides how a key is spelled and escaped. Its
    ///     <see cref="ISerializer.ToJson" /> and never the storage form, because a number is rewritten
    ///     for storage and a dictionary key is a string that keeps its text.
    /// </param>
    public static string ToPath(Expression expression, JsonNamingPolicy? namingPolicy,
        ISerializer? serializer = null)
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
        CollectSegments(expression, segments, namingPolicy, serializer);
        return string.Join(".", segments);
    }

    private static void CollectSegments(Expression expression, List<string> segments,
        JsonNamingPolicy? namingPolicy, ISerializer? serializer)
    {
        switch (expression)
        {
            case MemberExpression member when IsValueOfNullable(member):
                // x.N.Value: the serializer writes the value at N, and there is no key called value
                CollectSegments(member.Expression!, segments, namingPolicy, serializer);
                break;

            case MemberExpression member:
                if (member.Expression is not ParameterExpression)
                {
                    CollectSegments(member.Expression!, segments, namingPolicy, serializer);
                }

                segments.Add(Segment(FormatMember(member.Member, namingPolicy), serializer));
                break;

            case MethodCallExpression methodCall when IsListIndexer(methodCall):
                // An element has no key, and quoting the index would make this a path to nothing
                throw new NotSupportedException(
                    $"A patch path cannot go through the list index in {methodCall}. Use Insert or Remove " +
                    "for an element, or Set the whole list.");

            case MethodCallExpression methodCall when IsDictionaryIndexer(methodCall):
                // Dictionary indexer: x.Dict["key"] or x.Dict[variable]
                CollectSegments(methodCall.Object!, segments, namingPolicy, serializer);
                var key = EvaluateExpression(methodCall.Arguments[0])
                          ?? throw new InvalidOperationException("Dictionary key cannot be null.");

                segments.Add(Segment(KeyText(key, serializer), serializer));
                break;

            case ParameterExpression:
                // Root parameter, nothing to add
                break;

            default:
                throw new NotSupportedException(
                    $"Unsupported expression type in patch path: {expression.NodeType} ({expression.GetType().Name})");
        }
    }

    /// <summary>
    ///     <c>.Value</c> on a nullable (#875). This is the rule for the patch and index paths, which
    ///     build theirs separately from the LINQ translator.
    /// </summary>
    internal static bool IsValueOfNullable(MemberExpression member)
        => member.Member.Name == nameof(Nullable<int>.Value)
           && member.Expression != null
           && Nullable.GetUnderlyingType(member.Expression.Type) != null;

    private static bool IsListIndexer(MethodCallExpression expression)
    {
        if (expression.Method.Name != "get_Item" || expression.Method.DeclaringType is not { IsGenericType: true } type)
        {
            return false;
        }

        var definition = type.GetGenericTypeDefinition();
        return definition == typeof(List<>) || definition == typeof(IList<>) || definition == typeof(IReadOnlyList<>);
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
    /// <remarks>
    ///     #507: internal rather than private because the index DDL builder needs the SAME answer.
    ///     It had its own copy that applied a casing transform and never looked at the attribute, so
    ///     an aliased member got a computed column over a path the serializer never writes.
    ///     There is one rule and it lives here.
    /// </remarks>
    internal static string FormatMember(MemberInfo member, JsonNamingPolicy? namingPolicy)
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

    /// <summary>
    ///     #861: the key System.Text.Json writes for a dictionary key, before any escaping.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         A key is written the way a value of its type is, so <see cref="DateOnly" />,
    ///         <see cref="DateTime" />, <see cref="Guid" /> and the numbers come out of the serializer
    ///         here, invariant on every host (#848), with any converter that was registered for them.
    ///     </para>
    ///     <para>
    ///         There are two differences from a value.
    ///         <see cref="JsonSerializerOptions.PropertyNamingPolicy" /> is not applied to a key at all:
    ///         it has <see cref="JsonSerializerOptions.DictionaryKeyPolicy" /> of its own, for strings
    ///         and enums only. And an enum is written by name as a key even where its value is stored
    ///         as an integer.
    ///     </para>
    /// </remarks>
    internal static string KeyText(object key, ISerializer? serializer)
    {
        var policy = (serializer as Serializer)?.Options.DictionaryKeyPolicy;

        if (key is string text)
        {
            return policy?.ConvertName(text) ?? text;
        }

        using var value = JsonDocument.Parse(serializer?.ToJson(key) ?? JsonSerializer.Serialize(key, key.GetType()));

        if (key is Enum)
        {
            var name = value.RootElement.ValueKind == JsonValueKind.String
                ? value.RootElement.GetString()!
                : key.ToString()!;

            return policy?.ConvertName(name) ?? name;
        }

        return value.RootElement.ValueKind == JsonValueKind.String
            ? value.RootElement.GetString()!
            : value.RootElement.GetRawText();
    }

    /// <summary>
    ///     A key as one segment of a path. An ASCII identifier goes in as it is. Anything else has to be
    ///     quoted, and inside the quotes it has to be what the serializer wrote, escapes and all, so it
    ///     is the serializer that writes it. See the remarks on this type.
    /// </summary>
    internal static string Segment(string key, ISerializer? serializer)
    {
        if (IsPlainIdentifier(key))
        {
            return key;
        }

        return serializer?.ToJson(key) ?? $"\"{JsonEncodedText.Encode(key)}\"";
    }

    /// <summary>
    ///     What a path can name without quotes, measured rather than read: a digit or a non-ASCII letter
    ///     is refused as a badly formed path, and a bare <c>*</c> is taken as a wildcard.
    /// </summary>
    private static bool IsPlainIdentifier(string key)
    {
        if (key.Length == 0 || char.IsAsciiDigit(key[0]))
        {
            return false;
        }

        foreach (var character in key)
        {
            if (!char.IsAsciiLetterOrDigit(character) && character != '_')
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    ///     The segments of a path, leaving alone any dot inside a quoted one.
    /// </summary>
    internal static string[] SplitPath(string path)
    {
        var segments = new List<string>();
        var start = 0;
        var quoted = false;

        for (var i = 0; i < path.Length; i++)
        {
            switch (path[i])
            {
                case '\\' when quoted:
                    i++;
                    break;

                case '"':
                    quoted = !quoted;
                    break;

                case '.' when !quoted:
                    segments.Add(path[start..i]);
                    start = i + 1;
                    break;
            }
        }

        segments.Add(path[start..]);
        return segments.ToArray();
    }

    /// <summary>
    ///     The key in a segment, unescaped, which is how <c>OPENJSON</c> reports it in its
    ///     <c>[key]</c> column.
    /// </summary>
    internal static string KeyOf(string segment)
    {
        if (!segment.StartsWith('"'))
        {
            return segment;
        }

        using var key = JsonDocument.Parse(segment);
        return key.RootElement.GetString()!;
    }

    /// <summary>
    ///     A segment as the name of a JSON member, for writing one into a document by hand.
    /// </summary>
    internal static string MemberName(string segment)
        => segment.StartsWith('"') ? segment : $"\"{segment}\"";
}
