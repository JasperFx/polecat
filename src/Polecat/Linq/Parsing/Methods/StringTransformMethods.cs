using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses string.ToLower()/ToLowerInvariant() calls within comparisons.
///     Transforms x.Name.ToLower() == "abc" → LOWER(JSON_VALUE(data, '$.name')) = @p0
/// </summary>
internal class StringToLower : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(string)
            && (expression.Method.Name == "ToLower" || expression.Method.Name == "ToLowerInvariant")
            && expression.Arguments.Count == 0;
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        // This is called when ToLower() is the object of a comparison.
        // The comparison will be handled by the parent expression visitor.
        // We return a marker that applies LOWER() to the locator.
        var member = memberFactory.ResolveMember((MemberExpression)expression.Object!);
        return new SqlFunctionLocator("LOWER", member.RawLocator);
    }
}

/// <summary>
///     Parses string.ToUpper()/ToUpperInvariant() calls within comparisons.
/// </summary>
internal class StringToUpper : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(string)
            && (expression.Method.Name == "ToUpper" || expression.Method.Name == "ToUpperInvariant")
            && expression.Arguments.Count == 0;
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        var member = memberFactory.ResolveMember((MemberExpression)expression.Object!);
        return new SqlFunctionLocator("UPPER", member.RawLocator);
    }
}

/// <summary>
///     Parses string.Trim() calls within comparisons.
/// </summary>
internal class StringTrim : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(string)
            && expression.Method.Name is "Trim" or "TrimStart" or "TrimEnd"
            && expression.Arguments.Count == 0;
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        var member = memberFactory.ResolveMember((MemberExpression)expression.Object!);
        var function = expression.Method.Name switch
        {
            "Trim" => "LTRIM(RTRIM",
            "TrimStart" => "LTRIM",
            "TrimEnd" => "RTRIM",
            _ => "LTRIM(RTRIM"
        };

        if (expression.Method.Name == "Trim")
        {
            // LTRIM(RTRIM(x)) — needs double wrapping
            return new SqlFunctionLocator("LTRIM", $"RTRIM({member.RawLocator})");
        }

        return new SqlFunctionLocator(function, member.RawLocator);
    }
}
