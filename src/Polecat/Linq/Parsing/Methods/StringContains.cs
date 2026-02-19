using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses string.Contains("value") → LIKE '%value%'.
/// </summary>
internal class StringContains : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.Name == "Contains"
            && expression.Method.DeclaringType == typeof(string)
            && expression.Arguments.Count == 1;
    }

    public ISqlFragment Parse(MemberFactory memberFactory, MethodCallExpression expression)
    {
        var member = memberFactory.ResolveMember((MemberExpression)expression.Object!);
        var value = WhereClauseParser.ExtractValue(expression.Arguments[0]);
        var pattern = $"%{EscapeLikePattern(value?.ToString() ?? "")}%";
        return new ComparisonFilter(member.RawLocator, "LIKE", pattern);
    }

    internal static string EscapeLikePattern(string value)
    {
        return value
            .Replace("[", "[[]")
            .Replace("%", "[%]")
            .Replace("_", "[_]");
    }
}
