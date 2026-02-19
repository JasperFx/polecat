using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses string.StartsWith("value") → LIKE 'value%'.
/// </summary>
internal class StringStartsWith : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.Name == "StartsWith"
            && expression.Method.DeclaringType == typeof(string)
            && expression.Arguments.Count == 1;
    }

    public ISqlFragment Parse(MemberFactory memberFactory, MethodCallExpression expression)
    {
        var member = memberFactory.ResolveMember((MemberExpression)expression.Object!);
        var value = WhereClauseParser.ExtractValue(expression.Arguments[0]);
        var pattern = $"{StringContains.EscapeLikePattern(value?.ToString() ?? "")}%";
        return new ComparisonFilter(member.RawLocator, "LIKE", pattern);
    }
}
