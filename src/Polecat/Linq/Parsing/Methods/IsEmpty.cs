using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses x.Tags.IsEmpty() → OPENJSON count check.
/// </summary>
internal class IsEmpty : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(LinqExtensions)
            && expression.Method.Name == "IsEmpty";
    }

    public ISqlFragment Parse(MemberFactory memberFactory, MethodCallExpression expression)
    {
        // IsEmpty is an extension: first arg is the collection member
        var memberExpr = expression.Arguments[0];
        if (memberExpr is MemberExpression me)
        {
            var member = memberFactory.ResolveMember(me);
            var jsonPath = ExtractJsonPath(member.RawLocator);
            return new WhereFragment(
                $"(SELECT COUNT(*) FROM OPENJSON(data, '{jsonPath}')) = 0");
        }

        throw new NotSupportedException($"IsEmpty requires a member expression, got: {memberExpr}");
    }

    private static string ExtractJsonPath(string rawLocator)
    {
        var start = rawLocator.IndexOf("'", StringComparison.Ordinal) + 1;
        var end = rawLocator.LastIndexOf("'", StringComparison.Ordinal);
        return rawLocator[start..end];
    }
}
