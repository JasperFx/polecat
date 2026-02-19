using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses string.IsNullOrEmpty(x.Prop) and string.IsNullOrWhiteSpace(x.Prop).
/// </summary>
internal class StringIsNullOrEmpty : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(string)
            && expression.Method.Name is "IsNullOrEmpty" or "IsNullOrWhiteSpace"
            && expression.Method.IsStatic
            && expression.Arguments.Count == 1;
    }

    public ISqlFragment Parse(MemberFactory memberFactory, MethodCallExpression expression)
    {
        var arg = expression.Arguments[0];

        if (arg is MemberExpression memberExpr)
        {
            var member = memberFactory.ResolveMember(memberExpr);

            if (expression.Method.Name == "IsNullOrWhiteSpace")
            {
                return new WhereFragment(
                    $"({member.RawLocator} IS NULL OR LTRIM(RTRIM({member.RawLocator})) = '')");
            }

            return new WhereFragment(
                $"({member.RawLocator} IS NULL OR {member.RawLocator} = '')");
        }

        throw new NotSupportedException(
            $"string.{expression.Method.Name} requires a member expression argument.");
    }
}
