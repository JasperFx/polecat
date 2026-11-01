using System.Linq.Expressions;
using Polecat.Internal;
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

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        var arg = expression.Arguments[0];

        if (arg is MemberExpression memberExpr)
        {
            var member = memberFactory.ResolveMember(memberExpr);

            var value = expression.Method.Name == "IsNullOrWhiteSpace"
                ? SqlText.Trim(member.RawLocator)
                : member.RawLocator;

            // #839: DATALENGTH, not `= ''` and not LEN. `=` pads with trailing spaces, so it calls "   "
            // empty; the default collation gives some characters no weight at all, so it calls U+FEFF
            // empty too; and LEN ignores trailing spaces. Only the byte count means what .NET means.
            //
            // ISNULL folds "absent" and "empty" into one comparison, so the document is parsed once
            // rather than twice, and so the predicate is never UNKNOWN, which keeps a negation honest.
            return new WhereFragment($"ISNULL(DATALENGTH({value}), 0) = 0");
        }

        throw new BadLinqExpressionException(
            $"string.{expression.Method.Name} requires a member expression argument.");
    }
}
