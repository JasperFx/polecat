using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses object.Equals(value) for non-string types (int, Guid, etc.).
/// </summary>
internal class ObjectEquals : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        // Match instance .Equals(object) calls on document member properties
        // Exclude string.Equals with StringComparison (handled by StringEquals)
        return expression.Method.Name == "Equals"
            && expression.Arguments.Count == 1
            && expression.Object is MemberExpression;
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        var member = memberFactory.ResolveMember((MemberExpression)expression.Object!);
        var value = WhereClauseParser.ExtractValue(expression.Arguments[0]);

        if (value == null)
        {
            return new WhereFragment($"{member.RawLocator} IS NULL");
        }

        var convertedValue = member.ConvertValue(value);
        return new ComparisonFilter(member.TypedLocator, "=", convertedValue!);
    }
}
