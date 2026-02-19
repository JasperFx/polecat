using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses string.Equals(value, StringComparison) for case-insensitive comparison.
/// </summary>
internal class StringEquals : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.Name == "Equals"
            && expression.Method.DeclaringType == typeof(string)
            && expression.Arguments.Count == 2
            && expression.Arguments[1].Type == typeof(StringComparison);
    }

    public ISqlFragment Parse(MemberFactory memberFactory, MethodCallExpression expression)
    {
        var member = memberFactory.ResolveMember((MemberExpression)expression.Object!);
        var value = WhereClauseParser.ExtractValue(expression.Arguments[0]);
        var comparison = (StringComparison)WhereClauseParser.ExtractValue(expression.Arguments[1])!;

        if (comparison is StringComparison.OrdinalIgnoreCase or StringComparison.CurrentCultureIgnoreCase
            or StringComparison.InvariantCultureIgnoreCase)
        {
            // SQL Server default collation is typically case-insensitive (CI).
            // Use explicit COLLATE for guaranteed case-insensitive comparison.
            return new CaseInsensitiveEqualsFilter(member.RawLocator, value!);
        }

        return new ComparisonFilter(member.RawLocator, "=", value!);
    }
}

/// <summary>
///     Case-insensitive equals using COLLATE.
/// </summary>
internal class CaseInsensitiveEqualsFilter : ISqlFragment
{
    private readonly string _locator;
    private readonly object _value;

    public CaseInsensitiveEqualsFilter(string locator, object value)
    {
        _locator = locator;
        _value = value;
    }

    public void Apply(CommandBuilder builder)
    {
        builder.Append(_locator);
        builder.Append(" COLLATE Latin1_General_CI_AS = ");
        builder.AppendParameter(_value);
    }
}
