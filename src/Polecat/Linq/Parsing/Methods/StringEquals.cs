using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

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

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        var locator = TextLocator.For(memberFactory, expression);
        var value = WhereClauseParser.ExtractValue(expression.Arguments[0]);
        var comparison = (StringComparison)WhereClauseParser.ExtractValue(expression.Arguments[1])!;

        if (comparison is StringComparison.OrdinalIgnoreCase or StringComparison.CurrentCultureIgnoreCase
            or StringComparison.InvariantCultureIgnoreCase)
        {
            // SQL Server default collation is typically case-insensitive (CI).
            // Use explicit COLLATE for guaranteed case-insensitive comparison.
            return new CaseInsensitiveEqualsFilter(locator, value!);
        }

        return value is string
            ? new TextEqualityFilter(locator, TextEqualityFilter.Measured(locator), true, value)
            : new ComparisonFilter(locator, "=", value!);
    }
}

/// <summary>
///     Case-insensitive equals using COLLATE, and as unpadded as <see cref="TextEqualityFilter" />.
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

    public void Apply(ICommandBuilder builder)
    {
        // Parenthesized because the locator can already end in a COLLATE, and only one is allowed.
        builder.Append($"(({_locator}) COLLATE Latin1_General_CI_AS = ");
        var parameter = builder.AppendParameter(_value);
        builder.Append($" AND {TextEqualityFilter.SameBytes(TextEqualityFilter.Measured(_locator), "=", parameter.ParameterName)})");
    }
}
