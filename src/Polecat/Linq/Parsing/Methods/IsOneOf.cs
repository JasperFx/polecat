using System.Collections;
using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses IsOneOf() and In() extensions → SQL IN (...).
/// </summary>
internal class IsOneOf : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(LinqExtensions)
            && expression.Method.Name is "IsOneOf" or "In";
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        // IsOneOf is an extension method: first arg is the member, second is the values
        var memberExpr = expression.Arguments[0];
        var valuesExpr = expression.Arguments[1];

        // Resolve the member
        var stripped = StripConvert(memberExpr);
        if (stripped is not MemberExpression me)
            throw new BadLinqExpressionException(
                $"IsOneOf()/In() requires a member expression on the left, got: {stripped}");

        var member = memberFactory.ResolveMember(me);
        var values = ExtractValues(valuesExpr);

        return new InFilter(member.TypedLocator, member, values);
    }

    private static IList ExtractValues(Expression expression)
    {
        var value = WhereClauseParser.ExtractValue(expression);
        if (value is IList list) return list;
        if (value is IEnumerable enumerable)
        {
            var result = new List<object?>();
            foreach (var item in enumerable) result.Add(item);
            return result;
        }

        throw new BadLinqExpressionException(
            $"Polecat cannot evaluate the IsOneOf()/In() value list '{expression}'. Pass an array, a "
            + "collection, or a variable holding one — the list has to be known before the query runs.");
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            expression = unary.Operand;
        return expression;
    }
}

internal class InFilter : ISqlFragment
{
    private readonly string _locator;
    private readonly IQueryableMember _member;
    private readonly IList _values;

    public InFilter(string locator, IQueryableMember member, IList values)
    {
        _locator = locator;
        _member = member;
        _values = values;
    }

    public void Apply(ICommandBuilder builder)
    {
        if (_values.Count == 0)
        {
            builder.Append("1=0"); // No values → always false
            return;
        }

        // #710: SQL Server rejects a command with more than 2100 parameters, so a large list cannot
        // travel as one parameter per value -- it threw rather than running. Above the threshold the
        // whole list goes as ONE JSON array parameter instead, which is what the by-id paths have
        // done since #363 and why LoadManyAsync worked over a list this could not.
        //
        // The locator's own SQL type is what the unpacked column is typed to; see
        // JsonValueList.AppendInClause for why reading it beats inferring it in both directions.
        if (JsonValueList.ShouldBindAsJsonArray(builder, _values.Count))
        {
            JsonValueList.AppendInClause(builder, _locator, _values, _member.LocatorSqlType,
                _member.ConvertValue);
            return;
        }

        builder.Append(_locator);
        builder.Append(" IN (");
        for (var i = 0; i < _values.Count; i++)
        {
            if (i > 0) builder.Append(", ");
            builder.AppendParameter(_member.ConvertValue(_values[i])!);
        }

        builder.Append(")");
    }
}
