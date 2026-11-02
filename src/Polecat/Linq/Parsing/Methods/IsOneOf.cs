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
        => Parse(memberFactory, expression, positive: false);

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression, bool positive)
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

        return new InFilter(member.ListLocator(positive, values), member, values);
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

/// <summary>
///     <c>locator IN (...)</c>, and for a string member the same list again with a character on the end.
/// </summary>
/// <remarks>
///     #877: <c>IN</c> is a chain of <c>=</c>, and <c>=</c> pads. The <c>DATALENGTH</c> of
///     <see cref="TextEqualityFilter" /> does not carry over, because no one length belongs to a list.
///     Padding stops at the last character though, so a value with a character after it cannot be padded
///     into another. As with that filter it is a residual: the plain <c>IN</c> stays in front for an index
///     to seek.
/// </remarks>
internal class InFilter : ISqlFragment
{
    private const string Sentinel = "N'.'";

    private readonly string _locator;
    private readonly IQueryableMember _member;
    private readonly IList _values;
    private readonly string? _exact;

    public InFilter(string locator, IQueryableMember member, IList values)
    {
        _locator = locator;
        _member = member;
        _values = values;

        if (member.TextCapacity != null) _exact = member.ExactTextLocator(locator);
    }

    public void Apply(ICommandBuilder builder)
    {
        if (_values.Count == 0)
        {
            builder.Append("1=0"); // No values → always false
            return;
        }

        if (_exact != null) builder.Append("(");

        // #710: SQL Server rejects a command with more than 2100 parameters, so a large list cannot
        // travel as one parameter per value -- it threw rather than running. Above the threshold the
        // whole list goes as ONE JSON array parameter instead, which is what the by-id paths have
        // done since #363 and why LoadManyAsync worked over a list this could not.
        //
        // The locator's own SQL type is what the unpacked column is typed to; see
        // JsonValueList.AppendInClause for why reading it beats inferring it in both directions.
        if (JsonValueList.ShouldBindAsJsonArray(builder, _values.Count))
        {
            // #835: the type read off the member describes TypedLocator. When this list is compared
            // through the full read instead, the unpacked column has to be as wide as that read, or
            // the very value that sent it there comes back out of OPENJSON truncated or NULL.
            var sqlType = _locator == _member.TypedLocator ? _member.LocatorSqlType : "nvarchar(max)";

            var list = JsonValueList.AppendInClause(builder, _locator, _values, sqlType, _member.ConvertValue);

            if (_exact != null)
            {
                builder.Append($" AND {_exact} + {Sentinel} IN (SELECT [value] + {Sentinel} FROM OPENJSON(@{list}) "
                               + "WITH ([value] nvarchar(max) '$')))");
            }

            return;
        }

        builder.Append(_locator);
        builder.Append(" IN (");

        var names = new string[_values.Count];
        for (var i = 0; i < _values.Count; i++)
        {
            if (i > 0) builder.Append(", ");
            names[i] = builder.AppendParameter(_member.ConvertValue(_values[i])!).ParameterName.TrimStart('@');
        }

        builder.Append(")");

        if (_exact != null)
        {
            var values = string.Join(", ", names.Select(x => $"@{x} + {Sentinel}"));
            builder.Append($" AND {_exact} + {Sentinel} IN ({values}))");
        }
    }
}
