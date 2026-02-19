using System.Collections;
using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

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

    public ISqlFragment Parse(MemberFactory memberFactory, MethodCallExpression expression)
    {
        // IsOneOf is an extension method: first arg is the member, second is the values
        var memberExpr = expression.Arguments[0];
        var valuesExpr = expression.Arguments[1];

        // Resolve the member
        var stripped = StripConvert(memberExpr);
        if (stripped is not MemberExpression me)
            throw new NotSupportedException($"IsOneOf/In requires a member expression, got: {stripped}");

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

        throw new NotSupportedException($"Cannot extract values from: {expression}");
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

    public void Apply(CommandBuilder builder)
    {
        if (_values.Count == 0)
        {
            builder.Append("1=0"); // No values → always false
            return;
        }

        builder.Append(_locator);
        builder.Append(" IN (");
        for (var i = 0; i < _values.Count; i++)
        {
            if (i > 0) builder.Append(", ");
            builder.AppendParameter(_member.ConvertValue(_values[i]));
        }

        builder.Append(")");
    }
}
