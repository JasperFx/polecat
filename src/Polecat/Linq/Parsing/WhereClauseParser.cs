using System.Linq.Expressions;
using System.Reflection;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing.Methods;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing;

/// <summary>
///     Parses a predicate expression tree into an ISqlFragment for the WHERE clause.
/// </summary>
internal class WhereClauseParser
{
    private static readonly Dictionary<ExpressionType, string> Operators = new()
    {
        { ExpressionType.Equal, "=" },
        { ExpressionType.NotEqual, "!=" },
        { ExpressionType.GreaterThan, ">" },
        { ExpressionType.GreaterThanOrEqual, ">=" },
        { ExpressionType.LessThan, "<" },
        { ExpressionType.LessThanOrEqual, "<=" }
    };

    private readonly MemberFactory _memberFactory;

    public WhereClauseParser(MemberFactory memberFactory)
    {
        _memberFactory = memberFactory;
    }

    public ISqlFragment Parse(Expression expression)
    {
        return expression switch
        {
            BinaryExpression binary => ParseBinary(binary),
            UnaryExpression { NodeType: ExpressionType.Not } unary => ParseNot(unary),
            UnaryExpression { NodeType: ExpressionType.Convert } unary => Parse(unary.Operand),
            MethodCallExpression methodCall => ParseMethodCall(methodCall),
            MemberExpression member when IsBooleanMember(member) => ParseBooleanMember(member, true),
            ConstantExpression { Value: bool boolValue } => boolValue
                ? new WhereFragment("1=1")
                : new WhereFragment("1=0"),
            _ => throw new NotSupportedException(
                $"Unsupported expression type in WHERE clause: {expression.NodeType} ({expression.GetType().Name})")
        };
    }

    private ISqlFragment ParseMethodCall(MethodCallExpression expression)
    {
        var parser = MethodCallParserRegistry.FindParser(expression)
            ?? throw new NotSupportedException(
                $"Unsupported method call in WHERE clause: {expression.Method.DeclaringType?.Name}.{expression.Method.Name}");

        return parser.Parse(_memberFactory, expression);
    }

    private ISqlFragment ParseBinary(BinaryExpression binary)
    {
        // Logical operators: AND, OR
        if (binary.NodeType == ExpressionType.AndAlso)
            return new CompoundWhereFragment("AND", Parse(binary.Left), Parse(binary.Right));

        if (binary.NodeType == ExpressionType.OrElse)
            return new CompoundWhereFragment("OR", Parse(binary.Left), Parse(binary.Right));

        // Comparison operators: ==, !=, <, >, <=, >=
        if (Operators.TryGetValue(binary.NodeType, out var op))
        {
            return ParseComparison(binary, op);
        }

        throw new NotSupportedException($"Unsupported binary operator: {binary.NodeType}");
    }

    private ISqlFragment ParseComparison(BinaryExpression binary, string op)
    {
        // Try to resolve: left=member, right=value
        if (TryResolveMemberAndValue(binary.Left, binary.Right, out var member, out var value))
        {
            return BuildComparisonFilter(member!, value, op);
        }

        // Try reversed: left=value, right=member
        if (TryResolveMemberAndValue(binary.Right, binary.Left, out member, out value))
        {
            // Reverse the operator for reversed operands
            op = ReverseOperator(op);
            return BuildComparisonFilter(member!, value, op);
        }

        throw new NotSupportedException(
            $"Cannot resolve comparison: {binary}");
    }

    private ISqlFragment BuildComparisonFilter(IQueryableMember member, object? value, string op)
    {
        // Handle null comparisons
        if (value == null)
        {
            return op == "="
                ? new WhereFragment($"{member.RawLocator} IS NULL")
                : new WhereFragment($"{member.RawLocator} IS NOT NULL");
        }

        var convertedValue = member.ConvertValue(value);
        return new ComparisonFilter(member.TypedLocator, op, convertedValue!);
    }

    private ISqlFragment ParseNot(UnaryExpression unary)
    {
        var operand = unary.Operand;

        // !x.IsActive
        if (operand is MemberExpression member && IsBooleanMember(member))
        {
            return ParseBooleanMember(member, false);
        }

        // !(expression) — negate the inner expression
        var inner = Parse(operand);
        return new NotFragment(inner);
    }

    private ISqlFragment ParseBooleanMember(MemberExpression memberExpr, bool expectedValue)
    {
        var member = _memberFactory.ResolveMember(memberExpr);
        var value = member.ConvertValue(expectedValue);
        return new ComparisonFilter(member.TypedLocator, "=", value!);
    }

    private bool TryResolveMemberAndValue(Expression memberExpr, Expression valueExpr,
        out IQueryableMember? member, out object? value)
    {
        member = null;
        value = null;

        var unwrappedMember = StripConvert(memberExpr);

        if (unwrappedMember is MemberExpression me && IsDocumentMember(me))
        {
            member = _memberFactory.ResolveMember(me);
            value = ExtractValue(valueExpr);
            return true;
        }

        return false;
    }

    internal static object? ExtractValue(Expression expression)
    {
        expression = StripConvert(expression);

        // Direct constant
        if (expression is ConstantExpression constant)
            return constant.Value;

        // Closure variable: field on a captured closure object
        if (expression is MemberExpression { Expression: ConstantExpression closureObj } memberExpr)
        {
            return memberExpr.Member switch
            {
                FieldInfo field => field.GetValue(closureObj.Value),
                PropertyInfo prop => prop.GetValue(closureObj.Value),
                _ => CompileAndInvoke(expression)
            };
        }

        // Nested member access (e.g., obj.Property.Field)
        if (expression is MemberExpression nestedMember)
        {
            return CompileAndInvoke(expression);
        }

        // Array or other complex expression — compile and evaluate
        return CompileAndInvoke(expression);
    }

    private static object? CompileAndInvoke(Expression expression)
    {
        var lambda = Expression.Lambda(expression);
        var compiled = lambda.Compile();
        return compiled.DynamicInvoke();
    }

    private static bool IsDocumentMember(MemberExpression expression)
    {
        // Walk up the chain to find if it originates from a ParameterExpression
        var current = expression;
        while (current != null)
        {
            if (current.Expression is ParameterExpression)
                return true;

            current = current.Expression as MemberExpression;
        }

        return false;
    }

    private static bool IsBooleanMember(MemberExpression expression)
    {
        var memberType = expression.Member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => null
        };

        return memberType == typeof(bool) && IsDocumentMember(expression);
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    private static string ReverseOperator(string op)
    {
        return op switch
        {
            ">" => "<",
            ">=" => "<=",
            "<" => ">",
            "<=" => ">=",
            _ => op // = and != are symmetric
        };
    }
}
