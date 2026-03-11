using System.Linq.Expressions;
using System.Reflection;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Events.Operations;

/// <summary>
///     Parses a predicate expression against IEvent properties into an ISqlFragment
///     for WHERE clauses against the pc_events table.
/// </summary>
internal class EventWhereClauseParser
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

    private static readonly Dictionary<string, (string column, string sqlType, Type clrType)> EventColumns = new()
    {
        { "Id", ("id", "uniqueidentifier", typeof(Guid)) },
        { "Sequence", ("seq_id", "bigint", typeof(long)) },
        { "StreamId", ("stream_id", "uniqueidentifier", typeof(Guid)) },
        { "StreamKey", ("stream_id", "nvarchar(250)", typeof(string)) },
        { "Version", ("version", "bigint", typeof(long)) },
        { "Timestamp", ("timestamp", "datetimeoffset", typeof(DateTimeOffset)) },
        { "EventTypeName", ("type", "nvarchar(250)", typeof(string)) },
        { "DotNetTypeName", ("dotnet_type", "nvarchar(500)", typeof(string)) },
        { "IsArchived", ("is_archived", "bit", typeof(bool)) },
        { "CorrelationId", ("correlation_id", "nvarchar(250)", typeof(string)) },
        { "CausationId", ("causation_id", "nvarchar(250)", typeof(string)) }
    };

    public ISqlFragment Parse(Expression expression)
    {
        return expression switch
        {
            BinaryExpression binary => ParseBinary(binary),
            UnaryExpression { NodeType: ExpressionType.Not } unary => ParseNot(unary),
            UnaryExpression { NodeType: ExpressionType.Convert } unary => Parse(unary.Operand),
            MemberExpression member when IsBooleanEventMember(member) => ParseBooleanMember(member, true),
            _ => throw new NotSupportedException(
                $"Unsupported expression type in event WHERE clause: {expression.NodeType} ({expression.GetType().Name})")
        };
    }

    private ISqlFragment ParseBinary(BinaryExpression binary)
    {
        if (binary.NodeType == ExpressionType.AndAlso)
            return new CompoundWhereFragment("AND", Parse(binary.Left), Parse(binary.Right));

        if (binary.NodeType == ExpressionType.OrElse)
            return new CompoundWhereFragment("OR", Parse(binary.Left), Parse(binary.Right));

        if (Operators.TryGetValue(binary.NodeType, out var op))
        {
            return ParseComparison(binary, op);
        }

        throw new NotSupportedException($"Unsupported binary operator in event WHERE: {binary.NodeType}");
    }

    private ISqlFragment ParseComparison(BinaryExpression binary, string op)
    {
        if (TryResolveEventMemberAndValue(binary.Left, binary.Right, out var member, out var value))
        {
            return BuildComparisonFilter(member!, value, op);
        }

        if (TryResolveEventMemberAndValue(binary.Right, binary.Left, out member, out value))
        {
            op = ReverseOperator(op);
            return BuildComparisonFilter(member!, value, op);
        }

        throw new NotSupportedException($"Cannot resolve event comparison: {binary}");
    }

    private ISqlFragment BuildComparisonFilter(IQueryableMember member, object? value, string op)
    {
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
        if (unary.Operand is MemberExpression member && IsBooleanEventMember(member))
        {
            return ParseBooleanMember(member, false);
        }

        var inner = Parse(unary.Operand);
        return new NotFragment(inner);
    }

    private ISqlFragment ParseBooleanMember(MemberExpression memberExpr, bool expectedValue)
    {
        var member = ResolveEventMember(memberExpr);
        var value = expectedValue ? 1 : 0;
        return new ComparisonFilter(member.TypedLocator, "=", value);
    }

    private bool TryResolveEventMemberAndValue(Expression memberExpr, Expression valueExpr,
        out IQueryableMember? member, out object? value)
    {
        member = null;
        value = null;

        var unwrapped = StripConvert(memberExpr);

        if (unwrapped is MemberExpression me && IsEventMember(me))
        {
            member = ResolveEventMember(me);
            value = ExtractValue(valueExpr);
            return true;
        }

        return false;
    }

    private IQueryableMember ResolveEventMember(MemberExpression expression)
    {
        var propName = expression.Member.Name;

        if (!EventColumns.TryGetValue(propName, out var mapping))
        {
            throw new NotSupportedException(
                $"IEvent property '{propName}' is not supported in event WHERE clauses.");
        }

        var (column, sqlType, clrType) = mapping;
        var isBoolean = clrType == typeof(bool);
        return new QueryableMember(column, column, clrType, isBoolean);
    }

    private static bool IsEventMember(MemberExpression expression)
    {
        var current = expression;
        while (current != null)
        {
            if (current.Expression is ParameterExpression)
                return true;
            current = current.Expression as MemberExpression;
        }

        return false;
    }

    private static bool IsBooleanEventMember(MemberExpression expression)
    {
        var memberType = expression.Member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => null
        };

        return memberType == typeof(bool) && IsEventMember(expression);
    }

    private static object? ExtractValue(Expression expression)
    {
        expression = StripConvert(expression);

        if (expression is ConstantExpression constant)
            return constant.Value;

        if (expression is MemberExpression { Expression: ConstantExpression closureObj } memberExpr)
        {
            return memberExpr.Member switch
            {
                FieldInfo field => field.GetValue(closureObj.Value),
                PropertyInfo prop => prop.GetValue(closureObj.Value),
                _ => CompileAndInvoke(expression)
            };
        }

        return CompileAndInvoke(expression);
    }

    private static object? CompileAndInvoke(Expression expression)
    {
        var lambda = Expression.Lambda(expression);
        var compiled = lambda.Compile();
        return compiled.DynamicInvoke();
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
            _ => op
        };
    }
}
