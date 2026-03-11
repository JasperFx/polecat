using System.Linq.Expressions;
using System.Text.Json;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;
using Polecat.Serialization;
using Weasel.SqlServer;

namespace Polecat.Linq.Parsing;

/// <summary>
///     Builds JSON_OBJECT SELECT clause, GROUP BY columns, and HAVING fragments
///     from a GroupBy key selector and result projection.
/// </summary>
internal class GroupBySelectBuilder
{
    private readonly MemberFactory _memberFactory;
    private readonly JsonNamingPolicy? _namingPolicy;

    // Key members for GROUP BY (maps anonymous type member name to SQL locator)
    private readonly Dictionary<string, string> _keyLocators = new();
    private string? _simpleKeyLocator;
    private bool _isCompositeKey;

    public List<string> GroupByColumns { get; } = [];
    public string SelectColumns { get; private set; } = "data";

    public GroupBySelectBuilder(MemberFactory memberFactory, StoreOptions options)
    {
        _memberFactory = memberFactory;

        if (options.Serializer is Serializer s)
        {
            _namingPolicy = s.Options.PropertyNamingPolicy;
        }
        else
        {
            _namingPolicy = JsonNamingPolicy.CamelCase;
        }
    }

    public void Build(LambdaExpression keySelector, LambdaExpression selectExpression)
    {
        ParseKeySelector(keySelector);
        BuildSelectFromProjection(selectExpression);
    }

    public void BuildScalarKeySelect(LambdaExpression keySelector)
    {
        ParseKeySelector(keySelector);
        // For .Select(g => g.Key) - just return the key locator
        SelectColumns = _simpleKeyLocator ?? GroupByColumns[0];
    }

    private void ParseKeySelector(LambdaExpression keySelector)
    {
        var body = LinqQueryParser.StripConvert(keySelector.Body);

        if (body is NewExpression newExpr)
        {
            _isCompositeKey = true;
            var parameters = newExpr.Constructor!.GetParameters();
            for (var i = 0; i < parameters.Length; i++)
            {
                var memberExpr = newExpr.Arguments[i] as MemberExpression
                    ?? throw new NotSupportedException(
                        $"GroupBy composite key member must be a property, got: {newExpr.Arguments[i]}");
                var member = _memberFactory.ResolveMember(memberExpr);
                _keyLocators[parameters[i].Name!] = member.TypedLocator;
                GroupByColumns.Add(member.TypedLocator);
            }
        }
        else if (body is MemberExpression memberBody)
        {
            _isCompositeKey = false;
            var member = _memberFactory.ResolveMember(memberBody);
            _simpleKeyLocator = member.TypedLocator;
            GroupByColumns.Add(member.TypedLocator);
        }
        else
        {
            throw new NotSupportedException($"Unsupported GroupBy key expression: {body}");
        }
    }

    private void BuildSelectFromProjection(LambdaExpression selectExpression)
    {
        var body = selectExpression.Body;
        var groupingParam = selectExpression.Parameters[0];

        // Check for scalar select: g => g.Key or g => g.Count()
        if (body is MemberExpression memberAccess && memberAccess.Member.Name == "Key"
            && memberAccess.Expression == groupingParam)
        {
            SelectColumns = _simpleKeyLocator ?? GroupByColumns[0];
            return;
        }

        if (body is MethodCallExpression scalarMethod)
        {
            var sql = ResolveAggregate(scalarMethod, groupingParam);
            if (sql != null)
            {
                SelectColumns = sql;
                return;
            }
        }

        // Complex projection: build JSON_OBJECT
        if (body is NewExpression newExpr)
        {
            SelectColumns = BuildJsonObject(newExpr, groupingParam);
            return;
        }

        if (body is MemberInitExpression memberInit)
        {
            SelectColumns = BuildJsonObjectFromMemberInit(memberInit, groupingParam);
            return;
        }

        throw new NotSupportedException($"Unsupported GroupBy Select expression: {body}");
    }

    private string BuildJsonObject(NewExpression newExpr, ParameterExpression groupingParam)
    {
        var parameters = newExpr.Constructor!.GetParameters();
        var parts = new List<string>();

        for (var i = 0; i < parameters.Length; i++)
        {
            var name = FormatKey(parameters[i].Name!);
            var sql = ResolveProjectionMember(newExpr.Arguments[i], groupingParam);
            parts.Add($"'{name}': {sql}");
        }

        return $"JSON_OBJECT({string.Join(", ", parts)}) as data";
    }

    private string BuildJsonObjectFromMemberInit(MemberInitExpression memberInit, ParameterExpression groupingParam)
    {
        var parts = new List<string>();

        // Handle constructor parameters
        var ctorParams = memberInit.NewExpression.Constructor!.GetParameters();
        for (var i = 0; i < ctorParams.Length; i++)
        {
            var name = FormatKey(ctorParams[i].Name!);
            var sql = ResolveProjectionMember(memberInit.NewExpression.Arguments[i], groupingParam);
            parts.Add($"'{name}': {sql}");
        }

        // Handle member bindings
        foreach (var binding in memberInit.Bindings.OfType<MemberAssignment>())
        {
            var name = FormatKey(binding.Member.Name);
            var sql = ResolveProjectionMember(binding.Expression, groupingParam);
            parts.Add($"'{name}': {sql}");
        }

        return $"JSON_OBJECT({string.Join(", ", parts)}) as data";
    }

    private string ResolveProjectionMember(Expression expr, ParameterExpression groupingParam)
    {
        // g.Key
        if (expr is MemberExpression memberAccess && memberAccess.Member.Name == "Key"
            && memberAccess.Expression == groupingParam)
        {
            if (_isCompositeKey)
            {
                throw new NotSupportedException(
                    "Cannot select the entire composite GroupBy key directly. Access individual key members like g.Key.Color instead.");
            }

            return _simpleKeyLocator!;
        }

        // g.Key.PropertyName (composite key)
        if (expr is MemberExpression compositeAccess
            && compositeAccess.Expression is MemberExpression innerMember
            && innerMember.Member.Name == "Key"
            && innerMember.Expression == groupingParam)
        {
            var propName = compositeAccess.Member.Name;
            if (_keyLocators.TryGetValue(propName, out var locator))
            {
                return locator;
            }

            throw new NotSupportedException(
                $"Unknown composite key member '{propName}' in GroupBy projection");
        }

        // Aggregate method calls
        if (expr is MethodCallExpression methodCall)
        {
            var sql = ResolveAggregate(methodCall, groupingParam);
            if (sql != null) return sql;
        }

        throw new NotSupportedException(
            $"Unsupported expression in GroupBy projection: {expr}");
    }

    private string? ResolveAggregate(MethodCallExpression method, ParameterExpression groupingParam)
    {
        var name = method.Method.Name;

        // g.Count() / g.LongCount()
        if (name is "Count" or "LongCount" && method.Arguments.Count == 1
            && method.Arguments[0] == groupingParam)
        {
            return name == "LongCount" ? "CAST(COUNT(*) AS bigint)" : "COUNT(*)";
        }

        // g.Count(predicate) - count with predicate via CASE WHEN
        if (name is "Count" or "LongCount" && method.Arguments.Count == 2
            && method.Arguments[0] == groupingParam)
        {
            // Not supported for now - complex predicate translation
            return "COUNT(*)";
        }

        // g.Sum(x => x.Prop) / g.Min() / g.Max() / g.Average()
        if (name is "Sum" or "Min" or "Max" or "Average" && method.Arguments.Count == 2
            && method.Arguments[0] == groupingParam)
        {
            var lambda = LinqQueryParser.GetLambda(method.Arguments[1]);
            var body = LinqQueryParser.StripConvert(lambda.Body);
            if (body is MemberExpression memberExpr)
            {
                var member = _memberFactory.ResolveMember(memberExpr);
                return name switch
                {
                    "Sum" => $"ISNULL(SUM({member.TypedLocator}), 0)",
                    "Min" => $"MIN({member.TypedLocator})",
                    "Max" => $"MAX({member.TypedLocator})",
                    "Average" => $"AVG(CAST({member.TypedLocator} AS float))",
                    _ => null
                };
            }
        }

        return null;
    }

    /// <summary>
    ///     Resolves a HAVING expression (from Where() after GroupBy()) to an ISqlFragment.
    /// </summary>
    public ISqlFragment ResolveHaving(Expression expression, ParameterExpression groupingParam)
    {
        if (expression is BinaryExpression binary)
        {
            return ResolveHavingBinary(binary, groupingParam);
        }

        throw new NotSupportedException(
            $"Unsupported HAVING expression type: {expression.NodeType}");
    }

    private ISqlFragment ResolveHavingBinary(BinaryExpression binary, ParameterExpression groupingParam)
    {
        if (binary.NodeType == ExpressionType.AndAlso)
        {
            var left = ResolveHaving(binary.Left, groupingParam);
            var right = ResolveHaving(binary.Right, groupingParam);
            return new CompoundHavingFragment("AND", left, right);
        }

        if (binary.NodeType == ExpressionType.OrElse)
        {
            var left = ResolveHaving(binary.Left, groupingParam);
            var right = ResolveHaving(binary.Right, groupingParam);
            return new CompoundHavingFragment("OR", left, right);
        }

        var op = binary.NodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => throw new NotSupportedException(
                $"Unsupported comparison in HAVING: {binary.NodeType}")
        };

        var leftSql = ResolveHavingOperand(binary.Left, groupingParam);
        var rightSql = ResolveHavingOperand(binary.Right, groupingParam);

        return new LiteralSqlFragment($"{leftSql} {op} {rightSql}");
    }

    private string ResolveHavingOperand(Expression expr, ParameterExpression groupingParam)
    {
        if (expr is MethodCallExpression method)
        {
            var sql = ResolveAggregate(method, groupingParam);
            if (sql != null) return sql;
        }

        if (expr is ConstantExpression constant)
        {
            return constant.Value?.ToString() ?? "NULL";
        }

        // Try to evaluate as constant
        try
        {
            var value = Expression.Lambda(expr).Compile().DynamicInvoke();
            return value?.ToString() ?? "NULL";
        }
        catch
        {
            throw new NotSupportedException(
                $"Unsupported HAVING operand: {expr}");
        }
    }

    private string FormatKey(string name)
    {
        return _namingPolicy?.ConvertName(name) ?? name;
    }
}

internal class CompoundHavingFragment : ISqlFragment
{
    private readonly string _separator;
    private readonly ISqlFragment _left;
    private readonly ISqlFragment _right;

    public CompoundHavingFragment(string separator, ISqlFragment left, ISqlFragment right)
    {
        _separator = separator;
        _left = left;
        _right = right;
    }

    public void Apply(ICommandBuilder builder)
    {
        builder.Append("(");
        _left.Apply(builder);
        builder.Append($" {_separator} ");
        _right.Apply(builder);
        builder.Append(")");
    }
}
