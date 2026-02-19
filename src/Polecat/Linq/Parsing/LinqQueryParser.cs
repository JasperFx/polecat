using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing;

/// <summary>
///     Visits a LINQ expression tree and builds a Statement for SQL generation.
/// </summary>
internal class LinqQueryParser : ExpressionVisitor
{
    private readonly MemberFactory _memberFactory;
    private readonly WhereClauseParser _whereParser;

    public Statement Statement { get; }
    public SingleValueMode? ValueMode { get; private set; }

    /// <summary>
    ///     For aggregation methods (Sum, Min, Max, Average), the member selector expression.
    /// </summary>
    public IQueryableMember? AggregationMember { get; private set; }

    /// <summary>
    ///     The Select() lambda, if present. Used for projections.
    /// </summary>
    public LambdaExpression? SelectExpression { get; private set; }

    /// <summary>
    ///     Whether Distinct() was applied.
    /// </summary>
    public bool IsDistinct { get; private set; }

    /// <summary>
    ///     Whether AnyTenant() was called, suppressing the default tenant_id filter.
    /// </summary>
    public bool IsAnyTenant { get; private set; }

    /// <summary>
    ///     If TenantIsOneOf() was called, the tenant IDs to filter by.
    /// </summary>
    public string[]? TenantIds { get; private set; }

    public LinqQueryParser(MemberFactory memberFactory, string fromTable)
    {
        _memberFactory = memberFactory;
        _whereParser = new WhereClauseParser(memberFactory);
        Statement = new Statement { FromTable = fromTable };
    }

    public void Parse(Expression expression)
    {
        Visit(expression);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Visit the source (first argument for Queryable extension methods)
        if (node.Arguments.Count > 0)
        {
            Visit(node.Arguments[0]);
        }

        switch (node.Method.Name)
        {
            case "Where":
                HandleWhere(node);
                break;
            case "OrderBy":
                HandleOrderBy(node, descending: false, replace: true);
                break;
            case "OrderByDescending":
                HandleOrderBy(node, descending: true, replace: true);
                break;
            case "ThenBy":
                HandleOrderBy(node, descending: false, replace: false);
                break;
            case "ThenByDescending":
                HandleOrderBy(node, descending: true, replace: false);
                break;
            case "Take":
                HandleTake(node);
                break;
            case "Skip":
                HandleSkip(node);
                break;
            case "First":
                HandleSingleValue(node, SingleValueMode.First);
                break;
            case "FirstOrDefault":
                HandleSingleValue(node, SingleValueMode.FirstOrDefault);
                break;
            case "Single":
                HandleSingleValue(node, SingleValueMode.Single);
                break;
            case "SingleOrDefault":
                HandleSingleValue(node, SingleValueMode.SingleOrDefault);
                break;
            case "Last":
                HandleSingleValue(node, SingleValueMode.Last);
                break;
            case "LastOrDefault":
                HandleSingleValue(node, SingleValueMode.LastOrDefault);
                break;
            case "Count":
                HandleSingleValue(node, SingleValueMode.Count);
                break;
            case "LongCount":
                HandleSingleValue(node, SingleValueMode.LongCount);
                break;
            case "Any":
                HandleSingleValue(node, SingleValueMode.Any);
                break;
            case "Sum":
                HandleAggregation(node, SingleValueMode.Sum);
                break;
            case "Min":
                HandleAggregation(node, SingleValueMode.Min);
                break;
            case "Max":
                HandleAggregation(node, SingleValueMode.Max);
                break;
            case "Average":
                HandleAggregation(node, SingleValueMode.Average);
                break;
            case "Select":
                HandleSelect(node);
                break;
            case "Distinct":
                IsDistinct = true;
                break;
            case "AnyTenant" when node.Method.DeclaringType == typeof(LinqExtensions):
                IsAnyTenant = true;
                break;
            case "TenantIsOneOf" when node.Method.DeclaringType == typeof(LinqExtensions):
                HandleTenantIsOneOf(node);
                break;
        }

        return node;
    }

    private void HandleWhere(MethodCallExpression node)
    {
        var predicate = GetLambda(node.Arguments[1]);
        var fragment = _whereParser.Parse(predicate.Body);
        Statement.Wheres.Add(fragment);
    }

    private void HandleOrderBy(MethodCallExpression node, bool descending, bool replace)
    {
        if (replace)
        {
            Statement.OrderBys.Clear();
        }

        var lambda = GetLambda(node.Arguments[1]);
        var body = StripConvert(lambda.Body);

        // After Select(x => x.Name), OrderBy(x => x) has a ParameterExpression body
        if (body is ParameterExpression)
        {
            // Order by the current select columns (the projected scalar)
            Statement.OrderBys.Add((Statement.SelectColumns, descending));
            return;
        }

        var memberExpr = body as MemberExpression
            ?? throw new NotSupportedException($"OrderBy requires a member expression, got: {body}");

        var member = _memberFactory.ResolveMember(memberExpr);
        Statement.OrderBys.Add((member.TypedLocator, descending));
    }

    private void HandleTake(MethodCallExpression node)
    {
        Statement.Limit = (int)ExtractConstant(node.Arguments[1]);
    }

    private void HandleSkip(MethodCallExpression node)
    {
        Statement.Offset = (int)ExtractConstant(node.Arguments[1]);
    }

    private void HandleSelect(MethodCallExpression node)
    {
        SelectExpression = GetLambda(node.Arguments[1]);

        // For scalar Select (e.g., Select(x => x.Name)), change the select columns
        // to use JSON_VALUE for efficiency
        var body = SelectExpression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;

        if (body is MemberExpression memberExpr && IsDocumentMember(memberExpr))
        {
            var member = _memberFactory.ResolveMember(memberExpr);
            Statement.SelectColumns = member.TypedLocator;
            // Mark as scalar select so provider knows to use ScalarListHandler
            IsScalarSelect = true;
        }
        // For complex projections (anonymous types, DTOs), keep selecting 'data' column
        // and project in-memory after deserialization
    }

    private static bool IsDocumentMember(MemberExpression expression)
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

    /// <summary>
    ///     Whether the Select is a simple scalar member access.
    /// </summary>
    public bool IsScalarSelect { get; private set; }

    private void HandleSingleValue(MethodCallExpression node, SingleValueMode mode)
    {
        ValueMode = mode;

        // Some single-value operators can have an inline predicate: First(x => x.Age > 30)
        if (node.Arguments.Count > 1)
        {
            var predicate = GetLambda(node.Arguments[1]);
            var fragment = _whereParser.Parse(predicate.Body);
            Statement.Wheres.Add(fragment);
        }
    }

    private void HandleAggregation(MethodCallExpression node, SingleValueMode mode)
    {
        ValueMode = mode;

        // Aggregations have a selector: Sum(x => x.Age)
        if (node.Arguments.Count > 1)
        {
            var lambda = GetLambda(node.Arguments[1]);
            var memberExpr = StripConvert(lambda.Body) as MemberExpression
                ?? throw new NotSupportedException(
                    $"{mode} requires a member expression selector, got: {lambda.Body}");

            AggregationMember = _memberFactory.ResolveMember(memberExpr);
        }
    }

    private void HandleTenantIsOneOf(MethodCallExpression node)
    {
        // TenantIsOneOf(queryable, params string[] tenantIds)
        // The tenant IDs are the second argument (first is the source queryable)
        var value = WhereClauseParser.ExtractValue(node.Arguments[1]);
        if (value is string[] ids)
        {
            TenantIds = ids;
        }
        else
        {
            throw new NotSupportedException("TenantIsOneOf requires string[] tenant IDs");
        }
    }

    internal static LambdaExpression GetLambda(Expression expression)
    {
        // Strip Quote wrapper
        if (expression is UnaryExpression { NodeType: ExpressionType.Quote } unary)
        {
            return (LambdaExpression)unary.Operand;
        }

        return (LambdaExpression)expression;
    }

    private static object ExtractConstant(Expression expression)
    {
        expression = StripConvert(expression);

        if (expression is ConstantExpression constant)
            return constant.Value!;

        // Compile for complex expressions
        return Expression.Lambda(expression).Compile().DynamicInvoke()!;
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }
}
