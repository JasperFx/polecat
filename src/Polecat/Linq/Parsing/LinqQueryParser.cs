using System.Linq.Expressions;
using Polecat.Linq.Joins;
using Polecat.Linq.Members;
using Polecat.Linq.Metadata;
using Polecat.Linq.SoftDeletes;
using Polecat.Linq.SqlGeneration;

// ReSharper disable ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract

namespace Polecat.Linq.Parsing;

/// <summary>
///     Visits a LINQ expression tree and builds a Statement for SQL generation.
/// </summary>
internal class LinqQueryParser : ExpressionVisitor
{
    private readonly IMemberResolver _memberFactory;
    private readonly WhereClauseParser _whereParser;

    public Statement Statement { get; }
    public SingleValueMode? ValueMode { get; private set; }

    /// <summary>
    ///     If a GroupJoin+SelectMany pattern was detected, holds the parsed join data.
    /// </summary>
    public GroupJoinData? GroupJoinData { get; private set; }

    /// <summary>
    ///     For aggregation methods (Sum, Min, Max, Average), the member selector expression.
    /// </summary>
    public IQueryableMember? AggregationMember { get; private set; }

    /// <summary>
    ///     The Select() lambda, if present. Used for projections.
    /// </summary>
    public LambdaExpression? SelectExpression { get; private set; }

    /// <summary>
    ///     If GroupBy was detected, the key selector lambda.
    /// </summary>
    public LambdaExpression? GroupByKeySelector { get; private set; }

    /// <summary>
    ///     HAVING expressions collected from Where() after GroupBy().
    /// </summary>
    public List<Expression> GroupByHavingExpressions { get; } = [];

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

    /// <summary>
    ///     Whether MaybeDeleted() was called, suppressing the soft delete filter entirely.
    /// </summary>
    public bool IsMaybeDeleted { get; private set; }

    /// <summary>
    ///     Whether IsDeleted() was called, switching the filter to is_deleted = 1.
    /// </summary>
    public bool IsDeletedOnly { get; private set; }

    /// <summary>
    ///     If DeletedSince() was called, the timestamp to filter by.
    /// </summary>
    public DateTimeOffset? DeletedSinceTimestamp { get; private set; }

    /// <summary>
    ///     If DeletedBefore() was called, the timestamp to filter by.
    /// </summary>
    public DateTimeOffset? DeletedBeforeTimestamp { get; private set; }

    /// <summary>
    ///     If ModifiedSince() was called, the timestamp to filter by.
    /// </summary>
    public DateTimeOffset? ModifiedSinceTimestamp { get; private set; }

    /// <summary>
    ///     If ModifiedBefore() was called, the timestamp to filter by.
    /// </summary>
    public DateTimeOffset? ModifiedBeforeTimestamp { get; private set; }

    /// <summary>
    ///     If CreatedSince() was called, the timestamp to filter by.
    /// </summary>
    public DateTimeOffset? CreatedSinceTimestamp { get; private set; }

    /// <summary>
    ///     If CreatedBefore() was called, the timestamp to filter by.
    /// </summary>
    public DateTimeOffset? CreatedBeforeTimestamp { get; private set; }

    /// <summary>
    ///     If QueryForNonStaleData() was called, the timeout for waiting.
    ///     Null means not called, TimeSpan.Zero means use default (5s).
    /// </summary>
    public TimeSpan? NonStaleDataTimeout { get; private set; }

    public LinqQueryParser(IMemberResolver memberFactory, string fromTable)
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
        // For GroupJoin, handle the outer source visit internally and set GroupJoinData.
        // We must not let the default Arguments[0] visit run, since HandleGroupJoin does it.
        if (node.Method.Name == "GroupJoin")
        {
            HandleGroupJoin(node);
            return node;
        }

        // Visit the source (first argument for Queryable extension methods)
        if (node.Arguments.Count > 0)
        {
            Visit(node.Arguments[0]);
        }

        switch (node.Method.Name)
        {
            // SelectMany after GroupJoin: capture selectors for the join pattern.
            // GroupJoinData is set by visiting Arguments[0] above (the GroupJoin node).
            case "SelectMany" when GroupJoinData != null:
                HandleSelectManyAfterGroupJoin(node);
                return node;
            case "GroupBy":
                HandleGroupBy(node);
                break;
            case "Where" when GroupByKeySelector != null:
                HandleGroupByWhere(node);
                break;
            case "Where":
                HandleWhere(node);
                break;
            case "OrderBy":
                if (GroupJoinData != null)
                    HandleGroupJoinOrderBy(node, descending: false, replace: true);
                else
                    HandleOrderBy(node, descending: false, replace: true);
                break;
            case "OrderByDescending":
                if (GroupJoinData != null)
                    HandleGroupJoinOrderBy(node, descending: true, replace: true);
                else
                    HandleOrderBy(node, descending: true, replace: true);
                break;
            case "ThenBy":
                if (GroupJoinData != null)
                    HandleGroupJoinOrderBy(node, descending: false, replace: false);
                else
                    HandleOrderBy(node, descending: false, replace: false);
                break;
            case "ThenByDescending":
                if (GroupJoinData != null)
                    HandleGroupJoinOrderBy(node, descending: true, replace: false);
                else
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
            case "Select" when GroupByKeySelector != null:
                // For GroupBy queries, just store the select expression;
                // the GroupBy execution path in the provider handles projection.
                SelectExpression = GetLambda(node.Arguments[1]);
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
            case "MaybeDeleted" when node.Method.DeclaringType == typeof(SoftDeletedExtensions):
                IsMaybeDeleted = true;
                break;
            case "IsDeleted" when node.Method.DeclaringType == typeof(SoftDeletedExtensions):
                IsDeletedOnly = true;
                break;
            case "DeletedSince" when node.Method.DeclaringType == typeof(SoftDeletedExtensions):
                HandleDeletedSince(node);
                break;
            case "DeletedBefore" when node.Method.DeclaringType == typeof(SoftDeletedExtensions):
                HandleDeletedBefore(node);
                break;
            case "ModifiedSince" when node.Method.DeclaringType == typeof(MetadataExtensions):
                HandleModifiedSince(node);
                break;
            case "ModifiedBefore" when node.Method.DeclaringType == typeof(MetadataExtensions):
                HandleModifiedBefore(node);
                break;
            case "CreatedSince" when node.Method.DeclaringType == typeof(CreatedAtExtensions):
                HandleCreatedSince(node);
                break;
            case "CreatedBefore" when node.Method.DeclaringType == typeof(CreatedAtExtensions):
                HandleCreatedBefore(node);
                break;
            case "QueryForNonStaleData" when node.Method.DeclaringType == typeof(NonStaleDataExtensions):
                HandleQueryForNonStaleData(node);
                break;
        }

        return node;
    }

    private void HandleGroupJoin(MethodCallExpression node)
    {
        // GroupJoin(outer, inner, outerKeySelector, innerKeySelector, resultSelector)
        // Arguments: [0]=outer source, [1]=inner source, [2]=outerKey, [3]=innerKey, [4]=resultSelector

        // Visit the outer source first (for any Where clauses applied before GroupJoin)
        Visit(node.Arguments[0]);

        var outerType = node.Method.GetGenericArguments()[0];
        var innerType = node.Method.GetGenericArguments()[1];

        GroupJoinData = new GroupJoinData
        {
            InnerSourceExpression = node.Arguments[1],
            OuterKeySelector = GetLambda(node.Arguments[2]),
            InnerKeySelector = GetLambda(node.Arguments[3]),
            GroupJoinResultSelector = GetLambda(node.Arguments[4]),
            OuterElementType = outerType,
            InnerElementType = innerType
        };
    }

    private void HandleSelectManyAfterGroupJoin(MethodCallExpression node)
    {
        // SelectMany(source, collectionSelector, resultSelector)
        // Arguments: [0]=source (the GroupJoin result), [1]=collectionSelector, [2]=resultSelector

        var collectionSelector = GetLambda(node.Arguments[1]);
        GroupJoinData!.SelectManyCollectionSelector = collectionSelector;

        if (node.Arguments.Count > 2)
        {
            GroupJoinData.SelectManyResultSelector = GetLambda(node.Arguments[2]);
        }

        // Detect DefaultIfEmpty in the collection selector body to determine LEFT JOIN
        GroupJoinData.IsLeftJoin = ContainsDefaultIfEmpty(collectionSelector.Body);
    }

    private static bool ContainsDefaultIfEmpty(Expression expression)
    {
        if (expression is MethodCallExpression methodCall)
        {
            if (methodCall.Method.Name == "DefaultIfEmpty")
                return true;

            // Check nested calls
            foreach (var arg in methodCall.Arguments)
            {
                if (ContainsDefaultIfEmpty(arg))
                    return true;
            }
        }

        return false;
    }

    private void HandleGroupJoinOrderBy(MethodCallExpression node, bool descending, bool replace)
    {
        if (replace)
        {
            GroupJoinData!.OrderByExpressions.Clear();
        }

        var lambda = GetLambda(node.Arguments[1]);
        GroupJoinData!.OrderByExpressions.Add((lambda, descending));
    }

    private void HandleGroupBy(MethodCallExpression node)
    {
        GroupByKeySelector = GetLambda(node.Arguments[1]);
    }

    private void HandleGroupByWhere(MethodCallExpression node)
    {
        // Where() after GroupBy() becomes a HAVING clause.
        // Store the expression body for later resolution in the provider.
        var predicate = GetLambda(node.Arguments[1]);
        GroupByHavingExpressions.Add(predicate.Body);
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

    private void HandleDeletedSince(MethodCallExpression node)
    {
        IsDeletedOnly = true;
        var value = WhereClauseParser.ExtractValue(node.Arguments[1]);
        DeletedSinceTimestamp = (DateTimeOffset)value;
    }

    private void HandleDeletedBefore(MethodCallExpression node)
    {
        IsDeletedOnly = true;
        var value = WhereClauseParser.ExtractValue(node.Arguments[1]);
        DeletedBeforeTimestamp = (DateTimeOffset)value;
    }

    private void HandleModifiedSince(MethodCallExpression node)
    {
        var value = WhereClauseParser.ExtractValue(node.Arguments[1]);
        ModifiedSinceTimestamp = (DateTimeOffset)value;
    }

    private void HandleModifiedBefore(MethodCallExpression node)
    {
        var value = WhereClauseParser.ExtractValue(node.Arguments[1]);
        ModifiedBeforeTimestamp = (DateTimeOffset)value;
    }

    private void HandleCreatedSince(MethodCallExpression node)
    {
        var value = WhereClauseParser.ExtractValue(node.Arguments[1]);
        CreatedSinceTimestamp = (DateTimeOffset)value;
    }

    private void HandleCreatedBefore(MethodCallExpression node)
    {
        var value = WhereClauseParser.ExtractValue(node.Arguments[1]);
        CreatedBeforeTimestamp = (DateTimeOffset)value;
    }

    private void HandleQueryForNonStaleData(MethodCallExpression node)
    {
        if (node.Arguments.Count > 1)
        {
            var value = WhereClauseParser.ExtractValue(node.Arguments[1]);
            NonStaleDataTimeout = (TimeSpan)value;
        }
        else
        {
            NonStaleDataTimeout = TimeSpan.FromSeconds(5); // default
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

    internal static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }
}
