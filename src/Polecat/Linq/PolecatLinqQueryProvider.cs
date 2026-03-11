using System.Data.Common;
using System.Linq.Expressions;
using System.Text;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Linq.Joins;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Linq.QueryHandlers;
using Polecat.Linq.Selectors;
using Polecat.Linq.SqlGeneration;
using Polecat.Metadata;
using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Linq;

/// <summary>
///     IQueryProvider that translates LINQ expression trees into SQL Server queries.
///     All SQL execution routes through session's Polly-wrapped centralized methods.
/// </summary>
internal class PolecatLinqQueryProvider : IPolecatAsyncQueryProvider
{
    private readonly QuerySession _session;
    private readonly DocumentProviderRegistry _providers;
    private readonly DocumentTableEnsurer _tableEnsurer;

    public PolecatLinqQueryProvider(
        QuerySession session,
        DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer)
    {
        _session = session;
        _providers = providers;
        _tableEnsurer = tableEnsurer;
    }

    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = GetElementType(expression);
        var queryableType = typeof(PolecatLinqQueryable<>).MakeGenericType(elementType);
        return (IQueryable)Activator.CreateInstance(queryableType, this, expression)!;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new PolecatLinqQueryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        throw new NotSupportedException(
            "Polecat does not support synchronous LINQ execution. Use async methods (ToListAsync, etc.) instead.");
    }

    public TResult Execute<TResult>(Expression expression)
    {
        throw new NotSupportedException(
            "Polecat does not support synchronous LINQ execution. Use async methods (ToListAsync, etc.) instead.");
    }

    internal string BuildSql(Expression expression, string tenantId)
    {
        var statement = BuildStatement(expression, tenantId);
        var builder = new BatchBuilder();
        statement.Apply(builder);
        var batch = builder.Compile();
        return batch.BatchCommands[0].CommandText;
    }

    internal Statement BuildStatement(Expression expression, string tenantId)
    {
        var documentType = FindDocumentType(expression);
        var provider = _providers.GetProvider(documentType);

        var memberFactory = new MemberFactory(_session.Options, provider.Mapping);
        var parser = new LinqQueryParser(memberFactory, provider.Mapping.QualifiedTableName);
        parser.Parse(expression);

        ApplySingleValueMode(parser);

        if (parser.IsDistinct) parser.Statement.IsDistinct = true;

        if (!parser.IsAnyTenant)
        {
            if (parser.TenantIds != null)
            {
                parser.Statement.Wheres.Add(new TenantInFilter(parser.TenantIds));
            }
            else
            {
                parser.Statement.Wheres.Add(new ComparisonFilter("tenant_id", "=", tenantId));
            }
        }

        if (provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete && !parser.IsMaybeDeleted)
        {
            parser.Statement.Wheres.Add(new LiteralSqlFragment(parser.IsDeletedOnly ? "is_deleted = 1" : "is_deleted = 0"));
        }

        ApplyModifiedFilters(parser);

        return parser.Statement;
    }

    internal async Task<string> ExecuteJsonArrayAsync(Expression expression, CancellationToken token)
    {
        var documentType = FindDocumentType(expression);
        var provider = _providers.GetProvider(documentType);
        await _tableEnsurer.EnsureTableAsync(provider, token);

        var memberFactory = new MemberFactory(_session.Options, provider.Mapping);
        var parser = new LinqQueryParser(memberFactory, provider.Mapping.QualifiedTableName);
        parser.Parse(expression);

        // Force select only the data column
        parser.Statement.SelectColumns = "data";

        if (parser.IsDistinct) parser.Statement.IsDistinct = true;

        if (!parser.IsAnyTenant)
        {
            if (parser.TenantIds != null)
            {
                parser.Statement.Wheres.Add(new TenantInFilter(parser.TenantIds));
            }
            else
            {
                parser.Statement.Wheres.Add(new ComparisonFilter("tenant_id", "=", _session.TenantId));
            }
        }

        if (provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete && !parser.IsMaybeDeleted)
        {
            parser.Statement.Wheres.Add(new LiteralSqlFragment(parser.IsDeletedOnly ? "is_deleted = 1" : "is_deleted = 0"));
        }

        ApplyModifiedFilters(parser);

        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        parser.Statement.Apply(builder);
        builder.Compile();

        await using var reader = await _session.ExecuteReaderAsync(batch, token);

        var sb = new StringBuilder("[");
        var first = true;
        while (await reader.ReadAsync(token))
        {
            if (!first) sb.Append(',');
            sb.Append(reader.GetString(0));
            first = false;
        }

        sb.Append(']');
        return sb.ToString();
    }

    public async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken token)
    {
        var documentType = FindDocumentType(expression);
        var provider = _providers.GetProvider(documentType);
        await _tableEnsurer.EnsureTableAsync(provider, token);

        var memberFactory = new MemberFactory(_session.Options, provider.Mapping);
        var parser = new LinqQueryParser(memberFactory, provider.Mapping.QualifiedTableName);
        parser.Parse(expression);

        // Route to GroupJoin execution if detected
        if (parser.GroupJoinData != null)
        {
            return await ExecuteGroupJoinAsync<TResult>(parser, token);
        }

        // Route to GroupBy execution if detected
        if (parser.GroupByKeySelector != null)
        {
            return await ExecuteGroupByAsync<TResult>(parser, memberFactory, token);
        }

        // Wait for non-stale projection data if requested
        if (parser.NonStaleDataTimeout.HasValue)
        {
            await WaitForNonStaleDataAsync(parser.NonStaleDataTimeout.Value, token);
        }

        // Apply single-value mode to Statement
        ApplySingleValueMode(parser);

        // Apply Distinct
        if (parser.IsDistinct)
        {
            parser.Statement.IsDistinct = true;
        }

        // Add tenant filter (unless AnyTenant was called)
        if (!parser.IsAnyTenant)
        {
            if (parser.TenantIds != null)
            {
                // TenantIsOneOf: tenant_id IN (...)
                var tenantIn = new TenantInFilter(parser.TenantIds);
                parser.Statement.Wheres.Add(tenantIn);
            }
            else
            {
                parser.Statement.Wheres.Add(
                    new ComparisonFilter("tenant_id", "=", _session.TenantId));
            }
        }

        // Add soft delete filter for soft-deleted types (unless overridden by LINQ extensions)
        if (provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete && !parser.IsMaybeDeleted)
        {
            if (parser.IsDeletedOnly)
            {
                parser.Statement.Wheres.Add(new LiteralSqlFragment("is_deleted = 1"));
            }
            else
            {
                parser.Statement.Wheres.Add(new LiteralSqlFragment("is_deleted = 0"));
            }

            if (parser.DeletedSinceTimestamp.HasValue)
            {
                parser.Statement.Wheres.Add(new ComparisonFilter("deleted_at", ">=", parser.DeletedSinceTimestamp.Value));
            }

            if (parser.DeletedBeforeTimestamp.HasValue)
            {
                parser.Statement.Wheres.Add(new ComparisonFilter("deleted_at", "<", parser.DeletedBeforeTimestamp.Value));
            }
        }

        ApplyModifiedFilters(parser);

        // Adjust select columns for version syncing on IRevisioned/IVersioned types
        bool syncRevision = provider.Mapping.UseNumericRevisions;
        bool syncGuidVersion = provider.Mapping.UseOptimisticConcurrency;
        if (parser.Statement.SelectColumns == "data")
        {
            if (syncGuidVersion)
            {
                parser.Statement.SelectColumns = "data, version, guid_version";
            }
            else if (syncRevision)
            {
                parser.Statement.SelectColumns = "data, version";
            }
        }

        // Build SQL and execute via BatchBuilder/SqlBatch
        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        parser.Statement.Apply(builder);
        builder.Compile();

        await using var reader = await _session.ExecuteReaderAsync(batch, token);

        return await HandleResultAsync<TResult>(reader, parser, documentType, token, syncRevision, syncGuidVersion);
    }

    private async Task<TResult> ExecuteGroupByAsync<TResult>(
        LinqQueryParser parser, MemberFactory memberFactory, CancellationToken token)
    {
        var builder2 = new GroupBySelectBuilder(memberFactory, _session.Options);

        if (parser.SelectExpression != null)
        {
            builder2.Build(parser.GroupByKeySelector!, parser.SelectExpression);
        }
        else
        {
            throw new NotSupportedException(
                "GroupBy must be followed by a Select() projection.");
        }

        // Apply GROUP BY columns
        foreach (var col in builder2.GroupByColumns)
        {
            parser.Statement.GroupByColumns.Add(col);
        }

        // Apply SELECT columns
        parser.Statement.SelectColumns = builder2.SelectColumns;

        // Apply HAVING clauses
        if (parser.GroupByHavingExpressions.Count > 0)
        {
            // We need the grouping parameter from the Where lambda to resolve aggregates.
            // The expressions were stored as lambda bodies; find the grouping parameter.
            // The parameter comes from the original Where() lambda, but we only stored the body.
            // We need to find the IGrouping parameter by examining the expression tree.
            var groupingParam = FindGroupingParameterInExpression(parser.GroupByHavingExpressions[0]);

            foreach (var havingExpr in parser.GroupByHavingExpressions)
            {
                var fragment = builder2.ResolveHaving(havingExpr, groupingParam!);
                parser.Statement.HavingClauses.Add(fragment);
            }
        }

        // Add tenant filter
        if (!parser.IsAnyTenant)
        {
            if (parser.TenantIds != null)
            {
                parser.Statement.Wheres.Add(new TenantInFilter(parser.TenantIds));
            }
            else
            {
                parser.Statement.Wheres.Add(new ComparisonFilter("tenant_id", "=", _session.TenantId));
            }
        }

        // Build SQL and execute
        await using var batch = new SqlBatch();
        var batchBuilder = new BatchBuilder(batch);
        parser.Statement.Apply(batchBuilder);
        batchBuilder.Compile();

        await using var reader = await _session.ExecuteReaderAsync(batch, token);

        // Check if the select is JSON_OBJECT (complex projection) or scalar
        if (builder2.SelectColumns.StartsWith("JSON_OBJECT("))
        {
            // Deserialize JSON results
            return await InvokeGroupByListHandlerAsync<TResult>(reader, parser.SelectExpression!, token);
        }

        // Scalar select (g.Key or g.Count())
        return await InvokeScalarListHandlerAsync<TResult>(reader, token);
    }

    private async Task<TResult> InvokeGroupByListHandlerAsync<TResult>(
        System.Data.Common.DbDataReader reader, LambdaExpression selectExpression,
        CancellationToken token)
    {
        // TResult is IReadOnlyList<TElement>
        var elementType = typeof(TResult).GetGenericArguments()[0];
        var handlerType = typeof(QueryHandlers.GroupByListHandler<>).MakeGenericType(elementType);
        var handler = Activator.CreateInstance(handlerType, _session.Serializer)!;

        var handleMethod = handlerType.GetMethod("HandleAsync")!;
        var task = (Task)handleMethod.Invoke(handler, [reader, token])!;
        await task;

        var resultProperty = task.GetType().GetProperty("Result")!;
        return (TResult)resultProperty.GetValue(task)!;
    }

    private static System.Linq.Expressions.ParameterExpression? FindGroupingParameterInExpression(
        Expression expression)
    {
        var finder = new GroupingParameterFinder();
        finder.Visit(expression);
        return finder.Parameter;
    }

    private class GroupingParameterFinder : ExpressionVisitor
    {
        public System.Linq.Expressions.ParameterExpression? Parameter { get; private set; }

        protected override Expression VisitParameter(System.Linq.Expressions.ParameterExpression node)
        {
            if (Parameter == null && node.Type.IsGenericType
                && node.Type.GetGenericTypeDefinition() == typeof(IGrouping<,>))
            {
                Parameter = node;
            }
            return base.VisitParameter(node);
        }
    }

    private async Task<TResult> ExecuteGroupJoinAsync<TResult>(LinqQueryParser parser, CancellationToken token)
    {
        var joinData = parser.GroupJoinData!;

        // Resolve providers for both sides
        var outerProvider = _providers.GetProvider(joinData.OuterElementType);
        var innerProvider = _providers.GetProvider(joinData.InnerElementType);
        await _tableEnsurer.EnsureTableAsync(outerProvider, token);
        await _tableEnsurer.EnsureTableAsync(innerProvider, token);

        var outerMapping = outerProvider.Mapping;
        var innerMapping = innerProvider.Mapping;

        // Create MemberFactories for both sides
        var outerMemberFactory = new MemberFactory(_session.Options, outerMapping);
        var innerMemberFactory = new MemberFactory(_session.Options, innerMapping);

        // Resolve key selectors to SQL locators
        var outerKeyBody = LinqQueryParser.StripConvert(joinData.OuterKeySelector.Body);
        var innerKeyBody = LinqQueryParser.StripConvert(joinData.InnerKeySelector.Body);

        var outerKeyLocator = ResolveKeyLocator(outerKeyBody, outerMemberFactory);
        var innerKeyLocator = ResolveKeyLocator(innerKeyBody, innerMemberFactory);

        // Alias the locators for the join
        outerKeyLocator = JoinStatement.AliasLocator(outerKeyLocator, "outer_t");
        innerKeyLocator = JoinStatement.AliasLocator(innerKeyLocator, "inner_t");

        // Build the JoinStatement
        var joinStatement = new JoinStatement
        {
            OuterTable = outerMapping.QualifiedTableName,
            InnerTable = innerMapping.QualifiedTableName,
            OuterKeyLocator = outerKeyLocator,
            InnerKeyLocator = innerKeyLocator,
            IsLeftJoin = joinData.IsLeftJoin
        };

        // Add tenant filters for both sides
        if (!parser.IsAnyTenant)
        {
            if (parser.TenantIds != null)
            {
                joinStatement.OuterWheres.Add(new TenantInFilter(parser.TenantIds, "outer_t.tenant_id"));
                joinStatement.InnerWheres.Add(new TenantInFilter(parser.TenantIds, "inner_t.tenant_id"));
            }
            else
            {
                joinStatement.OuterWheres.Add(new ComparisonFilter("outer_t.tenant_id", "=", _session.TenantId));
                joinStatement.InnerWheres.Add(new ComparisonFilter("inner_t.tenant_id", "=", _session.TenantId));
            }
        }

        // Add soft delete filters for both sides
        if (outerMapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            joinStatement.OuterWheres.Add(new LiteralSqlFragment("outer_t.is_deleted = 0"));
        }
        if (innerMapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            joinStatement.InnerWheres.Add(new LiteralSqlFragment("inner_t.is_deleted = 0"));
        }

        // Add any WHERE clauses from the outer source (captured by parser before GroupJoin).
        // These use bare column names (data, id) — wrap them with aliasing for outer_t.
        foreach (var where in parser.Statement.Wheres)
        {
            joinStatement.OuterWheres.Add(new AliasedSqlFragment(where, "outer_t"));
        }

        // Handle Count/First/Single terminal operators
        if (parser.ValueMode != null)
        {
            switch (parser.ValueMode)
            {
                case SingleValueMode.Count:
                    joinStatement.SelectColumns = "COUNT(*)";
                    break;
                case SingleValueMode.LongCount:
                    joinStatement.SelectColumns = "CAST(COUNT(*) AS bigint)";
                    break;
                case SingleValueMode.First:
                case SingleValueMode.FirstOrDefault:
                    joinStatement.Limit = 1;
                    break;
                case SingleValueMode.Single:
                case SingleValueMode.SingleOrDefault:
                    joinStatement.Limit = 2;
                    break;
            }
        }

        // Transfer pagination from the parser's Statement
        if (parser.Statement.Limit.HasValue && !joinStatement.Limit.HasValue)
        {
            joinStatement.Limit = parser.Statement.Limit;
        }
        if (parser.Statement.Offset.HasValue)
        {
            joinStatement.Offset = parser.Statement.Offset;
        }

        // Resolve OrderBy expressions captured after SelectMany.
        // These reference the projected anonymous type — map each member to
        // the correct table (outer or inner) via the result selector.
        var memberOriginMap = BuildMemberOriginMap(joinData);
        foreach (var (orderByLambda, descending) in joinData.OrderByExpressions)
        {
            var body = LinqQueryParser.StripConvert(orderByLambda.Body);
            if (body is MemberExpression memberExpr)
            {
                var memberName = memberExpr.Member.Name;
                var (factory, alias) = memberOriginMap.TryGetValue(memberName, out var origin) && origin == "inner"
                    ? (innerMemberFactory, "inner_t")
                    : (outerMemberFactory, "outer_t");

                // Resolve via the correct member factory using the original property on the document type
                var docProperty = origin == "inner"
                    ? FindOriginalProperty(joinData, memberName, isOuter: false)
                    : FindOriginalProperty(joinData, memberName, isOuter: true);

                if (docProperty != null)
                {
                    var docMember = factory.ResolveMember(docProperty);
                    joinStatement.OrderBys.Add((JoinStatement.AliasLocator(docMember.TypedLocator, alias), descending));
                }
            }
        }

        // Build SQL and execute
        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        joinStatement.Apply(builder);
        builder.Compile();

        await using var reader = await _session.ExecuteReaderAsync(batch, token);

        // Handle scalar results (Count, etc.)
        if (parser.ValueMode is SingleValueMode.Count or SingleValueMode.LongCount)
        {
            var scalarHandler = new ScalarHandler<TResult>();
            return await scalarHandler.HandleAsync(reader, token);
        }

        // Rewrite the result selector and compile
        var resultSelector = joinData.SelectManyResultSelector;
        if (resultSelector == null)
        {
            throw new NotSupportedException(
                "GroupJoin without SelectMany is not supported. Use GroupJoin(...).SelectMany(...).");
        }

        var rewrittenSelector = JoinResultSelectorRewriter.Rewrite(
            joinData.GroupJoinResultSelector, resultSelector,
            joinData.OuterElementType, joinData.InnerElementType);

        // Use reflection to invoke the generic handler
        return await InvokeJoinHandlerAsync<TResult>(
            joinData.OuterElementType, joinData.InnerElementType,
            rewrittenSelector, joinData.IsLeftJoin,
            reader, parser.ValueMode, token);
    }

    /// <summary>
    ///     Builds a map from anonymous result type member names to "outer" or "inner"
    ///     by tracing through the SelectMany result selector and GroupJoin result selector.
    /// </summary>
    private static Dictionary<string, string> BuildMemberOriginMap(GroupJoinData joinData)
    {
        var map = new Dictionary<string, string>();

        if (joinData.SelectManyResultSelector?.Body is not NewExpression resultNew || resultNew.Members == null)
            return map;

        // Parse GroupJoin result selector to know which anonymous member = outer, which = inner collection
        var groupJoinResultSelector = joinData.GroupJoinResultSelector;
        var outerParam = groupJoinResultSelector.Parameters[0];
        var innerCollectionParam = groupJoinResultSelector.Parameters[1];
        var gjMemberMap = new Dictionary<string, string>(); // member name → "outer" or "inner"

        if (groupJoinResultSelector.Body is NewExpression gjNew && gjNew.Members != null)
        {
            for (var i = 0; i < gjNew.Members.Count; i++)
            {
                if (gjNew.Arguments[i] == outerParam)
                    gjMemberMap[gjNew.Members[i].Name] = "outer";
                else if (gjNew.Arguments[i] == innerCollectionParam)
                    gjMemberMap[gjNew.Members[i].Name] = "inner";
            }
        }

        // Now trace each member of the SelectMany result selector
        var smTempParam = joinData.SelectManyResultSelector.Parameters[0]; // temp (anonymous from GroupJoin)
        var smInnerParam = joinData.SelectManyResultSelector.Parameters[1]; // o (inner element)

        for (var i = 0; i < resultNew.Members.Count; i++)
        {
            var resultMemberName = resultNew.Members[i].Name;
            var arg = resultNew.Arguments[i];

            // Trace back to determine origin
            var origin = TraceExpressionOrigin(arg, smTempParam, smInnerParam, gjMemberMap);
            if (origin != null)
            {
                map[resultMemberName] = origin;
            }
        }

        return map;
    }

    private static string? TraceExpressionOrigin(
        Expression expr,
        ParameterExpression tempParam,
        ParameterExpression innerParam,
        Dictionary<string, string> gjMemberMap)
    {
        // Direct parameter reference: o → inner
        if (expr == innerParam) return "inner";

        // Member access chain: trace to root
        if (expr is MemberExpression member)
        {
            var root = GetRootExpression(member);
            if (root == innerParam) return "inner";
            if (root == tempParam)
            {
                // Find the first member after temp: temp.c → "c"
                var firstMember = GetFirstMemberAfterParam(member, tempParam);
                if (firstMember != null && gjMemberMap.TryGetValue(firstMember, out var origin))
                    return origin;
            }
        }

        // Conditional (ternary) — check both branches
        if (expr is ConditionalExpression cond)
        {
            return TraceExpressionOrigin(cond.IfTrue, tempParam, innerParam, gjMemberMap)
                ?? TraceExpressionOrigin(cond.IfFalse, tempParam, innerParam, gjMemberMap);
        }

        return null;
    }

    private static Expression GetRootExpression(MemberExpression expr)
    {
        Expression current = expr;
        while (current is MemberExpression m)
            current = m.Expression!;
        return current;
    }

    private static string? GetFirstMemberAfterParam(MemberExpression expr, ParameterExpression param)
    {
        var chain = new List<string>();
        Expression? current = expr;
        while (current is MemberExpression m)
        {
            chain.Insert(0, m.Member.Name);
            current = m.Expression;
        }

        // chain[0] is the first member after the parameter (e.g., "c" in temp.c.Name)
        return chain.Count > 0 && current == param ? chain[0] : null;
    }

    /// <summary>
    ///     Finds the original MemberExpression on the document type for an anonymous type member.
    ///     Traces through the SelectMany result selector to find the source expression.
    /// </summary>
    private static MemberExpression? FindOriginalProperty(GroupJoinData joinData, string memberName, bool isOuter)
    {
        if (joinData.SelectManyResultSelector?.Body is not NewExpression resultNew || resultNew.Members == null)
            return null;

        for (var i = 0; i < resultNew.Members.Count; i++)
        {
            if (resultNew.Members[i].Name != memberName) continue;

            // Trace the argument back to the document property
            var arg = resultNew.Arguments[i];
            return FindDeepestMemberExpression(arg);
        }

        return null;
    }

    private static MemberExpression? FindDeepestMemberExpression(Expression expr)
    {
        // Walk through member chains to find the deepest member on a document type
        // e.g., temp.c.Name → we want the MemberExpression for .Name on the document type
        if (expr is MemberExpression member)
        {
            // Create a synthetic member expression on a parameter of the document type
            // We need to resolve through the document's type, not through anonymous types
            var rootType = GetUltimateDeclaringType(member);
            if (rootType != null)
            {
                var syntheticParam = Expression.Parameter(rootType, "x");
                return RebuildMemberAccess(member, syntheticParam);
            }
        }

        return null;
    }

    private static Type? GetUltimateDeclaringType(MemberExpression expr)
    {
        Expression? current = expr;
        Type? lastType = null;
        while (current is MemberExpression m)
        {
            lastType = m.Expression?.Type;
            current = m.Expression;
        }
        return lastType;
    }

    private static MemberExpression? RebuildMemberAccess(MemberExpression original, ParameterExpression param)
    {
        // Collect the member chain from innermost to outermost
        var chain = new List<System.Reflection.MemberInfo>();
        Expression? current = original;
        while (current is MemberExpression m)
        {
            chain.Insert(0, m.Member);
            current = m.Expression;
        }

        // Skip anonymous type members — start from the first member that exists on the document type
        Expression result = param;
        var started = false;
        foreach (var memberInfo in chain)
        {
            if (!started)
            {
                // Check if this member exists on the param type (the document type)
                var prop = param.Type.GetProperty(memberInfo.Name);
                var field = param.Type.GetField(memberInfo.Name);
                if (prop != null || field != null)
                {
                    started = true;
                    result = Expression.MakeMemberAccess(result, (System.Reflection.MemberInfo?)prop ?? field!);
                    continue;
                }
                // Skip anonymous type members
                continue;
            }

            result = Expression.MakeMemberAccess(result, memberInfo);
        }

        return result as MemberExpression;
    }

    private static string ResolveKeyLocator(Expression keyBody, MemberFactory memberFactory)
    {
        if (keyBody is MemberExpression memberExpr)
        {
            var member = memberFactory.ResolveMember(memberExpr);
            return member.TypedLocator;
        }

        throw new NotSupportedException(
            $"Join key selector must be a member expression, got: {keyBody.NodeType}");
    }

    private async Task<TResult> InvokeJoinHandlerAsync<TResult>(
        Type outerType, Type innerType,
        LambdaExpression rewrittenSelector, bool isLeftJoin,
        DbDataReader reader, SingleValueMode? valueMode,
        CancellationToken token)
    {
        // Determine the result element type
        // TResult is IReadOnlyList<TElement> for list results, or TElement for single results
        Type resultElementType;
        if (typeof(TResult).IsGenericType &&
            typeof(TResult).GetGenericTypeDefinition() == typeof(IReadOnlyList<>))
        {
            resultElementType = typeof(TResult).GetGenericArguments()[0];
        }
        else
        {
            resultElementType = typeof(TResult);
        }

        // Compile the rewritten selector to a Func<TOuter, TInner?, TResultElement>
        var funcType = typeof(Func<,,>).MakeGenericType(outerType, innerType, resultElementType);
        var compiledSelector = rewrittenSelector.Compile();

        // Create and invoke JoinListHandler<TOuter, TInner, TResultElement>
        var handlerType = typeof(JoinListHandler<,,>).MakeGenericType(outerType, innerType, resultElementType);
        var handler = Activator.CreateInstance(handlerType, _session.Serializer, compiledSelector, isLeftJoin)!;

        var handleMethod = handlerType.GetMethod("HandleAsync")!;
        var task = (Task)handleMethod.Invoke(handler, [reader, token])!;
        await task;

        var resultProperty = task.GetType().GetProperty("Result")!;
        var list = resultProperty.GetValue(task)!;

        // For single-value modes, extract the single element
        if (valueMode is SingleValueMode.First or SingleValueMode.FirstOrDefault
            or SingleValueMode.Single or SingleValueMode.SingleOrDefault)
        {
            var items = (System.Collections.IList)list;
            if (valueMode is SingleValueMode.Single or SingleValueMode.SingleOrDefault && items.Count > 1)
            {
                throw new InvalidOperationException("Sequence contains more than one element");
            }

            if (items.Count == 0)
            {
                if (valueMode is SingleValueMode.First or SingleValueMode.Single)
                {
                    throw new InvalidOperationException("Sequence contains no elements");
                }
                return default!;
            }

            return (TResult)items[0]!;
        }

        return (TResult)list;
    }

    private static void ApplySingleValueMode(LinqQueryParser parser)
    {
        if (parser.ValueMode == null) return;

        var statement = parser.Statement;

        switch (parser.ValueMode)
        {
            case SingleValueMode.First:
            case SingleValueMode.FirstOrDefault:
                statement.Limit = 1;
                break;

            case SingleValueMode.Single:
            case SingleValueMode.SingleOrDefault:
                // Fetch 2 rows to detect "more than one element"
                statement.Limit = 2;
                break;

            case SingleValueMode.Last:
            case SingleValueMode.LastOrDefault:
                // Reverse the ordering and take 1
                for (var i = 0; i < statement.OrderBys.Count; i++)
                {
                    var (locator, desc) = statement.OrderBys[i];
                    statement.OrderBys[i] = (locator, !desc);
                }
                statement.Limit = 1;
                break;

            case SingleValueMode.Count:
                statement.SelectColumns = "COUNT(*)";
                break;

            case SingleValueMode.LongCount:
                statement.SelectColumns = "CAST(COUNT(*) AS bigint)";
                break;

            case SingleValueMode.Any:
                // Wrap in EXISTS
                var innerSelect = statement.SelectColumns;
                statement.SelectColumns = "1";
                statement.Limit = 1;
                statement.IsExistsWrapper = true;
                break;

            case SingleValueMode.Sum:
                statement.SelectColumns = parser.AggregationMember != null
                    ? $"ISNULL(SUM({parser.AggregationMember.TypedLocator}), 0)"
                    : "ISNULL(SUM(data), 0)";
                break;

            case SingleValueMode.Min:
                statement.SelectColumns = parser.AggregationMember != null
                    ? $"MIN({parser.AggregationMember.TypedLocator})"
                    : "MIN(data)";
                break;

            case SingleValueMode.Max:
                statement.SelectColumns = parser.AggregationMember != null
                    ? $"MAX({parser.AggregationMember.TypedLocator})"
                    : "MAX(data)";
                break;

            case SingleValueMode.Average:
                statement.SelectColumns = parser.AggregationMember != null
                    ? $"AVG(CAST({parser.AggregationMember.TypedLocator} AS float))"
                    : "AVG(CAST(data AS float))";
                break;
        }
    }

    private async Task<TResult> HandleResultAsync<TResult>(
        DbDataReader reader, LinqQueryParser parser, Type documentType, CancellationToken token,
        bool syncRevision = false, bool syncGuidVersion = false)
    {
        if (parser.ValueMode == null)
        {
            // Check for Select projection
            if (parser.SelectExpression != null)
            {
                if (parser.IsScalarSelect)
                {
                    // Scalar select: read values directly from the column
                    return await InvokeScalarListHandlerAsync<TResult>(reader, token);
                }

                // Complex projection: read documents then project
                return await InvokeProjectionHandlerAsync<TResult>(documentType, reader, parser.SelectExpression, token);
            }

            // Plain list result
            return await InvokeListHandlerAsync<TResult>(documentType, reader, token, syncRevision, syncGuidVersion);
        }

        switch (parser.ValueMode)
        {
            case SingleValueMode.First:
            case SingleValueMode.Single:
            case SingleValueMode.Last:
                return await InvokeOneResultHandlerAsync<TResult>(
                    documentType, reader, token, canBeNull: false,
                    canBeMultiples: parser.ValueMode == SingleValueMode.First || parser.ValueMode == SingleValueMode.Last,
                    syncRevision: syncRevision, syncGuidVersion: syncGuidVersion);

            case SingleValueMode.FirstOrDefault:
            case SingleValueMode.SingleOrDefault:
            case SingleValueMode.LastOrDefault:
                return await InvokeOneResultHandlerAsync<TResult>(
                    documentType, reader, token, canBeNull: true,
                    canBeMultiples: parser.ValueMode != SingleValueMode.SingleOrDefault,
                    syncRevision: syncRevision, syncGuidVersion: syncGuidVersion);

            case SingleValueMode.Count:
            case SingleValueMode.LongCount:
            case SingleValueMode.Sum:
            case SingleValueMode.Min:
            case SingleValueMode.Max:
            case SingleValueMode.Average:
                var scalarHandler = new ScalarHandler<TResult>();
                return await scalarHandler.HandleAsync(reader, token);

            case SingleValueMode.Any:
                var anyHandler = new AnyHandler();
                var anyResult = await anyHandler.HandleAsync(reader, token);
                return (TResult)(object)anyResult;

            default:
                throw new NotSupportedException($"Unsupported single value mode: {parser.ValueMode}");
        }
    }

    private async Task<TResult> InvokeListHandlerAsync<TResult>(
        Type itemType, DbDataReader reader, CancellationToken token,
        bool syncRevision = false, bool syncGuidVersion = false)
    {
        var selectorType = typeof(DeserializingSelector<>).MakeGenericType(itemType);
        var selector = Activator.CreateInstance(selectorType, _session.Serializer, syncRevision, syncGuidVersion)!;

        var handlerType = typeof(ListQueryHandler<>).MakeGenericType(itemType);
        var handler = Activator.CreateInstance(handlerType, selector)!;

        var handleMethod = handlerType.GetMethod("HandleAsync")!;
        var task = (Task)handleMethod.Invoke(handler, [reader, token])!;
        await task;

        var resultProperty = task.GetType().GetProperty("Result")!;
        return (TResult)resultProperty.GetValue(task)!;
    }

    private async Task<TResult> InvokeScalarListHandlerAsync<TResult>(
        DbDataReader reader, CancellationToken token)
    {
        // TResult is IReadOnlyList<TScalar>, extract TScalar
        var scalarType = typeof(TResult).GetGenericArguments()[0];
        var handlerType = typeof(ScalarListHandler<>).MakeGenericType(scalarType);
        var handler = Activator.CreateInstance(handlerType)!;

        var handleMethod = handlerType.GetMethod("HandleAsync")!;
        var task = (Task)handleMethod.Invoke(handler, [reader, token])!;
        await task;

        var resultProperty = task.GetType().GetProperty("Result")!;
        return (TResult)resultProperty.GetValue(task)!;
    }

    private async Task<TResult> InvokeProjectionHandlerAsync<TResult>(
        Type sourceType, DbDataReader reader, LambdaExpression selectExpression,
        CancellationToken token)
    {
        // TResult is IReadOnlyList<TProjected>
        var projectedType = typeof(TResult).GetGenericArguments()[0];
        var handlerType = typeof(ProjectionListHandler<,>).MakeGenericType(sourceType, projectedType);
        var handler = Activator.CreateInstance(handlerType, _session.Serializer, selectExpression)!;

        var handleMethod = handlerType.GetMethod("HandleAsync")!;
        var task = (Task)handleMethod.Invoke(handler, [reader, token])!;
        await task;

        var resultProperty = task.GetType().GetProperty("Result")!;
        return (TResult)resultProperty.GetValue(task)!;
    }

    private async Task<TResult> InvokeOneResultHandlerAsync<TResult>(
        Type documentType, DbDataReader reader, CancellationToken token,
        bool canBeNull, bool canBeMultiples,
        bool syncRevision = false, bool syncGuidVersion = false)
    {
        var selectorType = typeof(DeserializingSelector<>).MakeGenericType(documentType);
        var selector = Activator.CreateInstance(selectorType, _session.Serializer, syncRevision, syncGuidVersion)!;

        var handlerType = typeof(OneResultHandler<>).MakeGenericType(documentType);
        var handler = Activator.CreateInstance(handlerType, selector, canBeNull, canBeMultiples)!;

        var handleMethod = handlerType.GetMethod("HandleAsync")!;
        var task = (Task)handleMethod.Invoke(handler, [reader, token])!;
        await task;

        var resultProperty = task.GetType().GetProperty("Result")!;
        return (TResult)resultProperty.GetValue(task)!;
    }

    private static Type FindDocumentType(Expression expression)
    {
        return expression switch
        {
            MethodCallExpression method => FindDocumentType(method.Arguments[0]),
            ConstantExpression { Value: IQueryable queryable } => queryable.ElementType,
            _ => throw new NotSupportedException(
                $"Cannot determine document type from expression: {expression.NodeType}")
        };
    }

    private static Type GetElementType(Expression expression)
    {
        if (expression.Type.IsGenericType)
        {
            var genericDef = expression.Type.GetGenericTypeDefinition();
            if (genericDef == typeof(IQueryable<>) || genericDef == typeof(IOrderedQueryable<>))
            {
                return expression.Type.GetGenericArguments()[0];
            }
        }

        throw new NotSupportedException($"Cannot determine element type from: {expression.Type}");
    }

    private static void ApplyModifiedFilters(LinqQueryParser parser)
    {
        if (parser.ModifiedSinceTimestamp.HasValue)
        {
            parser.Statement.Wheres.Add(new ComparisonFilter("last_modified", ">=", parser.ModifiedSinceTimestamp.Value));
        }

        if (parser.ModifiedBeforeTimestamp.HasValue)
        {
            parser.Statement.Wheres.Add(new ComparisonFilter("last_modified", "<", parser.ModifiedBeforeTimestamp.Value));
        }
    }

    private async Task WaitForNonStaleDataAsync(TimeSpan timeout, CancellationToken token)
    {
        var options = _session.Options;
        var eventGraph = options.EventGraph;

        // Get the registered async projection shard names
        var asyncShardNames = options.Projections.All
            .Where(p => p.Lifecycle == JasperFx.Events.Projections.ProjectionLifecycle.Async)
            .SelectMany(p => p.ShardNames())
            .Select(s => s.Identity)
            .ToHashSet();

        // If no async projections are registered, nothing to wait for
        if (asyncShardNames.Count == 0) return;

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (stopwatch.Elapsed < timeout)
        {
            token.ThrowIfCancellationRequested();

            // Use auto-closing connections for polling (independent of session lifetime)
            await using var conn = new SqlConnection(options.ConnectionString);
            await conn.OpenAsync(token);

            // Get high water mark
            long highWater;
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"SELECT ISNULL(MAX(seq_id), 0) FROM {eventGraph.EventsTableName};";
                var result = await cmd.ExecuteScalarAsync(token);
                highWater = result is long seq ? seq : 0;
            }

            if (highWater == 0) return;

            // Check only the registered async projection shards
            var allCaughtUp = true;
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"SELECT name, last_seq_id FROM {eventGraph.ProgressionTableName};";
                await using var reader = await cmd.ExecuteReaderAsync(token);
                var foundShards = new HashSet<string>();
                while (await reader.ReadAsync(token))
                {
                    var name = reader.GetString(0);
                    if (name == "HighWaterMark") continue;
                    // Only check shards registered in this store
                    if (!asyncShardNames.Contains(name)) continue;

                    foundShards.Add(name);
                    var seq = reader.GetInt64(1);
                    if (seq < highWater)
                    {
                        allCaughtUp = false;
                        break;
                    }
                }

                // All registered shards must exist and be caught up
                if (allCaughtUp && foundShards.SetEquals(asyncShardNames)) return;
            }

            await Task.Delay(100, token);
        }

        throw new TimeoutException(
            $"Timed out after {timeout} waiting for projection data to become non-stale.");
    }
}
