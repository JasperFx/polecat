using System.Linq.Expressions;
using System.Text;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Linq.QueryHandlers;
using Polecat.Linq.Selectors;
using Polecat.Linq.SqlGeneration;
using Polecat.Metadata;

namespace Polecat.Linq;

/// <summary>
///     IQueryProvider that translates LINQ expression trees into SQL Server queries.
/// </summary>
internal class PolecatLinqQueryProvider : IQueryProvider
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

        var builder = new CommandBuilder();
        parser.Statement.Apply(builder);
        return builder.ToString();
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

        var builder = new CommandBuilder();
        parser.Statement.Apply(builder);

        var conn = await _session.GetConnectionAsync(token);
        await using var cmd = conn.CreateCommand();

        if (_session.ActiveTransaction != null)
        {
            cmd.Transaction = _session.ActiveTransaction;
        }

        builder.ApplyTo(cmd);

        await using var reader = await cmd.ExecuteReaderAsync(token);

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

    internal async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken token)
    {
        var documentType = FindDocumentType(expression);
        var provider = _providers.GetProvider(documentType);
        await _tableEnsurer.EnsureTableAsync(provider, token);

        var memberFactory = new MemberFactory(_session.Options, provider.Mapping);
        var parser = new LinqQueryParser(memberFactory, provider.Mapping.QualifiedTableName);
        parser.Parse(expression);

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

        // Build SQL
        var builder = new CommandBuilder();
        parser.Statement.Apply(builder);

        // Execute query
        var conn = await _session.GetConnectionAsync(token);
        await using var cmd = conn.CreateCommand();

        if (_session.ActiveTransaction != null)
        {
            cmd.Transaction = _session.ActiveTransaction;
        }

        builder.ApplyTo(cmd);

        await using var reader = await cmd.ExecuteReaderAsync(token);

        return await HandleResultAsync<TResult>(reader, parser, documentType, token, syncRevision, syncGuidVersion);
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
        SqlDataReader reader, LinqQueryParser parser, Type documentType, CancellationToken token,
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
        Type itemType, SqlDataReader reader, CancellationToken token,
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
        SqlDataReader reader, CancellationToken token)
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
        Type sourceType, SqlDataReader reader, LambdaExpression selectExpression,
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
        Type documentType, SqlDataReader reader, CancellationToken token,
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
