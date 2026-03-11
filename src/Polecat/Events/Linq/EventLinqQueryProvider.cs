using System.Data.Common;
using System.Linq.Expressions;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Linq;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Linq.QueryHandlers;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Events.Linq;

/// <summary>
///     IQueryProvider for LINQ queries against the event store.
///     Supports both QueryAllRawEvents (IEvent) and QueryRawEventDataOnly&lt;T&gt; (event data type).
/// </summary>
internal class EventLinqQueryProvider : IPolecatAsyncQueryProvider
{
    private readonly QuerySession _session;
    private readonly EventGraph _events;
    private readonly IMemberResolver _memberFactory;
    private readonly string _eventsTable;
    private readonly bool _isAllEvents;
    private readonly string? _eventTypeName;
    private readonly Type? _eventDataType;

    /// <summary>
    ///     Create provider for QueryAllRawEvents().
    /// </summary>
    public EventLinqQueryProvider(QuerySession session, EventGraph events)
    {
        _session = session;
        _events = events;
        _memberFactory = new EventMemberFactory();
        _eventsTable = events.EventsTableName;
        _isAllEvents = true;
    }

    /// <summary>
    ///     Create provider for QueryRawEventDataOnly&lt;T&gt;().
    /// </summary>
    public EventLinqQueryProvider(QuerySession session, EventGraph events,
        string eventTypeName, Type eventDataType, StoreOptions options)
    {
        _session = session;
        _events = events;
        _memberFactory = new EventDataMemberFactory(options);
        _eventsTable = events.EventsTableName;
        _isAllEvents = false;
        _eventTypeName = eventTypeName;
        _eventDataType = eventDataType;
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

    public async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken token)
    {
        var parser = new LinqQueryParser(_memberFactory, _eventsTable);
        parser.Parse(expression);

        ApplySingleValueMode(parser);

        if (parser.IsDistinct) parser.Statement.IsDistinct = true;

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

        // Add is_archived = 0 filter by default (unless MaybeArchived was called)
        if (!parser.IsMaybeDeleted)
        {
            parser.Statement.Wheres.Add(new LiteralSqlFragment("is_archived = 0"));
        }

        // For QueryRawEventDataOnly, filter by event type
        if (!_isAllEvents && _eventTypeName != null)
        {
            parser.Statement.Wheres.Add(new ComparisonFilter("type", "=", _eventTypeName));
        }

        // Set select columns (only if not already set by ApplySingleValueMode for Count/Any/etc.)
        var isScalarAggregate = parser.ValueMode is SingleValueMode.Count or SingleValueMode.LongCount
            or SingleValueMode.Any or SingleValueMode.Sum or SingleValueMode.Min
            or SingleValueMode.Max or SingleValueMode.Average;

        if (!isScalarAggregate && parser.SelectExpression == null)
        {
            if (_isAllEvents)
            {
                // Full IEvent result set
                parser.Statement.SelectColumns =
                    "seq_id, id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type, is_archived";
            }
            else
            {
                // Event data only: just the JSON data column
                parser.Statement.SelectColumns = "data";
            }
        }

        // Build and execute SQL
        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        parser.Statement.Apply(builder);
        builder.Compile();

        await using var reader = await _session.ExecuteReaderAsync(batch, token);

        return await HandleResultAsync<TResult>(reader, parser, token);
    }

    private async Task<TResult> HandleResultAsync<TResult>(
        DbDataReader reader, LinqQueryParser parser, CancellationToken token)
    {
        if (parser.ValueMode == null)
        {
            if (parser.SelectExpression != null && parser.IsScalarSelect)
            {
                return await InvokeScalarListHandlerAsync<TResult>(reader, token);
            }

            if (_isAllEvents && parser.SelectExpression == null)
            {
                // Return IReadOnlyList<IEvent>
                var handler = new EventListHandler(_session.Serializer, _events);
                var events = await handler.HandleAsync(reader, token);
                return (TResult)(object)events;
            }

            // QueryRawEventDataOnly: deserialize from data column
            var documentType = FindDocumentType();
            return await InvokeListHandlerAsync<TResult>(documentType, reader, token);
        }

        switch (parser.ValueMode)
        {
            case SingleValueMode.First:
            case SingleValueMode.Single:
            case SingleValueMode.Last:
                if (_isAllEvents && parser.SelectExpression == null)
                    return await HandleSingleEventAsync<TResult>(reader, token, canBeNull: false);
                return await InvokeOneResultHandlerAsync<TResult>(
                    FindDocumentType(), reader, token, canBeNull: false,
                    canBeMultiples: parser.ValueMode != SingleValueMode.Single);

            case SingleValueMode.FirstOrDefault:
            case SingleValueMode.SingleOrDefault:
            case SingleValueMode.LastOrDefault:
                if (_isAllEvents && parser.SelectExpression == null)
                    return await HandleSingleEventAsync<TResult>(reader, token, canBeNull: true);
                return await InvokeOneResultHandlerAsync<TResult>(
                    FindDocumentType(), reader, token, canBeNull: true,
                    canBeMultiples: parser.ValueMode != SingleValueMode.SingleOrDefault);

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

    private async Task<TResult> HandleSingleEventAsync<TResult>(
        DbDataReader reader, CancellationToken token, bool canBeNull)
    {
        var handler = new EventListHandler(_session.Serializer, _events);
        var events = await handler.HandleAsync(reader, token);

        if (events.Count == 0)
        {
            if (!canBeNull)
                throw new InvalidOperationException("Sequence contains no elements");
            return default!;
        }

        return (TResult)(object)events[0];
    }

    private Type FindDocumentType()
    {
        if (!_isAllEvents && _eventDataType != null)
        {
            return _eventDataType;
        }

        return typeof(IEvent);
    }

    private async Task<TResult> InvokeListHandlerAsync<TResult>(
        Type itemType, DbDataReader reader, CancellationToken token)
    {
        var selectorType = typeof(Polecat.Linq.Selectors.DeserializingSelector<>).MakeGenericType(itemType);
        var selector = Activator.CreateInstance(selectorType, _session.Serializer)!;

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
        var scalarType = typeof(TResult).GetGenericArguments()[0];
        var handlerType = typeof(ScalarListHandler<>).MakeGenericType(scalarType);
        var handler = Activator.CreateInstance(handlerType)!;

        var handleMethod = handlerType.GetMethod("HandleAsync")!;
        var task = (Task)handleMethod.Invoke(handler, [reader, token])!;
        await task;

        var resultProperty = task.GetType().GetProperty("Result")!;
        return (TResult)resultProperty.GetValue(task)!;
    }

    private async Task<TResult> InvokeOneResultHandlerAsync<TResult>(
        Type documentType, DbDataReader reader, CancellationToken token,
        bool canBeNull, bool canBeMultiples)
    {
        var selectorType = typeof(Polecat.Linq.Selectors.DeserializingSelector<>).MakeGenericType(documentType);
        var selector = Activator.CreateInstance(selectorType, _session.Serializer)!;

        var handlerType = typeof(OneResultHandler<>).MakeGenericType(documentType);
        var handler = Activator.CreateInstance(handlerType, selector, canBeNull, canBeMultiples)!;

        var handleMethod = handlerType.GetMethod("HandleAsync")!;
        var task = (Task)handleMethod.Invoke(handler, [reader, token])!;
        await task;

        var resultProperty = task.GetType().GetProperty("Result")!;
        return (TResult)resultProperty.GetValue(task)!;
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
                statement.Limit = 2;
                break;

            case SingleValueMode.Last:
            case SingleValueMode.LastOrDefault:
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
}
