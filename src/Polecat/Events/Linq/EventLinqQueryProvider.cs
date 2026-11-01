using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Linq;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Linq.QueryHandlers;
using Polecat.Linq.Selectors;
using Polecat.Linq.SqlGeneration;
using Polecat.Serialization;
using Weasel.SqlServer;

namespace Polecat.Events.Linq;

/// <summary>
///     IQueryProvider for LINQ queries against the event store.
///     Supports both QueryAllRawEvents (IEvent) and QueryRawEventDataOnly&lt;T&gt; (event data type),
///     and through the latter the store-agnostic <c>IReadOnlyEventStore.QueryEventData&lt;T&gt;</c>
///     (jasperfx#1073).
/// </summary>
/// <remarks>
///     Implements the shared <see cref="JasperFx.Events.Documents.IDocumentQueryExecutor" /> hook by
///     delegating to Polecat's own terminators, exactly as <see cref="StreamStateLinqQueryProvider" />
///     does, so the JasperFx.Events.Documents extension terminators and
///     <see cref="PolecatQueryableExtensions" /> are one execution path.
/// </remarks>
internal class EventLinqQueryProvider : IPolecatAsyncQueryProvider,
    JasperFx.Events.Documents.IDocumentQueryExecutor
{
    private readonly QuerySession _session;
    private readonly EventGraph _events;
    private readonly IMemberResolver _memberFactory;
    private readonly string _eventsTable;
    private readonly bool _isAllEvents;
    private readonly string? _eventTypeName;
    private readonly Type? _eventDataType;
    private readonly string? _tenantId;
    private readonly bool _includeArchived;

    /// <summary>
    ///     The owning session, surfaced for event-query terminals that need live-session context
    ///     (AggregateToAsync / AggregateToManyAsync).
    /// </summary>
    internal QuerySession Session => _session;

    /// <summary>
    ///     Create provider for QueryAllRawEvents().
    /// </summary>
    /// <param name="includeArchived">
    ///     Leave archived events in rather than adding the default <c>is_archived = 0</c>. A predicate
    ///     naming IsArchived is applied either way.
    /// </param>
    public EventLinqQueryProvider(QuerySession session, EventGraph events, string? tenantId = null,
        bool includeArchived = false)
    {
        _tenantId = tenantId;
        _includeArchived = includeArchived;
        _session = session;
        _events = events;
        _memberFactory = new EventMemberFactory();
        _eventsTable = events.EventsTableName;
        _isAllEvents = true;
    }

    /// <summary>
    ///     Create provider for QueryRawEventDataOnly&lt;T&gt;() and QueryEventData&lt;T&gt;(tenantId).
    /// </summary>
    /// <param name="tenantId">
    ///     Explicit tenant scope from <c>QueryEventData&lt;T&gt;(tenantId)</c>, already validated by the
    ///     caller (a non-null tenant on a store with no tenant dimension was refused there). Null falls
    ///     back to the session's own tenant, matching <see cref="StreamStateLinqQueryProvider" />.
    /// </param>
    public EventLinqQueryProvider(QuerySession session, EventGraph events,
        string eventTypeName, Type eventDataType, StoreOptions options, string? tenantId = null)
    {
        _tenantId = tenantId;
        _session = session;
        _events = events;
        _memberFactory = new EventDataMemberFactory(options);
        _eventsTable = events.EventsTableName;
        _isAllEvents = false;
        _eventTypeName = eventTypeName;
        _eventDataType = eventDataType;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2046",
        Justification = "IQueryProvider.CreateQuery(Expression) lacks RUC; the AOT-safe entry is the generic CreateQuery<TElement>(Expression).")]
    [UnconditionalSuppressMessage("AOT", "IL3051",
        Justification = "IQueryProvider.CreateQuery(Expression) lacks RDC; the AOT-safe entry is the generic CreateQuery<TElement>(Expression).")]
    [RequiresDynamicCode("Closes PolecatLinqQueryable<> over the element type via Type.MakeGenericType. AOT consumers should call CreateQuery<TElement>(Expression) instead.")]
    [RequiresUnreferencedCode("Activator.CreateInstance reflects over the constructor of PolecatLinqQueryable<>.")]
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

    [RequiresDynamicCode("Event-store LINQ execution closes ListQueryHandler<>/DeserializingSelector<>/etc. over the event/event-data type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Event-store LINQ execution reflects over the event type (Activator.CreateInstance on handler types, MethodInfo.Invoke on HandleAsync). AOT consumers must preserve event + handler members through DAM or source generation.")]
    public async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken token)
    {
        // HasTag() is only meaningful over IEvent queries, where the FROM table is pc_events.
        var parser = new LinqQueryParser(_memberFactory, _eventsTable,
            // gh-558: the events table has no is_deleted column, so a soft-delete operator over an
            // event query is refused rather than ignored, exactly as it is over a hard-delete
            // document type.
            SoftDeleteTarget.NeverSoftDeleted("The event store"),
            _isAllEvents ? [new HasTagParser(_events), new HasTagValueParser(_events)] : null);
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
                parser.Statement.Wheres.Add(
                    new ComparisonFilter("tenant_id", "=", _tenantId ?? _session.TenantId));
            }
        }

        // Add is_archived = 0 filter by default
        if (!_includeArchived && !parser.IsMaybeDeleted)
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
                // Full IEvent result set. #256 appended the opt-in metadata columns by hand here, and
                // left headers out (#756) -- so QueryAllRawEvents never carried Headers. Composed by the
                // one helper every event read uses, so EventListHandler's MetadataSlots line up.
                parser.Statement.SelectColumns =
                    Internal.PcEventsRowReader.ComposeSelectColumns(_events.EventOptions);
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

    [RequiresDynamicCode("Routes to handler invocations that close generic handler types over the event/event-data type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Routes to handler invocations that reflect over handler types.")]
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

    [RequiresDynamicCode("Closes DeserializingSelector<> + ListQueryHandler<> over itemType via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Reflects over handler/selector types (Activator.CreateInstance, MethodInfo.Invoke on HandleAsync, GetProperty on Task<>.Result).")]
    private async Task<TResult> InvokeListHandlerAsync<TResult>(
        Type itemType, DbDataReader reader, CancellationToken token)
    {
        // Polecat#46 cold-start row: per-call MakeGenericType + Activator.CreateInstance
        // routed through JasperFx.Core.Reflection.GenericFactoryCache. Each
        // (openType, itemType) tuple builds an Activator delegate once; steady-state
        // calls skip both the Type.MakeGenericType lookup and the reflective ctor
        // invocation. Sibling of Marten#4308 / PR #4329.
        var selector = GenericFactoryCache.BuildAs<object>(
            typeof(DeserializingSelector<>),
            itemType,
            _session.Serializer,
            static closed => arg => Activator.CreateInstance(closed, arg)!);

        var handler = GenericFactoryCache.BuildAs<object>(
            typeof(ListQueryHandler<>),
            itemType,
            selector,
            static closed => arg => Activator.CreateInstance(closed, arg)!);

        var handlerType = handler.GetType();
        var handleMethod = handlerType.GetMethod("HandleAsync")!;
        var task = (Task)handleMethod.Invoke(handler, [reader, token])!;
        await task;

        var resultProperty = task.GetType().GetProperty("Result")!;
        return (TResult)resultProperty.GetValue(task)!;
    }

    [RequiresDynamicCode("Closes ScalarListHandler<> over TResult's element type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Reflects over ScalarListHandler<>.HandleAsync via MethodInfo.Invoke and Task<>.Result via GetProperty.")]
    private async Task<TResult> InvokeScalarListHandlerAsync<TResult>(
        DbDataReader reader, CancellationToken token)
    {
        var scalarType = typeof(TResult).GetGenericArguments()[0];

        // #743: the instantiation ToListAsync<T> rooted statically, so a VALUE-type scalar has native code
        // under AOT. Polecat#46 cold-start: otherwise cache the parameterless ctor delegate for
        // ScalarListHandler<TScalar> per scalarType via GenericFactoryCache.
        var handler = ScalarListHandlers.TryCreate(scalarType)
                      ?? GenericFactoryCache.BuildAs<IQueryHandler>(
                          typeof(ScalarListHandler<>),
                          scalarType,
                          static closed => () => (IQueryHandler)Activator.CreateInstance(closed)!);

        // #741's fix, which the document provider took and this copy never did: the non-generic
        // IQueryHandler instead of GetMethod("HandleAsync")! and GetProperty("Result")!, both of which
        // return null for a trimmed member, so each `!` was a NullReferenceException under Native AOT.
        return (TResult)(await handler.HandleAsObjectAsync(reader, token).ConfigureAwait(false))!;
    }

    [RequiresDynamicCode("Closes DeserializingSelector<> + OneResultHandler<> over documentType via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Reflects over handler/selector types (Activator.CreateInstance, MethodInfo.Invoke on HandleAsync, GetProperty on Task<>.Result).")]
    private async Task<TResult> InvokeOneResultHandlerAsync<TResult>(
        Type documentType, DbDataReader reader, CancellationToken token,
        bool canBeNull, bool canBeMultiples)
    {
        // Polecat#46 cold-start: cache both factories per documentType. The
        // canBeNull / canBeMultiples booleans are invocation arguments, not
        // cache-key components — the same OneResultHandler<TDoc> ctor delegate
        // serves all (canBeNull, canBeMultiples) combinations for a given TDoc.
        var selector = GenericFactoryCache.BuildAs<object>(
            typeof(DeserializingSelector<>),
            documentType,
            _session.Serializer,
            static closed => arg => Activator.CreateInstance(closed, arg)!);

        var handler = GenericFactoryCache.BuildAs<object>(
            typeof(OneResultHandler<>),
            documentType,
            selector,
            canBeNull,
            canBeMultiples,
            static closed => (a, b, c) => Activator.CreateInstance(closed, a, b, c)!);

        var handlerType = handler.GetType();
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
                    statement.OrderBys[i] = statement.OrderBys[i].Reversed();
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

        throw new Polecat.Linq.BadLinqExpressionException($"Cannot determine the element type from: {expression.Type}");
    }

    // The shared JasperFx.Events.Documents terminators dispatch through these four primitives, each
    // delegating to Polecat's own terminator (jasperfx#1073, as StreamStateLinqQueryProvider does).

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. This records that the path is unsafe and that the diagnostic has nowhere left to go.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. This records that the path is unsafe and that the diagnostic has nowhere left to go.")]
    Task<IReadOnlyList<T>> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteToListAsync<T>(
        IQueryable<T> queryable, CancellationToken token)
        => queryable.ToListAsync(token);

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. This records that the path is unsafe and that the diagnostic has nowhere left to go.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. This records that the path is unsafe and that the diagnostic has nowhere left to go.")]
    Task<T?> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteFirstOrDefaultAsync<T>(
        IQueryable<T> queryable, CancellationToken token) where T : default
        => queryable.FirstOrDefaultAsync(token);

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. This records that the path is unsafe and that the diagnostic has nowhere left to go.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. This records that the path is unsafe and that the diagnostic has nowhere left to go.")]
    Task<int> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteCountAsync<T>(
        IQueryable<T> queryable, CancellationToken token)
        => queryable.CountAsync(token);

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. This records that the path is unsafe and that the diagnostic has nowhere left to go.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. This records that the path is unsafe and that the diagnostic has nowhere left to go.")]
    Task<bool> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteAnyAsync<T>(
        IQueryable<T> queryable, CancellationToken token)
        => queryable.AnyAsync(token);
}
