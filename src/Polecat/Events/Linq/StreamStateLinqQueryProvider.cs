using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Events.Internal;
using Polecat.Internal;
using Polecat.Linq;
using Polecat.Linq.Parsing;
using Polecat.Linq.QueryHandlers;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Events.Linq;

/// <summary>
///     IQueryProvider for <see cref="QueryEventStore.QueryStreamStates"/> (jasperfx#740): LINQ
///     queries over the <c>pc_streams</c> table hydrating <see cref="StreamState"/> rows through
///     the canonical <see cref="PcStreamsRowReader"/>. Implements the shared
///     <see cref="JasperFx.Events.Documents.IDocumentQueryExecutor"/> hook by delegating to
///     Polecat's own terminators, so the JasperFx.Events.Documents extension terminators
///     (<c>ToListAsync</c>, <c>CountAsync</c>, <c>AnyAsync</c>, <c>FirstOrDefaultAsync</c>) and
///     <see cref="PolecatQueryableExtensions"/> are the same execution path.
/// </summary>
/// <remarks>
///     Unlike the events queryable there is NO implicit <c>is_archived = 0</c> filter here:
///     archived streams are first-class rows of this surface (<c>Where(x =&gt; x.IsArchived)</c> is
///     part of the contract), and the compaction-policy predicate composes <c>!x.IsArchived</c>
///     explicitly.
/// </remarks>
internal class StreamStateLinqQueryProvider : IPolecatAsyncQueryProvider,
    JasperFx.Events.Documents.IDocumentQueryExecutor
{
    private readonly QuerySession _session;
    private readonly EventGraph _events;
    private readonly string? _tenantId;
    private readonly StreamStateMemberFactory _memberFactory;

    /// <param name="tenantId">
    ///     Explicit tenant scope from <c>QueryStreamStates(tenantId)</c>, already validated by the
    ///     caller (a non-null tenant on a store with no tenant dimension was refused there). Null
    ///     falls back to the session's own tenant, matching every other pc_streams read.
    /// </param>
    public StreamStateLinqQueryProvider(QuerySession session, EventGraph events, string? tenantId)
    {
        _session = session;
        _events = events;
        _tenantId = tenantId;
        _memberFactory = new StreamStateMemberFactory(events);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2046",
        Justification = "IQueryProvider.CreateQuery(Expression) lacks RUC; the AOT-safe entry is the generic CreateQuery<TElement>(Expression).")]
    [UnconditionalSuppressMessage("AOT", "IL3051",
        Justification = "IQueryProvider.CreateQuery(Expression) lacks RDC; the AOT-safe entry is the generic CreateQuery<TElement>(Expression).")]
    [RequiresDynamicCode("Closes PolecatLinqQueryable<> over the element type via Type.MakeGenericType. AOT consumers should call CreateQuery<TElement>(Expression) instead.")]
    [RequiresUnreferencedCode("Activator.CreateInstance reflects over the constructor of PolecatLinqQueryable<>.")]
    public IQueryable CreateQuery(Expression expression)
    {
        return CreateQuery<StreamState>(expression);
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
        var parser = new LinqQueryParser(_memberFactory, _events.StreamsTableName);
        parser.Parse(expression);

        ApplySingleValueMode(parser);

        // Tenant scope: the explicit QueryStreamStates(tenantId) argument wins; otherwise the
        // session's own tenant, exactly like FetchStreamStateAsync — unless the query opted out
        // via AnyTenant()/TenantIsOneOf().
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

        // The stream-query CLI's stated ordering tiebreaks by Id THEN Key so it is deterministic
        // on either identity style — but both members are the one id column here, and SQL Server
        // refuses a column named twice in ORDER BY. Deduping by locator (first occurrence wins) is
        // semantically identity: ordering twice by the same column cannot change the order.
        var seenOrderBys = new HashSet<string>();
        parser.Statement.OrderBys.RemoveAll(orderBy => !seenOrderBys.Add(orderBy.Item1));

        var isScalar = parser.ValueMode is SingleValueMode.Count or SingleValueMode.LongCount
            or SingleValueMode.Any;

        if (!isScalar && parser.SelectExpression == null)
        {
            parser.Statement.SelectColumns = PcStreamsRowReader.SelectColumns;
        }

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
        switch (parser.ValueMode)
        {
            case null:
                return (TResult)(object)await ReadStreamStatesAsync(reader, token);

            case SingleValueMode.First:
            case SingleValueMode.Single:
            case SingleValueMode.Last:
                return await SingleStreamStateAsync<TResult>(reader, token, canBeNull: false);

            case SingleValueMode.FirstOrDefault:
            case SingleValueMode.SingleOrDefault:
            case SingleValueMode.LastOrDefault:
                return await SingleStreamStateAsync<TResult>(reader, token, canBeNull: true);

            case SingleValueMode.Count:
            case SingleValueMode.LongCount:
                var scalarHandler = new ScalarHandler<TResult>();
                return await scalarHandler.HandleAsync(reader, token);

            case SingleValueMode.Any:
                var anyHandler = new AnyHandler();
                var anyResult = await anyHandler.HandleAsync(reader, token);
                return (TResult)(object)anyResult;

            default:
                throw new NotSupportedException(
                    $"Unsupported operation over a stream state query: {parser.ValueMode}");
        }
    }

    private async Task<IReadOnlyList<StreamState>> ReadStreamStatesAsync(
        DbDataReader reader, CancellationToken token)
    {
        var identity = _events.StreamIdentity;
        var results = new List<StreamState>();
        while (await reader.ReadAsync(token))
        {
            results.Add(PcStreamsRowReader.ReadStreamState(reader, identity, _events));
        }

        return results;
    }

    private async Task<TResult> SingleStreamStateAsync<TResult>(
        DbDataReader reader, CancellationToken token, bool canBeNull)
    {
        var states = await ReadStreamStatesAsync(reader, token);
        if (states.Count == 0)
        {
            if (!canBeNull) throw new InvalidOperationException("Sequence contains no elements");
            return default!;
        }

        return (TResult)(object)states[0];
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

            default:
                throw new NotSupportedException(
                    $"Unsupported operation over a stream state query: {parser.ValueMode}");
        }
    }

    // The shared JasperFx.Events.Documents terminators dispatch through these four primitives —
    // same disposition as PolecatLinqQueryProvider: each delegates to Polecat's own terminator so
    // the shared surface and PolecatQueryableExtensions are one execution path.

    Task<IReadOnlyList<T>> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteToListAsync<T>(
        IQueryable<T> queryable, CancellationToken token)
        => queryable.ToListAsync(token);

    Task<T?> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteFirstOrDefaultAsync<T>(
        IQueryable<T> queryable, CancellationToken token) where T : default
        => queryable.FirstOrDefaultAsync(token);

    Task<int> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteCountAsync<T>(
        IQueryable<T> queryable, CancellationToken token)
        => queryable.CountAsync(token);

    Task<bool> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteAnyAsync<T>(
        IQueryable<T> queryable, CancellationToken token)
        => queryable.AnyAsync(token);
}
