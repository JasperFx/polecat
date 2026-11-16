using JasperFx;
using System.Diagnostics.CodeAnalysis;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Linq.CursorPaging;
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
/// <remarks>
///     The LINQ-to-SQL translation walks user-supplied Expression trees that reference
///     document types' members and Queryable / Enumerable method names. The trimmer
///     cannot reason statically about those references. AOT-publishing apps should
///     either avoid LINQ entirely (use the raw <c>session.QueryAsync</c> path with
///     SQL strings) or use source-generated compiled queries.
/// </remarks>
internal class PolecatLinqQueryProvider : IPolecatAsyncQueryProvider,
    JasperFx.Events.Documents.IDocumentQueryExecutor
{
    private readonly QuerySession _session;

    /// <summary>
    ///     The session this queryable was built from, so an operator that has to validate against the
    ///     document's MAPPING before it can build its SQL — <c>OrderByVectorDistance</c> is the one —
    ///     can reach it. Internal: the provider is an implementation detail and this does not widen it.
    /// </summary>
    internal QuerySession Session => _session;
    private readonly DocumentProviderRegistry _providers;
    private readonly DocumentTableEnsurer _tableEnsurer;

    /// <summary>
    ///     polecat#548: the tenant every implicit <c>tenant_id</c> filter is built from. Normally the
    ///     driving session's own tenant; a ForTenant() view passes its override tenant so a LINQ query
    ///     through it is scoped the same way its by-id loads are.
    /// </summary>
    private readonly string? _tenantOverride;

    public PolecatLinqQueryProvider(
        QuerySession session,
        DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer,
        string? tenantOverride = null)
    {
        _session = session;
        _providers = providers;
        _tableEnsurer = tableEnsurer;
        _tenantOverride = tenantOverride;
    }

    private string TenantIdForFilter => _tenantOverride ?? _session.TenantId;

    /// <summary>
    ///     A parsed query carrying its implicit filters. See <see cref="Parse" />.
    /// </summary>
    private readonly record struct ParsedQuery(
        LinqQueryParser Parser, MemberFactory Members, DocumentProvider Provider, Type DocumentType);

    /// <summary>
    ///     The one road into a statement: parses <paramref name="expression" /> and applies the
    ///     <see cref="ImplicitFilters" />, so an execution path cannot end up with a statement that
    ///     lacks them (#836).
    /// </summary>
    /// <remarks>
    ///     A join is the exception, because its filters have two tables to go to.
    ///     <see cref="ExecuteGroupJoinAsync{TResult}" /> asks <see cref="ImplicitFilters" /> for each side.
    /// </remarks>
    private ParsedQuery Parse(Expression expression, string tenantId)
    {
        var documentType = FindDocumentType(expression);
        var provider = _providers.GetProvider(documentType);

        var memberFactory = new MemberFactory(_session.Options, provider.Mapping);
        var parser = new LinqQueryParser(memberFactory, provider.Mapping.QualifiedTableName,
            SoftDeleteTarget.For(provider.Mapping));
        parser.Parse(expression);

        if (parser.GroupJoinData == null)
        {
            parser.Statement.Wheres.AddRange(
                ImplicitFilters.For(provider.Mapping, documentType, _tenantOverride ?? tenantId, parser));
        }

        return new ParsedQuery(parser, memberFactory, provider, documentType);
    }

    /// <summary>
    ///     <see cref="Parse" /> for a query that is about to run: makes sure the table exists and
    ///     honors <c>QueryForNonStaleData()</c>, which is an implicit promise like the filters but a
    ///     wait rather than a predicate. Every path that executes a query starts here.
    /// </summary>
    private async Task<ParsedQuery> PrepareAsync(Expression expression, CancellationToken token)
    {
        var query = Parse(expression, TenantIdForFilter);
        await _tableEnsurer.EnsureTableAsync(query.Provider, _session.HasOpenTransaction, token).ConfigureAwait(false);

        if (query.Parser.NonStaleDataTimeout is { } timeout)
        {
            await WaitForNonStaleDataAsync(timeout, token).ConfigureAwait(false);
        }

        return query;
    }

    /// <summary>
    ///     Refuses a shape a path has no SQL for. These paths read <c>Statement</c> and nothing else,
    ///     so a GroupBy or a join would otherwise be dropped and the rows of the table returned.
    /// </summary>
    private static void AssertNotGroupedOrJoined(LinqQueryParser parser, string operation)
    {
        if (parser.GroupByKeySelector != null || parser.GroupJoinData != null)
        {
            throw new BadLinqExpressionException(
                $"{operation} has no translation for a GroupBy or a GroupJoin, whose rows are not documents. "
                + "Materialize the query with ToListAsync() instead.");
        }
    }

    // IL2046 — IQueryProvider.CreateQuery(Expression) is not annotated [RUC]/[RDC]
    // in the BCL, but the only AOT-safe implementation is the generic
    // CreateQuery<TElement>(Expression) overload below. The non-generic
    // implementation requires runtime codegen + reflection on the element type;
    // the (necessary) annotation mismatch with the interface is suppressed here.
    [UnconditionalSuppressMessage("Trimming", "IL2046",
        Justification = "IQueryProvider.CreateQuery(Expression) lacks RUC; the AOT-safe entry is the generic CreateQuery<TElement>(Expression).")]
    [UnconditionalSuppressMessage("AOT", "IL3051",
        Justification = "IQueryProvider.CreateQuery(Expression) lacks RDC; the AOT-safe entry is the generic CreateQuery<TElement>(Expression).")]
    [RequiresDynamicCode("Closes PolecatLinqQueryable<> over the element type via Type.MakeGenericType. AOT consumers should call CreateQuery<TElement>(Expression) instead.")]
    [RequiresUnreferencedCode("Activator.CreateInstance reflects over the constructor of PolecatLinqQueryable<>. AOT consumers should call CreateQuery<TElement>(Expression) instead.")]
    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = GetElementType(expression);
        var queryableType = typeof(PolecatLinqQueryable<>).MakeGenericType(elementType);
        return (IQueryable)ConstructorCache.Create(queryableType, this, expression);
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

    /// <summary>
    ///     The statement <see cref="ExecuteAsync{TResult}" /> would run for <paramref name="expression" />,
    ///     for <c>ToSql()</c> and the query-plan tests.
    /// </summary>
    /// <remarks>
    ///     gh-1002 (marten#5916): a document query is finished by <see cref="PrepareDocumentStatement" />,
    ///     the same preparation execution uses, so the preview carries the select list execution reads
    ///     with -- <c>data, guid_version</c> for an <c>IVersioned</c> document rather than <c>data</c>.
    /// </remarks>
    internal Statement BuildStatement(Expression expression, string tenantId)
    {
        var query = Parse(expression, tenantId);
        if (query.Parser.GroupJoinData != null || query.Parser.GroupByKeySelector != null)
        {
            return BuildStatement(query);
        }

        PrepareDocumentStatement(query);
        return query.Parser.Statement;
    }

    /// <summary>
    ///     <see cref="BuildStatement(Expression, string)" /> for a query joining a batch, which runs
    ///     alongside the others without this provider's read path.
    /// </summary>
    internal Statement BuildBatchedStatement(Expression expression, string tenantId)
    {
        var query = Parse(expression, tenantId);

        if (query.Parser.NonStaleDataTimeout.HasValue)
        {
            throw new BadLinqExpressionException(
                "QueryForNonStaleData() cannot join a batched query, which has no point at which to wait. "
                + "Run the query on its own.");
        }

        if (query.Parser.GroupByKeySelector != null)
        {
            throw new BadLinqExpressionException(
                "A GroupBy cannot join a batched query, whose rows are read as documents. Run the query "
                + "on its own.");
        }

        return BuildStatement(query);
    }

    private static Statement BuildStatement(ParsedQuery query)
    {
        var parser = query.Parser;

        if (parser.GroupJoinData != null)
        {
            throw new BadLinqExpressionException(
                "A GroupJoin has no single statement to render, so it cannot be used with ToSql() or a "
                + "batched query.");
        }

        if (parser.GroupByKeySelector != null)
        {
            new GroupBySelectBuilder(query.Members).ApplyTo(parser);
            return parser.Statement;
        }

        DistinctProjection.ApplyTo(parser, query.Members);
        ApplySingleValueMode(parser);

        return parser.Statement;
    }

    /// <summary>
    ///     jasperfx#869: translate ONLY what <paramref name="expression" /> adds on top of
    ///     <c>Query&lt;T&gt;()</c> — its <c>Where</c> fragments and its <c>ORDER BY</c> terms — through
    ///     this provider's own parser, without any of the scoping every execution path appends
    ///     (tenant, soft deletes, <c>doc_type</c>, the metadata markers).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         For the document diagnostics surface, which applies operator-typed criteria and owns its
    ///         scoping itself: it has to reach columns no LINQ operator exposes (correlation, causation,
    ///         last-modified-by), and it reports the row's metadata alongside its byte-exact JSON. So the
    ///         provider contributes the translation and the caller the rest of the statement.
    ///     </para>
    ///     <para>
    ///         Anything other than <c>Where</c> / <c>OrderBy</c> / <c>ThenBy</c> on the chain is refused:
    ///         a <c>Take</c>, <c>Select</c> or scoping operator would be silently dropped by a caller that
    ///         only reads the fragments.
    ///     </para>
    /// </remarks>
    /// <exception cref="BadLinqExpressionException">A shape this provider cannot translate.</exception>
    internal Statement TranslateCriteria(Expression expression)
    {
        AssertCriteriaOnly(expression);

        var documentType = FindDocumentType(expression);
        var provider = _providers.GetProvider(documentType);

        var memberFactory = new MemberFactory(_session.Options, provider.Mapping);
        var parser = new LinqQueryParser(memberFactory, provider.Mapping.QualifiedTableName,
            SoftDeleteTarget.For(provider.Mapping));
        parser.Parse(expression);

        return parser.Statement;
    }

    private static void AssertCriteriaOnly(Expression expression)
    {
        while (expression is MethodCallExpression call)
        {
            if (call.Method.DeclaringType != typeof(Queryable)
                || call.Method.Name is not ("Where" or "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending"))
            {
                throw new BadLinqExpressionException(
                    $"Only Where and OrderBy can be translated as document criteria, not {call.Method.Name}.");
            }

            expression = call.Arguments[0];
        }
    }

    internal async Task<string> ExecuteJsonArrayAsync(Expression expression, CancellationToken token)
    {
        var query = await PrepareAsync(expression, token).ConfigureAwait(false);
        var parser = query.Parser;

        if (parser.GroupByKeySelector != null && parser.GroupJoinData == null)
        {
            ApplyStreamedGrouping(query);
        }
        else
        {
            AssertNotGroupedOrJoined(parser, nameof(JsonQueryExtensions.ToJsonArrayAsync));
            parser.Statement.SelectFragment = ResolveStreamingSelect(parser, query.Members);
        }

        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        parser.Statement.Apply(builder);
        builder.Compile();

        await using var reader = (await _session.ExecuteReaderAsync(batch, token).ConfigureAwait(false));

        var sb = new StringBuilder("[");
        var first = true;
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            if (!first) sb.Append(',');
            sb.Append(reader.GetString(0));
            first = false;
        }

        sb.Append(']');
        return sb.ToString();
    }

    /// <summary>
    ///     Reads the first matching document's raw <c>data</c> JSON <em>and</em> its <c>version</c>
    ///     in a single round trip (<c>SELECT TOP(1) data, version …</c>). Used by the ASP.NET Core
    ///     <c>StreamOne</c> ETag path so the version is available without a second metadata query.
    ///     Returns <c>null</c> when the query matches no document.
    /// </summary>
    internal async Task<(string Json, long Version)?> ExecuteJsonFirstWithVersionAsync(
        Expression expression, CancellationToken token)
    {
        var query = await PrepareAsync(expression, token).ConfigureAwait(false);
        var parser = query.Parser;
        AssertNotGroupedOrJoined(parser, nameof(JsonQueryExtensions.ToJsonFirstWithVersionAsync));

        // Read the persisted JSON and the version column together — one round trip, no reserialize.
        // A Select() does not change that: the ETag belongs to the document, which is what the
        // streaming_etag_hardening_tests pin.
        parser.Statement.SelectColumns = "data, version";
        parser.Statement.Limit = 1;

        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        parser.Statement.Apply(builder);
        builder.Compile();

        await using var reader = (await _session.ExecuteReaderAsync(batch, token).ConfigureAwait(false));
        if (!await reader.ReadAsync(token).ConfigureAwait(false)) return null;

        var json = reader.GetString(0);
        var version = reader.GetInt64(1);
        return (json, version);
    }

    /// <summary>
    ///     Streams a single page of raw document JSON plus paging metadata as one JSON envelope
    ///     (<c>{"pageNumber":..,"pageSize":..,"totalItemCount":..,"pageCount":..,"hasNextPage":..,
    ///     "hasPreviousPage":..,"items":[..]}</c>) directly to <paramref name="destination"/> in a
    ///     single database round trip. The total row count rides along on every row via
    ///     <c>COUNT(*) OVER()</c> so no second COUNT query is needed. Raw <c>data</c> column JSON is
    ///     written straight through — no deserialize/reserialize. Mirrors Marten's
    ///     <c>StreamPagedMany</c> (JasperFx/marten#5014). Envelope key names/shape are byte-for-byte
    ///     identical to Marten so clients are interchangeable.
    /// </summary>
    internal async Task StreamPagedJsonArrayAsync(
        Expression expression, int pageNumber, int pageSize, Stream destination, CancellationToken token)
    {
        if (pageNumber < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(pageNumber), pageNumber,
                "pageNumber must be greater than or equal to 1.");
        }

        if (pageSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), pageSize,
                "pageSize must be greater than or equal to 1.");
        }

        var query = await PrepareAsync(expression, token).ConfigureAwait(false);
        var parser = query.Parser;
        AssertNotGroupedOrJoined(parser, nameof(JsonQueryExtensions.StreamPagedJsonArray));

        // Ride the total-rows window column alongside the page so the total comes back on the same
        // round trip. COUNT(*) OVER() returns int on SQL Server. The page itself is whatever the query
        // selects: a Select() here used to be dropped, which handed back the whole document, and a
        // projection is the ordinary way to keep a member out of a response (#836).
        parser.Statement.SelectFragment = new CompositeSqlFragment(
            ResolveStreamingSelect(parser, query.Members), ", COUNT(*) OVER() AS total_rows");
        parser.Statement.Offset = (pageNumber - 1) * pageSize;
        parser.Statement.Limit = pageSize;

        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        parser.Statement.Apply(builder);
        builder.Compile();

        await using var reader = (await _session.ExecuteReaderAsync(batch, token).ConfigureAwait(false));

        // Read the first row up front: the window-function total lives on every row, so a zero-row
        // page (empty match, or paging past the end) simply has no total → totalItemCount = 0.
        var hasFirst = await reader.ReadAsync(token).ConfigureAwait(false);
        var total = hasFirst ? reader.GetInt32(1) : 0;

        var pageCount = total > 0 ? (int)Math.Ceiling(total / (double)pageSize) : 0;
        var hasNextPage = pageNumber < pageCount;
        var hasPreviousPage = pageNumber > 1;

        var prefix =
            $"{{\"pageNumber\":{pageNumber},\"pageSize\":{pageSize},\"totalItemCount\":{total}," +
            $"\"pageCount\":{pageCount}," +
            $"\"hasNextPage\":{(hasNextPage ? "true" : "false")}," +
            $"\"hasPreviousPage\":{(hasPreviousPage ? "true" : "false")}," +
            "\"items\":[";
        await destination.WriteAsync(Encoding.UTF8.GetBytes(prefix), token).ConfigureAwait(false);

        if (hasFirst)
        {
            await destination.WriteAsync(Encoding.UTF8.GetBytes(reader.GetString(0)), token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                await destination.WriteAsync(","u8.ToArray(), token).ConfigureAwait(false);
                await destination.WriteAsync(Encoding.UTF8.GetBytes(reader.GetString(0)), token).ConfigureAwait(false);
            }
        }

        await destination.WriteAsync("]}"u8.ToArray(), token).ConfigureAwait(false);
    }

    /// <summary>
    ///     Executes one keyset (cursor / seek) page: streams the page's raw document JSON and builds
    ///     the continuation cursor from the last row's sort-key values — all in a single round trip
    ///     that also selects the ordering-key columns so the cursor is built without hydrating.
    ///     Mirrors Marten's <c>StreamPagedByCursor</c> mechanics (JasperFx/marten#5016), adapted to
    ///     SQL Server (composite seek predicate + <c>TOP(pageSize + 1)</c> to detect a next page).
    /// </summary>
    internal async Task<CursorPageResult> ExecuteCursorPageJsonAsync(
        Expression expression, string? cursor, int pageSize, CancellationToken token)
    {
        if (pageSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), pageSize,
                "pageSize must be greater than or equal to 1.");
        }

        var query = await PrepareAsync(expression, token).ConfigureAwait(false);
        var parser = query.Parser;
        AssertNotGroupedOrJoined(parser, nameof(CursorPagingQueryableExtensions.ToJsonPageByCursorAsync));

        // The cursor is read off each row's document, and the rows of a Distinct() have none.
        if (parser.IsDistinct)
        {
            throw new BadLinqExpressionException(
                $"{nameof(CursorPagingQueryableExtensions.ToJsonPageByCursorAsync)} has no translation for a "
                + "Distinct(), because a cursor names one document. Page it with Skip and Take instead.");
        }

        // gh-996: the seek predicate and the cursor's keys are the query's own, not a page's.
        if (parser.Statement.InnerPage != null)
        {
            throw new BadLinqExpressionException(
                $"{nameof(CursorPagingQueryableExtensions.ToJsonPageByCursorAsync)} has no translation for a "
                + "query that reorders or filters the rows of a Take() or Skip(). Page it with Skip and Take "
                + "instead.");
        }

        var orderBy = parser.OrderByMembers;
        CursorPagination.ValidateOrdering(orderBy);

        // Select the page's JSON plus each ordering-key column (aliased) so the cursor is built from
        // the last row without hydrating the document. The page is whatever the query selects, so a
        // Select() used to hide a field keeps hiding it (#836); the keys ride alongside it unprojected.
        var select = new List<object>(orderBy.Count + 1) { ResolveStreamingSelect(parser, query.Members) };
        for (var i = 0; i < orderBy.Count; i++)
        {
            select.Add($", {orderBy[i].Member.TypedLocator} AS __pc_k{i}");
        }

        parser.Statement.SelectFragment = new CompositeSqlFragment(select.ToArray());

        // Fetch one extra row to detect whether a next page exists.
        parser.Statement.Limit = pageSize + 1;

        if (cursor != null)
        {
            var values = CursorPagination.Decode(cursor, orderBy);
            parser.Statement.Wheres.Add(CursorPagination.BuildSeekPredicate(orderBy, values));
        }

        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        parser.Statement.Apply(builder);
        builder.Compile();

        await using var reader = (await _session.ExecuteReaderAsync(batch, token).ConfigureAwait(false));

        var page = new StringBuilder("[");
        var count = 0;
        var hasMore = false;
        object?[]? lastKeys = null;
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            // The probe row: there is a next page, and nothing of the row itself is needed.
            if (count == pageSize)
            {
                hasMore = true;
                break;
            }

            if (count > 0) page.Append(',');
            page.Append(reader.GetString(0));
            count++;

            // Only the last row of the page can seed the cursor, so only its keys are read.
            if (count == pageSize)
            {
                lastKeys = new object?[orderBy.Count];
                for (var i = 0; i < orderBy.Count; i++)
                {
                    lastKeys[i] = reader.IsDBNull(i + 1) ? null : reader.GetValue(i + 1);
                }
            }
        }

        page.Append(']');

        var nextCursor = hasMore ? CursorPagination.Encode(lastKeys!) : null;

        return new CursorPageResult(page.ToString(), count, nextCursor);
    }

    /// <summary>
    ///     Resolves the select list for a raw-JSON streaming query (#358). No projection → the raw
    ///     <c>data</c> column. A simple <c>Select(x =&gt; new { … })</c> projection of document members →
    ///     a server-side <see cref="JsonObjectFragment" /> (native-JSON / SQL Server 2025 only). Any
    ///     other projection (scalar select, method calls, arithmetic, non-native store, …) cannot be
    ///     streamed faithfully and throws <see cref="BadLinqExpressionException"/> rather than
    ///     silently returning the raw documents.
    /// </summary>
    private ISqlFragment ResolveStreamingSelect(LinqQueryParser parser, MemberFactory memberFactory)
    {
        if (parser.SelectExpression is null)
        {
            return new LiteralSqlFragment("data");
        }

        var columns = _session.Options.UseNativeJsonType
            ? SelectProjectionAnalyzer.TryAnalyze(parser.SelectExpression, memberFactory)
            : null;

        if (columns is null)
        {
            throw new BadLinqExpressionException(
                "This query cannot be streamed as raw JSON because its Select(...) projection is not a " +
                "simple object of document members. Either project into an anonymous type / DTO composed " +
                "only of document member accesses (e.g. Select(x => new { x.Name, x.Address.City })) on a " +
                "native-JSON store (SQL Server 2025), or materialize the results with ToListAsync() " +
                "instead of a JSON-streaming call.");
        }

        if (parser.IsDistinct) return DistinctProjection.ReadColumns(parser.Statement, columns);

        var members = columns.Select(column => new JsonObjectMember(column.JsonKey,
            new LiteralSqlFragment(column.Locator), column.MemberType, column.SqlType));

        return new CompositeSqlFragment(new JsonObjectFragment(members), " AS data");
    }

    /// <summary>
    ///     A grouped query streamed as JSON: its rows already are JSON objects when it projects one.
    /// </summary>
    private static void ApplyStreamedGrouping(ParsedQuery query)
    {
        var grouping = new GroupBySelectBuilder(query.Members);
        grouping.ApplyTo(query.Parser);

        if (!grouping.ReadsJson)
        {
            throw new BadLinqExpressionException(
                "A GroupBy can only be streamed as raw JSON when its Select(...) projects an object, "
                + "e.g. Select(g => new { g.Key, Count = g.Count() }). Materialize a scalar projection with "
                + "ToListAsync() instead.");
        }
    }

    [RequiresDynamicCode("LINQ-to-SQL execution closes ListQueryHandler<>/DeserializingSelector<>/etc. over the document type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("LINQ-to-SQL execution reflects over the document type (Activator.CreateInstance on handler types, MethodInfo.Invoke on HandleAsync). AOT consumers must preserve handler + document members through DAM or source generation.")]
    public async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken token)
    {
        var query = await PrepareAsync(expression, token).ConfigureAwait(false);
        var parser = query.Parser;
        var provider = query.Provider;
        var documentType = query.DocumentType;

        // Route to GroupJoin execution if detected
        if (parser.GroupJoinData != null)
        {
            return await ExecuteGroupJoinAsync<TResult>(parser, token).ConfigureAwait(false);
        }

        // Route to GroupBy execution if detected
        if (parser.GroupByKeySelector != null)
        {
            return await ExecuteGroupByAsync<TResult>(query, token).ConfigureAwait(false);
        }

        var shape = PrepareDocumentStatement(query);

        // Build SQL and execute via BatchBuilder/SqlBatch
        await using var batch = CompileStatement(parser);
        await using var reader = (await _session.ExecuteReaderAsync(batch, token).ConfigureAwait(false));

        return await HandleResultAsync<TResult>(reader, parser, documentType, token, shape.SyncRevision,
            shape.SyncGuidVersion, shape.HierarchyMapping, shape.SharedSelectClause).ConfigureAwait(false);
    }

    /// <summary>How the rows of a document query (not a GroupBy or a GroupJoin) are materialized.</summary>
    private sealed record DocumentStatementShape(
        bool SyncRevision,
        bool SyncGuidVersion,
        DocumentMapping? HierarchyMapping,
        Weasel.Storage.ISelectClause? SharedSelectClause);

    /// <summary>
    ///     Finishes the statement of a document query, shared by <see cref="ExecuteAsync{TResult}" /> and
    ///     <see cref="ExecuteStreamAsync{T}" /> so the two can't select different columns.
    /// </summary>
    private DocumentStatementShape PrepareDocumentStatement(ParsedQuery query)
    {
        var parser = query.Parser;
        var provider = query.Provider;

        // Apply Distinct, which needs the select list single-value mode replaces
        DistinctProjection.ApplyTo(parser, query.Members);

        // Apply single-value mode to Statement
        ApplySingleValueMode(parser);

        // Materialization shape (#273 E2d/E2e). Whole-document queries ride the closed-shape
        // QueryOnly storage: SELECT columns come from the shared select clause (data first,
        // then the QueryOnlyReadBinders' columns in binder order) and rows resolve through
        // the shared QueryOnly selector — for subclasses that is a SubClassPolecatStorage
        // downcasting over the root's hierarchical selector.
        Weasel.Storage.ISelectClause? sharedSelectClause = null;
        if (parser.Statement.SelectColumns == "data" && parser.Statement.DistinctColumns == null)
        {
            sharedSelectClause = _providers.ClosedShapeGraph.QueryOnlySelectClauseFor(query.DocumentType);
            parser.Statement.SelectColumns = string.Join(", ", sharedSelectClause.SelectFields());
        }

        return new DocumentStatementShape(
            provider.Mapping.UseNumericRevisions,
            provider.Mapping.UseOptimisticConcurrency,
            provider.Mapping.IsHierarchy() ? provider.Mapping : null,
            sharedSelectClause);
    }

    private static SqlBatch CompileStatement(LinqQueryParser parser)
    {
        var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        parser.Statement.Apply(builder);
        builder.Compile();
        return batch;
    }

    /// <summary>
    ///     #801: runs the query and hands its rows out as they are read, for
    ///     <see cref="PolecatQueryableExtensions.ToAsyncEnumerable{T}" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Every shape <c>ToListAsync()</c> reads row by row streams: whole documents, scalar and
    ///         projected <c>Select()</c>s, <c>Distinct()</c> and <c>GroupBy()</c>. A <c>GroupJoin</c> does
    ///         not: it gathers each outer row's inner rows, so it has read them all before it can return
    ///         one, and it is materialized and then handed out.
    ///     </para>
    ///     <para>
    ///         The reader stays open, holding the session's connection, until the enumeration ends or is
    ///         disposed. Opening it goes through the session's resilience pipeline like any other read; a
    ///         failure part-way through a stream is not retried, because rows have already been handed out.
    ///     </para>
    /// </remarks>
    [RequiresDynamicCode("Streaming LINQ execution closes ListQueryHandler<>/ProjectionListHandler<,>/DeserializingSelector<> over the document type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Streaming LINQ execution reflects over handler and selector types (Activator.CreateInstance).")]
    public async IAsyncEnumerable<T> ExecuteStreamAsync<T>(Expression expression,
        [EnumeratorCancellation] CancellationToken token)
    {
        var query = await PrepareAsync(expression, token).ConfigureAwait(false);
        var parser = query.Parser;

        if (parser.GroupJoinData != null)
        {
            var joined = await ExecuteGroupJoinAsync<IReadOnlyList<T>>(parser, token).ConfigureAwait(false);
            foreach (var item in joined)
            {
                yield return item;
            }

            yield break;
        }

        IStreamingQueryHandler<T> handler;
        if (parser.GroupByKeySelector != null)
        {
            var grouping = new GroupBySelectBuilder(query.Members);
            grouping.ApplyTo(parser);
            handler = grouping.ReadsJson
                ? new GroupByListHandler<T>(_session.Serializer)
                : new ScalarListHandler<T>();
        }
        else
        {
            var shape = PrepareDocumentStatement(query);
            handler = StreamingHandlerFor<T>(parser, query.DocumentType, shape);
        }

        if (parser.ValueMode != null)
        {
            // Unreachable through ToAsyncEnumerable, whose expression has no terminal operator.
            throw new BadLinqExpressionException(
                $"ToAsyncEnumerable() cannot stream a query ending in {parser.ValueMode}.");
        }

        await using var batch = CompileStatement(parser);
        await using var reader = (await _session.ExecuteReaderAsync(batch, token).ConfigureAwait(false));

        await foreach (var item in handler.StreamAsync(reader, token).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    /// <summary>The row reader for a document query's rows, mirroring <see cref="HandleResultAsync{TResult}" />.</summary>
    [RequiresDynamicCode("Closes ListQueryHandler<>/ProjectionListHandler<,>/DeserializingSelector<> over the document type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Activator.CreateInstance over handler and selector types.")]
    private IStreamingQueryHandler<T> StreamingHandlerFor<T>(LinqQueryParser parser, Type documentType,
        DocumentStatementShape shape)
    {
        if (parser.SelectExpression != null)
        {
            if (parser.IsScalarSelect)
            {
                return new ScalarListHandler<T>();
            }

            // Distinct rows are a JSON object each, like a GroupBy's: see DistinctProjection
            if (parser.Statement.DistinctColumns != null)
            {
                return new GroupByListHandler<T>(_session.Serializer);
            }

            var projectionType = typeof(ProjectionListHandler<,>).MakeGenericType(documentType, typeof(T));
            return (IStreamingQueryHandler<T>)Activator.CreateInstance(projectionType, _session.Serializer,
                parser.SelectExpression)!;
        }

        var selector = DocumentSelectorFor(documentType, shape.SyncRevision, shape.SyncGuidVersion,
            shape.HierarchyMapping, shape.SharedSelectClause);
        var listType = typeof(ListQueryHandler<>).MakeGenericType(documentType);

        // IStreamingQueryHandler is covariant, so a handler over a document type streams as any type it
        // derives from.
        return (IStreamingQueryHandler<T>)Activator.CreateInstance(listType, selector)!;
    }

    [RequiresDynamicCode("Closes DeserializingSelector<> over itemType via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Activator.CreateInstance over the selector type.")]
    private object DocumentSelectorFor(Type itemType, bool syncRevision, bool syncGuidVersion,
        DocumentMapping? hierarchyMapping, Weasel.Storage.ISelectClause? sharedSelectClause)
    {
        // #273 E2d: root documents materialize through the shared QueryOnly selector,
        // whose layout matches the SELECT columns the shared select clause produced.
        if (sharedSelectClause != null)
        {
            return sharedSelectClause.BuildSelector((Weasel.Storage.IStorageSession)_session);
        }

        var selectorType = typeof(DeserializingSelector<>).MakeGenericType(itemType);
        return ConstructorCache.Create(selectorType, _session.Serializer, syncRevision, syncGuidVersion,
            hierarchyMapping);
    }

    [RequiresDynamicCode("GroupBy execution closes GroupByListHandler<>/ScalarListHandler<> over the projected type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("GroupBy execution reflects over handler types (Activator.CreateInstance + MethodInfo.Invoke).")]
    private async Task<TResult> ExecuteGroupByAsync<TResult>(ParsedQuery query, CancellationToken token)
    {
        var parser = query.Parser;
        var grouping = new GroupBySelectBuilder(query.Members);
        grouping.ApplyTo(parser);

        // Build SQL and execute
        await using var batch = new SqlBatch();
        var batchBuilder = new BatchBuilder(batch);
        parser.Statement.Apply(batchBuilder);
        batchBuilder.Compile();

        await using var reader = (await _session.ExecuteReaderAsync(batch, token).ConfigureAwait(false));

        switch (parser.ValueMode)
        {
            case null:
                // Check if the select is a complex JSON projection or scalar
                return grouping.ReadsJson
                    ? await InvokeGroupByListHandlerAsync<TResult>(reader, parser.SelectExpression!, token).ConfigureAwait(false)
                    : await InvokeScalarListHandlerAsync<TResult>(reader, token).ConfigureAwait(false);

            case SingleValueMode.Any:
                return (TResult)(object)await new AnyHandler().HandleAsync(reader, token).ConfigureAwait(false);

            case SingleValueMode.Count:
            case SingleValueMode.LongCount:
            case SingleValueMode.Sum:
            case SingleValueMode.Min:
            case SingleValueMode.Max:
            case SingleValueMode.Average:
                return await new ScalarHandler<TResult>().HandleAsync(reader, token).ConfigureAwait(false);

            default:
                return await ReadOneRowAsync<TResult>(reader, parser.ValueMode.Value, grouping.ReadsJson, token).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     First, Single and Last over rows that are not documents: a scalar in the first column, or a
    ///     JSON object. The rows are closed over <typeparamref name="TResult" /> statically, so nothing
    ///     has to be at runtime, and a <c>TOP</c> has already bounded how many there are.
    /// </summary>
    [RequiresDynamicCode("Deserializes a projected row via ISerializer.FromJson.")]
    [RequiresUnreferencedCode("Deserializes a projected row via ISerializer.FromJson.")]
    private async Task<TResult> ReadOneRowAsync<TResult>(
        DbDataReader reader, SingleValueMode mode, bool readsJson, CancellationToken token)
    {
        var rows = readsJson
            ? await new GroupByListHandler<TResult>(_session.Serializer).HandleAsync(reader, token).ConfigureAwait(false)
            : await new ScalarListHandler<TResult>().HandleAsync(reader, token).ConfigureAwait(false);

        return mode.OneOf(rows);
    }

    [RequiresDynamicCode("Closes GroupByListHandler<> over TResult's element type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Reflects over GroupByListHandler<>.HandleAsync via MethodInfo.Invoke and Task<>.Result via GetProperty.")]
    private async Task<TResult> InvokeGroupByListHandlerAsync<TResult>(
        System.Data.Common.DbDataReader reader, LambdaExpression selectExpression,
        CancellationToken token)
    {
        // TResult is IReadOnlyList<TElement>
        var elementType = typeof(TResult).GetGenericArguments()[0];
        var handlerType = typeof(QueryHandlers.GroupByListHandler<>).MakeGenericType(elementType);
                // #741: the non-generic IQueryHandler, not reflection. BOTH
        // handlerType.GetMethod("HandleAsync")! and task.GetType().GetProperty("Result")!
        // return null for a trimmed member rather than throwing, so each `!` was a latent
        // NullReferenceException under Native AOT -- waiting for whichever query shape got
        // here first.
        var handler = (QueryHandlers.IQueryHandler)ConstructorCache.Create(handlerType, _session.Serializer);

        return (TResult)(await handler.HandleAsObjectAsync(reader, token).ConfigureAwait(false))!;
    }

    [RequiresDynamicCode("GroupJoin execution closes JoinListHandler<,,> + Func<,,> over outer/inner/result types via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("GroupJoin execution reflects over handler types and the result selector. AOT consumers must preserve outer/inner/result type members through DAM or source generation.")]
    private async Task<TResult> ExecuteGroupJoinAsync<TResult>(LinqQueryParser parser, CancellationToken token)
    {
        var joinData = parser.GroupJoinData!;

        // Resolve providers for both sides
        var outerProvider = _providers.GetProvider(joinData.OuterElementType);
        var innerProvider = _providers.GetProvider(joinData.InnerElementType);
        await _tableEnsurer.EnsureTableAsync(outerProvider, _session.HasOpenTransaction, token).ConfigureAwait(false);
        await _tableEnsurer.EnsureTableAsync(innerProvider, _session.HasOpenTransaction, token).ConfigureAwait(false);

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

        // The implicit filters go on one side at a time: since #682 the two sides of a join can
        // disagree about tenancy -- a conjoined document joined to a single-tenanted lookup table is the
        // ordinary case -- and each side has its own soft deletes and its own subclass. The inner side
        // can carry operators of its own, but AnyTenant()/TenantIsOneOf() on the outer side has always
        // covered the whole query.
        var inner = ParseInnerSource(joinData, innerMapping, innerMemberFactory);
        var innerTenancy = parser.IsAnyTenant || parser.TenantIds != null ? parser : inner;

        joinStatement.OuterWheres.AddRange(ImplicitFilters.For(outerMapping, joinData.OuterElementType,
            TenantIdForFilter, parser, alias: "outer_t"));
        joinStatement.InnerWheres.AddRange(ImplicitFilters.For(innerMapping, joinData.InnerElementType,
            TenantIdForFilter, inner, innerTenancy, alias: "inner_t"));

        // Add any WHERE clauses from the outer source (captured by parser before GroupJoin).
        // These use bare column names (data, id) — wrap them with aliasing for outer_t.
        foreach (var where in parser.Statement.Wheres)
        {
            joinStatement.OuterWheres.Add(new AliasedSqlFragment(where, "outer_t"));
        }

        foreach (var where in inner.Statement.Wheres)
        {
            joinStatement.InnerWheres.Add(new AliasedSqlFragment(where, "inner_t"));
        }

        var resultSelector = joinData.SelectManyResultSelector ?? throw GroupJoinData.MissingResultSelector();
        var rowSelector = JoinedRows.OnTheDocuments(joinData, resultSelector);
        var sides = new JoinSides(rowSelector, outerMemberFactory, innerMemberFactory);

        // gh-1001: a Where() after the join, or the predicate of a First()/Count()/Any(), filters the side
        // its members come from. It used to be resolved against the outer document whatever it named.
        foreach (var predicate in joinData.PostJoinPredicates)
        {
            sides.AddPredicate(joinData, predicate, joinStatement);
        }

        // gh-1001: the members of an ordering resolve through the projection too. They used to be rebuilt
        // on the GroupJoin's anonymous row, so x.c.Number read '$.c.number' -- null for every row.
        foreach (var (orderByLambda, descending) in joinData.OrderByExpressions)
        {
            var ordering = JoinedRows.OnTheDocuments(joinData, orderByLambda);

            // gh-999: a key that is not a member of one of the two documents has no exact translation. It used
            // to be skipped without a word, so the rows came back in whatever order SQL Server chose; a cast
            // was stepped over whether or not it changed the order.
            var key = LinqQueryParser.StripOrderPreservingConvert(ordering.Body);
            if (!sides.TryResolve(key, ordering, out var member, out var alias))
            {
                throw LinqQueryParser.RefuseOrderingKey(key);
            }

            var locator = JoinStatement.AliasLocator(member.TypedLocator, alias);

            // gh-997: the keys arrive most significant first, and SQL Server refuses a column named
            // twice in ORDER BY. A repeated key can never break a tie, so it is dropped.
            if (!joinStatement.OrderBys.Exists(o => o.Locator == locator))
            {
                joinStatement.OrderBys.Add((locator, descending));
            }
        }

        joinStatement.Limit = parser.Statement.Limit;
        joinStatement.Offset = parser.Statement.Offset;

        switch (parser.ValueMode)
        {
            case SingleValueMode.Count:
                joinStatement.Aggregate = JoinAggregate.Count;
                break;
            case SingleValueMode.LongCount:
                joinStatement.Aggregate = JoinAggregate.LongCount;
                break;
            case SingleValueMode.Any:
                joinStatement.Aggregate = JoinAggregate.Any;
                break;
            case SingleValueMode.First:
            case SingleValueMode.FirstOrDefault:
                joinStatement.Limit = Math.Min(joinStatement.Limit ?? 1, 1);
                break;
            case SingleValueMode.Single:
            case SingleValueMode.SingleOrDefault:
                joinStatement.Limit = Math.Min(joinStatement.Limit ?? 2, 2);
                break;
        }

        // gh-1001: Distinct() compares the projected rows, so they are read from a derived table of the
        // projected columns. It used to be ignored. Whether there is a row does not depend on it.
        var distinct = parser.IsDistinct && parser.ValueMode != SingleValueMode.Any
            ? sides.ApplyDistinct(joinStatement)
            : (JoinSides.DistinctShape?)null;

        // Build SQL and execute
        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        joinStatement.Apply(builder);
        builder.Compile();

        await using var reader = (await _session.ExecuteReaderAsync(batch, token).ConfigureAwait(false));

        // Handle scalar results (Count, Any)
        if (parser.ValueMode is SingleValueMode.Count or SingleValueMode.LongCount or SingleValueMode.Any)
        {
            var scalarHandler = new ScalarHandler<TResult>();
            return await scalarHandler.HandleAsync(reader, token).ConfigureAwait(false);
        }

        if (distinct != null)
        {
            return await ReadDistinctJoinRowsAsync<TResult>(distinct.Value, reader, parser.ValueMode, token)
                .ConfigureAwait(false);
        }

        // Use reflection to invoke the generic handler
        return await InvokeJoinHandlerAsync<TResult>(
            joinData.OuterElementType, joinData.InnerElementType,
            rowSelector, joinData.IsLeftJoin,
            reader, parser.ValueMode, token).ConfigureAwait(false);
    }

    [RequiresDynamicCode("Closes GroupByListHandler<>/ScalarListHandler<> over the projected type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Deserializes the projected rows of a distinct join.")]
    private async Task<TResult> ReadDistinctJoinRowsAsync<TResult>(
        JoinSides.DistinctShape shape, DbDataReader reader, SingleValueMode? valueMode, CancellationToken token)
    {
        var elementType = ResultElementType<TResult>();
        var handlerType = shape == JoinSides.DistinctShape.Object
            ? typeof(GroupByListHandler<>).MakeGenericType(elementType)
            : typeof(ScalarListHandler<>).MakeGenericType(elementType);

        var handler = shape == JoinSides.DistinctShape.Object
            ? (QueryHandlers.IQueryHandler)ConstructorCache.Create(handlerType, _session.Serializer)
            : (QueryHandlers.IQueryHandler)ConstructorCache.Create(handlerType);

        var list = (await handler.HandleAsObjectAsync(reader, token).ConfigureAwait(false))!;
        return SingleOrList<TResult>((System.Collections.IList)list, valueMode);
    }

    /// <summary>
    ///     Parses the operators on the inner side of a join, which used to be read for its element
    ///     type and otherwise dropped, so a <c>Where</c> there filtered nothing.
    /// </summary>
    private static LinqQueryParser ParseInnerSource(
        GroupJoinData joinData, DocumentMapping innerMapping, MemberFactory innerMemberFactory)
    {
        var inner = new LinqQueryParser(innerMemberFactory, innerMapping.QualifiedTableName,
            SoftDeleteTarget.For(innerMapping));
        inner.Parse(joinData.InnerSourceExpression);

        if (inner.Statement.Limit.HasValue || inner.Statement.Offset.HasValue
            || inner.Statement.InnerPage != null || inner.ValueMode.HasValue
            || inner.SelectExpression != null || inner.GroupByKeySelector != null || inner.GroupJoinData != null)
        {
            throw new BadLinqExpressionException(
                "The inner side of a GroupJoin can only filter: a Take, Skip, Select or GroupBy there has "
                + "no place in the JOIN. Apply those after the join.");
        }

        return inner;
    }

    private static string ResolveKeyLocator(Expression keyBody, MemberFactory memberFactory)
    {
        if (keyBody is MemberExpression memberExpr)
        {
            var member = memberFactory.ResolveMember(memberExpr);
            return member.TypedLocator;
        }

        throw new BadLinqExpressionException(
            $"Polecat cannot translate the join key selector '{keyBody}' to SQL ({keyBody.NodeType}). "
            + "Join on a member of each side, e.g. o => o.CustomerId.");
    }

    [RequiresDynamicCode("Closes Func<,,> + JoinListHandler<,,> over outer/inner/result types via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Reflects over JoinListHandler<,,>.HandleAsync via MethodInfo.Invoke and Task<>.Result via GetProperty.")]
    private async Task<TResult> InvokeJoinHandlerAsync<TResult>(
        Type outerType, Type innerType,
        LambdaExpression rewrittenSelector, bool isLeftJoin,
        DbDataReader reader, SingleValueMode? valueMode,
        CancellationToken token)
    {
        var resultElementType = ResultElementType<TResult>();

        // Compile the rewritten selector to a Func<TOuter, TInner?, TResultElement>
        var funcType = typeof(Func<,,>).MakeGenericType(outerType, innerType, resultElementType);
        var compiledSelector = rewrittenSelector.Compile();

        // Create and invoke JoinListHandler<TOuter, TInner, TResultElement>
        var handlerType = typeof(JoinListHandler<,,>).MakeGenericType(outerType, innerType, resultElementType);
                // #741: the non-generic IQueryHandler, not reflection. BOTH
        // handlerType.GetMethod("HandleAsync")! and task.GetType().GetProperty("Result")!
        // return null for a trimmed member rather than throwing, so each `!` was a latent
        // NullReferenceException under Native AOT -- waiting for whichever query shape got
        // here first.
        var handler = (QueryHandlers.IQueryHandler)ConstructorCache.Create(handlerType, _session.Serializer, compiledSelector, isLeftJoin);

        var list = (await handler.HandleAsObjectAsync(reader, token).ConfigureAwait(false))!;
        return SingleOrList<TResult>((System.Collections.IList)list, valueMode);
    }

    /// <summary>TResult is IReadOnlyList&lt;TElement&gt; for list results, or TElement for single results.</summary>
    private static Type ResultElementType<TResult>()
        => typeof(TResult).IsGenericType && typeof(TResult).GetGenericTypeDefinition() == typeof(IReadOnlyList<>)
            ? typeof(TResult).GetGenericArguments()[0]
            : typeof(TResult);

    /// <summary>The joined rows, or for a First()/Single() the one row they answer.</summary>
    private static TResult SingleOrList<TResult>(System.Collections.IList items, SingleValueMode? valueMode)
    {
        if (valueMode is not (SingleValueMode.First or SingleValueMode.FirstOrDefault
            or SingleValueMode.Single or SingleValueMode.SingleOrDefault))
        {
            return (TResult)items;
        }

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

    private static void ApplySingleValueMode(LinqQueryParser parser)
    {
        if (parser.ValueMode == null) return;

        var statement = parser.Statement;

        // Already an aggregate over a derived table: see LinqQueryParser.AggregateOverDerivedTable.
        if (statement.OuterAggregate != null) return;

        switch (parser.ValueMode)
        {
            // gh-995: a Take() in front narrows these rather than being replaced by them, so Take(0).First()
            // has no row and Take(1).Single() has exactly one.
            case SingleValueMode.First:
            case SingleValueMode.FirstOrDefault:
                statement.Limit = Math.Min(statement.Limit ?? 1, 1);
                break;

            case SingleValueMode.Single:
            case SingleValueMode.SingleOrDefault:
                // Fetch 2 rows to detect "more than one element"
                statement.Limit = Math.Min(statement.Limit ?? 2, 2);
                break;

            case SingleValueMode.Last:
            case SingleValueMode.LastOrDefault:
                statement.ReadLastRow();
                break;

            // gh-995: over a page these aggregate a derived table of the page; see Statement.SelectAggregate.
            case SingleValueMode.Count:
                statement.SelectAggregate(_ => "COUNT(*)", "1");
                break;

            case SingleValueMode.LongCount:
                statement.SelectAggregate(_ => "CAST(COUNT(*) AS bigint)", "1");
                break;

            case SingleValueMode.Any:
                statement.SelectColumns = "1";
                // gh-995: Take(0).Any() is false whatever the table holds, so its TOP(0) is kept rather than
                // widened. A Skip is left alone: one row past it is exactly what Skip(k).Any() asks.
                statement.Limit = statement.Limit == 0 ? 0 : 1;
                statement.IsExistsWrapper = true;
                break;

            case SingleValueMode.Sum:
                statement.SelectAggregate(v => $"ISNULL(SUM({v}), 0)",
                    parser.AggregationMember?.TypedLocator ?? "data");
                break;

            case SingleValueMode.Min:
                statement.SelectAggregate(v => $"MIN({v})", parser.AggregationMember?.ValueLocator() ?? "data");
                break;

            case SingleValueMode.Max:
                statement.SelectAggregate(v => $"MAX({v})", parser.AggregationMember?.ValueLocator() ?? "data");
                break;

            case SingleValueMode.Average:
                statement.SelectAggregate(v => $"AVG(CAST({v} AS float))",
                    parser.AggregationMember?.TypedLocator ?? "data");
                break;
        }
    }

    [RequiresDynamicCode("Routes to handler invocations that close generic handler types over the document/result type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Routes to handler invocations that reflect over handler/result types.")]
    private async Task<TResult> HandleResultAsync<TResult>(
        DbDataReader reader, LinqQueryParser parser, Type documentType, CancellationToken token,
        bool syncRevision = false, bool syncGuidVersion = false, DocumentMapping? hierarchyMapping = null,
        Weasel.Storage.ISelectClause? sharedSelectClause = null)
    {
        if (parser.ValueMode == null)
        {
            // Check for Select projection
            if (parser.SelectExpression != null)
            {
                if (parser.IsScalarSelect)
                {
                    // Scalar select: read values directly from the column
                    return await InvokeScalarListHandlerAsync<TResult>(reader, token).ConfigureAwait(false);
                }

                // Distinct rows are a JSON object each, like a GroupBy's: see DistinctProjection
                if (parser.Statement.DistinctColumns != null)
                {
                    return await InvokeGroupByListHandlerAsync<TResult>(reader, parser.SelectExpression, token).ConfigureAwait(false);
                }

                // Complex projection: read documents then project
                return await InvokeProjectionHandlerAsync<TResult>(documentType, reader, parser.SelectExpression, token).ConfigureAwait(false);
            }

            // Plain list result
            return await InvokeListHandlerAsync<TResult>(documentType, reader, token, syncRevision, syncGuidVersion,
                hierarchyMapping, sharedSelectClause).ConfigureAwait(false);
        }

        // #916: a Select's row is whatever it projected. Read as the document, a scalar failed as
        // malformed JSON and everything else as a bad cast.
        if (parser.SelectExpression != null && parser.ValueMode.Value.TakesOneRow())
        {
            var readsJson = parser.Statement.DistinctColumns != null;

            return parser.IsScalarSelect || readsJson
                ? await ReadOneRowAsync<TResult>(reader, parser.ValueMode.Value, readsJson, token).ConfigureAwait(false)
                : parser.ValueMode.Value.OneOf(await InvokeProjectionHandlerAsync<IReadOnlyList<TResult>>(
                    documentType, reader, parser.SelectExpression, token).ConfigureAwait(false));
        }

        switch (parser.ValueMode)
        {
            case SingleValueMode.First:
            case SingleValueMode.Single:
            case SingleValueMode.Last:
                return await InvokeOneResultHandlerAsync<TResult>(
                    documentType, reader, token, canBeNull: false,
                    canBeMultiples: parser.ValueMode == SingleValueMode.First || parser.ValueMode == SingleValueMode.Last,
                    syncRevision: syncRevision, syncGuidVersion: syncGuidVersion, hierarchyMapping: hierarchyMapping,
                    sharedSelectClause: sharedSelectClause).ConfigureAwait(false);

            case SingleValueMode.FirstOrDefault:
            case SingleValueMode.SingleOrDefault:
            case SingleValueMode.LastOrDefault:
                return await InvokeOneResultHandlerAsync<TResult>(
                    documentType, reader, token, canBeNull: true,
                    canBeMultiples: parser.ValueMode != SingleValueMode.SingleOrDefault,
                    syncRevision: syncRevision, syncGuidVersion: syncGuidVersion, hierarchyMapping: hierarchyMapping,
                    sharedSelectClause: sharedSelectClause).ConfigureAwait(false);

            case SingleValueMode.Count:
            case SingleValueMode.LongCount:
            case SingleValueMode.Sum:
            case SingleValueMode.Min:
            case SingleValueMode.Max:
            case SingleValueMode.Average:
                var scalarHandler = new ScalarHandler<TResult>();
                return await scalarHandler.HandleAsync(reader, token).ConfigureAwait(false);

            case SingleValueMode.Any:
                var anyHandler = new AnyHandler();
                var anyResult = await anyHandler.HandleAsync(reader, token).ConfigureAwait(false);
                return (TResult)(object)anyResult;

            default:
                throw new NotSupportedException($"Unsupported single value mode: {parser.ValueMode}");
        }
    }

    [RequiresDynamicCode("Closes DeserializingSelector<> + ListQueryHandler<> over itemType via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Reflects over handler/selector types (Activator.CreateInstance, MethodInfo.Invoke on HandleAsync, GetProperty on Task<>.Result).")]
    private async Task<TResult> InvokeListHandlerAsync<TResult>(
        Type itemType, DbDataReader reader, CancellationToken token,
        bool syncRevision = false, bool syncGuidVersion = false, DocumentMapping? hierarchyMapping = null,
        Weasel.Storage.ISelectClause? sharedSelectClause = null)
    {
        var selector = DocumentSelectorFor(itemType, syncRevision, syncGuidVersion, hierarchyMapping,
            sharedSelectClause);
        var handlerType = typeof(ListQueryHandler<>).MakeGenericType(itemType);

        // #741: cast to the NON-GENERIC IQueryHandler rather than reflecting for HandleAsync.
        // GetMethod returns null for a trimmed member, and the `!` after it was every LINQ query's
        // NullReferenceException under Native AOT. MakeGenericType over itemType alone is fine --
        // one reference-type argument shares a canonical body.
        var handler = (QueryHandlers.IQueryHandler)Activator.CreateInstance(handlerType, selector)!;
        return (TResult)(await handler.HandleAsObjectAsync(reader, token).ConfigureAwait(false))!;
    }


    [RequiresDynamicCode("Closes ScalarListHandler<> over TResult's element type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Reflects over ScalarListHandler<>.HandleAsync via MethodInfo.Invoke and Task<>.Result via GetProperty.")]
    private async Task<TResult> InvokeScalarListHandlerAsync<TResult>(
        DbDataReader reader, CancellationToken token)
    {
        // TResult is IReadOnlyList<TScalar>, extract TScalar
        var scalarType = typeof(TResult).GetGenericArguments()[0];

        // #743: an instantiation ToListAsync<T> rooted statically first, so a VALUE-type scalar has native
        // code under AOT; MakeGenericType only for a path no generic call site reached. See
        // ScalarListHandlers.
        // #741: the non-generic IQueryHandler, not reflection. BOTH
        // handlerType.GetMethod("HandleAsync")! and task.GetType().GetProperty("Result")!
        // return null for a trimmed member rather than throwing, so each `!` was a latent
        // NullReferenceException under Native AOT -- waiting for whichever query shape got
        // here first.
        var handler = ScalarListHandlers.TryCreate(scalarType)
                      ?? (QueryHandlers.IQueryHandler)Activator.CreateInstance(
                          typeof(ScalarListHandler<>).MakeGenericType(scalarType))!;

        return (TResult)(await handler.HandleAsObjectAsync(reader, token).ConfigureAwait(false))!;
    }

    [RequiresDynamicCode("Closes ProjectionListHandler<,> over (sourceType, TResult's projected type) via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Reflects over ProjectionListHandler<,>.HandleAsync via MethodInfo.Invoke and Task<>.Result via GetProperty.")]
    private async Task<TResult> InvokeProjectionHandlerAsync<TResult>(
        Type sourceType, DbDataReader reader, LambdaExpression selectExpression,
        CancellationToken token)
    {
        // TResult is IReadOnlyList<TProjected>
        var projectedType = typeof(TResult).GetGenericArguments()[0];
        var handlerType = typeof(ProjectionListHandler<,>).MakeGenericType(sourceType, projectedType);
                // #741: the non-generic IQueryHandler, not reflection. BOTH
        // handlerType.GetMethod("HandleAsync")! and task.GetType().GetProperty("Result")!
        // return null for a trimmed member rather than throwing, so each `!` was a latent
        // NullReferenceException under Native AOT -- waiting for whichever query shape got
        // here first.
        var handler = (QueryHandlers.IQueryHandler)Activator.CreateInstance(handlerType, _session.Serializer, selectExpression)!;

        return (TResult)(await handler.HandleAsObjectAsync(reader, token).ConfigureAwait(false))!;
    }

    [RequiresDynamicCode("Closes DeserializingSelector<> + OneResultHandler<> over documentType via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("Reflects over handler/selector types (Activator.CreateInstance, MethodInfo.Invoke on HandleAsync, GetProperty on Task<>.Result).")]
    private async Task<TResult> InvokeOneResultHandlerAsync<TResult>(
        Type documentType, DbDataReader reader, CancellationToken token,
        bool canBeNull, bool canBeMultiples,
        bool syncRevision = false, bool syncGuidVersion = false, DocumentMapping? hierarchyMapping = null,
        Weasel.Storage.ISelectClause? sharedSelectClause = null)
    {
        // #273 E2d: root documents materialize through the shared QueryOnly selector.
        object selector;
        if (sharedSelectClause != null)
        {
            selector = sharedSelectClause.BuildSelector((Weasel.Storage.IStorageSession)_session);
        }
        else
        {
            var selectorType = typeof(DeserializingSelector<>).MakeGenericType(documentType);
            selector = ConstructorCache.Create(selectorType, _session.Serializer, syncRevision, syncGuidVersion,
                hierarchyMapping);
        }

        var handlerType = typeof(OneResultHandler<>).MakeGenericType(documentType);
                // #741: the non-generic IQueryHandler, not reflection. BOTH
        // handlerType.GetMethod("HandleAsync")! and task.GetType().GetProperty("Result")!
        // return null for a trimmed member rather than throwing, so each `!` was a latent
        // NullReferenceException under Native AOT -- waiting for whichever query shape got
        // here first.
        var handler = (QueryHandlers.IQueryHandler)Activator.CreateInstance(handlerType, selector, canBeNull, canBeMultiples)!;

        return (TResult)(await handler.HandleAsObjectAsync(reader, token).ConfigureAwait(false))!;
    }

    private static Type FindDocumentType(Expression expression)
    {
        return expression switch
        {
            MethodCallExpression method => FindDocumentType(method.Arguments[0]),
            ConstantExpression { Value: IQueryable queryable } => queryable.ElementType,
            _ => throw new BadLinqExpressionException(
                $"Cannot determine the document type from expression: {expression.NodeType}")
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

        throw new BadLinqExpressionException($"Cannot determine the element type from: {expression.Type}");
    }

    private async Task WaitForNonStaleDataAsync(TimeSpan timeout, CancellationToken token)
    {
        var options = _session.Options;
        var eventGraph = options.EventGraph;

        // #916: under per-tenant partitioning a shard writes one row per tenant, and each tenant's seq_id
        // counts from 1 by itself, so this tenant's rows and this tenant's height are the only comparison
        // there is. The row called Name:All is never written, and the store's highest sequence is
        // another tenant's.
        var tenantId = eventGraph.UseTenantPartitionedEvents ? _session.TenantId : null;

        // Get the registered async projection shard names
        var asyncShardNames = options.Projections.All
            .Where(p => p.Lifecycle == JasperFx.Events.Projections.ProjectionLifecycle.Async)
            .SelectMany(p => p.ShardNames())
            .Select(s => tenantId == null ? s.Identity : s.ForTenant(tenantId).Identity)
            .ToHashSet();

        // If no async projections are registered, nothing to wait for
        if (asyncShardNames.Count == 0) return;

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (stopwatch.Elapsed < timeout)
        {
            token.ThrowIfCancellationRequested();

            // #148: poll on a fresh connection (independent of the session lifetime),
            // but route each probe through Options.ResiliencePipeline so the staleness
            // check is covered like the rest of the database access. Returns true once
            // the registered async shards have caught up to the high-water mark.
            var nonStale = await options.ResiliencePipeline.ExecuteAsync(static async (state, ct) =>
            {
                var (connectionString, eventGraph, asyncShardNames, tenantId) = state;

                await using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync(ct).ConfigureAwait(false);

                // Get high water mark
                long highWater;
                await using (var cmd = conn.CreateCommand())
                {
                    if (tenantId == null)
                    {
                        cmd.CommandText = $"SELECT ISNULL(MAX(seq_id), 0) FROM {eventGraph.EventsTableName};";
                    }
                    else
                    {
                        cmd.CommandText = $"""
                            SELECT ISNULL(MAX(seq_id), 0) FROM {eventGraph.EventsTableName}
                            WHERE tenant_id = @tenant_id AND {eventGraph.TenantPartitionManager.Column} =
                                (SELECT ordinal FROM {eventGraph.TenantPartitionsTableName} WHERE tenant_id = @tenant_id);
                            """;
                        cmd.Parameters.AddVarChar("@tenant_id", tenantId);
                    }

                    var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
                    highWater = result is long seq ? seq : 0;
                }

                if (highWater == 0) return true;

                // Check only the registered async projection shards
                var allCaughtUp = true;
                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"SELECT name, last_seq_id FROM {eventGraph.ProgressionTableName};";
                    await using var reader = (await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false));
                    var foundShards = new HashSet<string>();
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
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
                    return allCaughtUp && foundShards.SetEquals(asyncShardNames);
                }
            }, (options.ConnectionString, eventGraph, asyncShardNames, tenantId), token).ConfigureAwait(false);

            if (nonStale) return;

            await Task.Delay(100, token).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Timed out after {timeout} waiting for projection data to become non-stale.");
    }

    // ── #443 / jasperfx#647: IDocumentQueryExecutor ────────────────────────────────────────────────
    //
    // The shared document contract's async terminators (ToListAsync / FirstOrDefaultAsync /
    // CountAsync / AnyAsync in JasperFx.Events.Documents) are EXTENSION methods over IQueryable<T>
    // that dispatch through this interface on the query provider. They have to be extensions rather
    // than interface members because real consumer code composes a query across statements and every
    // System.Linq operator returns a plain IQueryable<T> -- member terminators would be unreachable
    // after a single Where.
    //
    // Each of the four delegates straight to Polecat's own terminator, so the shared surface and
    // PolecatQueryableExtensions are the same execution path rather than two that can drift. Note the
    // deliberate absence of predicate overloads: the shared extensions compose Queryable.Where
    // themselves and hand the narrowed queryable back down, so a store implements four primitives and
    // nothing more.

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. ⚠️ NOT the suppression #733 removed: that one asserted the path was SAFE. This records that it is unsafe and that the diagnostic has nowhere left to go.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. ⚠️ NOT the suppression #733 removed: that one asserted the path was SAFE. This records that it is unsafe and that the diagnostic has nowhere left to go.")]
    Task<IReadOnlyList<T>> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteToListAsync<T>(
        IQueryable<T> queryable, CancellationToken token)
        => queryable.ToListAsync(token);

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. ⚠️ NOT the suppression #733 removed: that one asserted the path was SAFE. This records that it is unsafe and that the diagnostic has nowhere left to go.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. ⚠️ NOT the suppression #733 removed: that one asserted the path was SAFE. This records that it is unsafe and that the diagnostic has nowhere left to go.")]
    Task<T?> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteFirstOrDefaultAsync<T>(
        IQueryable<T> queryable, CancellationToken token) where T : default
        => queryable.FirstOrDefaultAsync(token);

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. ⚠️ NOT the suppression #733 removed: that one asserted the path was SAFE. This records that it is unsafe and that the diagnostic has nowhere left to go.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. ⚠️ NOT the suppression #733 removed: that one asserted the path was SAFE. This records that it is unsafe and that the diagnostic has nowhere left to go.")]
    Task<int> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteCountAsync<T>(
        IQueryable<T> queryable, CancellationToken token)
        => queryable.CountAsync(token);

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. ⚠️ NOT the suppression #733 removed: that one asserted the path was SAFE. This records that it is unsafe and that the diagnostic has nowhere left to go.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. An EXPLICIT interface implementation cannot propagate the annotation -- the interface does not carry one, and annotating only the implementation is IL2046. A consumer is still warned, at the public entry point they call. ⚠️ NOT the suppression #733 removed: that one asserted the path was SAFE. This records that it is unsafe and that the diagnostic has nowhere left to go.")]
    Task<bool> JasperFx.Events.Documents.IDocumentQueryExecutor.ExecuteAnyAsync<T>(
        IQueryable<T> queryable, CancellationToken token)
        => queryable.AnyAsync(token);
}
