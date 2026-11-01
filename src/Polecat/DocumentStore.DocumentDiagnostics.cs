using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using JasperFx.Documents;
using JasperFx.Events;
using JasperFx.Linq;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;

using Polecat.Internal;
using Polecat.Linq;
namespace Polecat;

public partial class DocumentStore : IDocumentStoreDiagnostics
{
    /// <summary>
    ///     jasperfx#870 - the store this diagnostics surface reads. Must equal the same store's
    ///     <c>IDocumentStoreUsageSource.Subject</c>, which is why both are this one expression: a
    ///     console pairs the two without a lookup table.
    ///
    ///     #812: and that expression is the STORE's identity, <c>polecat://{storename}</c> — the same
    ///     value <c>IEventStore.Subject</c> returns — not the database URI. One store used to answer
    ///     with two subjects depending on which interface was asked, so a tool pairing a store's
    ///     document side with its event side by subject (CritterWatch's "Open in SQL", #1310) found
    ///     "no such store". Marten and Fisher give all three the same subject. The database identity
    ///     keeps its own surfaces: <c>Database.DatabaseUri</c> and the database descriptors.
    /// </summary>
    Uri IDocumentStoreDiagnostics.Subject => ((IEventStore)this).Subject;

    /// <summary>
    ///     The mapped document types this store can query. Sub-classes are listed alongside their
    ///     roots, because every member taking a <c>documentTypeName</c> accepts a registered
    ///     sub-class's name and a type picker that could not offer one would make that unreachable.
    /// </summary>
    Task<IReadOnlyList<DocumentTypeRef>> IDocumentStoreDiagnostics.DocumentTypesAsync(
        CancellationToken token)
    {
        var refs = new List<DocumentTypeRef>();

        foreach (var mapping in MaterializeMappings().OrderBy(m => m.DocumentType.Name))
        {
            var rootTypeName = mapping.DocumentType.FullNameInCode();

            // jasperfx#1070: whether a delete through the writer is permanent, known before it is
            // called. The ROOT's mapping decides it for the whole hierarchy, because the root's table
            // holds every sub-class row and its is_deleted column is the one a soft delete marks.
            var isSoftDeleted = mapping.DeleteStyle == DeleteStyle.SoftDelete;

            refs.Add(new DocumentTypeRef(
                rootTypeName,
                mapping.DocumentType.Name.ToLowerInvariant(),
                mapping.DatabaseSchemaName)
            {
                IsSoftDeleted = isSoftDeleted,
                // jasperfx#1082: names the projection AggregateDocumentJsonAsync rebuilds this type through.
                SingleStreamProjection = SingleStreamProjectionFor(mapping.DocumentType)?.Name
            });

            foreach (var sub in mapping.SubClasses)
            {
                // jasperfx#932: a sub-class names the root whose table holds its rows, so a type
                // picker can group the hierarchy instead of listing it flat. Polecat already listed
                // sub-classes while Marten and Fisher did not; the contract settled on listing them,
                // which makes the marker the part that was missing rather than the listing.
                refs.Add(new DocumentTypeRef(
                    sub.DocumentType.FullNameInCode(),
                    sub.Alias,
                    mapping.DatabaseSchemaName) { RootTypeName = rootTypeName, IsSoftDeleted = isSoftDeleted });
            }
        }

        return Task.FromResult<IReadOnlyList<DocumentTypeRef>>(refs);
    }

    async Task<DocumentQueryResult> IDocumentStoreDiagnostics.QueryDocumentsAsync(
        string documentTypeName, DocumentQueryOptions options, CancellationToken token)
    {
        var query = await PrepareDiagnosticsQueryAsync(documentTypeName, options).ConfigureAwait(false);
        if (query == null)
        {
            // An unknown type name reads as an empty page rather than failing.
            return new DocumentQueryResult(Array.Empty<StoredDocument>(), 0, options.PageNumber, options.PageSize);
        }

        if (query.MatchesNothing)
        {
            return new DocumentQueryResult(Array.Empty<StoredDocument>(), 0, query.PageNumber, query.PageSize);
        }

        await using var conn = new SqlConnection(query.ConnectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);

        long total;
        await using (var count = query.BuildCount(conn))
        {
            total = Convert.ToInt64(await count.ExecuteScalarAsync(token).ConfigureAwait(false));
        }

        var rows = new List<StoredDocument>();
        await using (var page = query.BuildPage(conn))
        {
            await using var reader = await page.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                rows.Add(ReadStoredDocument(reader, query.Mapping));
            }
        }

        return new DocumentQueryResult(rows, total, query.PageNumber, query.PageSize);
    }

    /// <summary>
    ///     The count and page statements <see cref="IDocumentStoreDiagnostics.QueryDocumentsAsync" /> runs, built
    ///     once so that <see cref="IDocumentStoreDiagnostics.ExplainDocumentQueryAsync" /> explains exactly the
    ///     statement the query executes — and refuses exactly what it refuses.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>The mechanism: Polecat's LINQ provider translates the criteria, this surface keeps its
    ///         own statement.</b> The text is composed onto <c>Query&lt;T&gt;()</c> for the REQUESTED type
    ///         (so a sub-class name resolves the sub-class's members) through
    ///         <see cref="DocumentQueryCriteria.ApplyCriteriaTo{T}" />, and the provider's own parser turns
    ///         the result into WHERE fragments and ORDER BY terms. Those are appended to the same
    ///         <c>select … from table where …</c> the criteria-free path runs, with the same scoping
    ///         conditions bound the same way.
    ///     </para>
    ///     <para>
    ///         <b>Why not run the LINQ query and read its rows.</b> Every semantic this surface already
    ///         has — the tenant (including database-per-tenant, which needs this surface's own connection
    ///         routing), <c>AllTenants</c>, soft deletes and <c>IncludeSoftDeleted</c>, the
    ///         <c>doc_type</c> filter for a sub-class, <c>IdEquals</c> bound as the stored identity type,
    ///         and the correlation / causation / last-modified-by filters — is a condition in that one
    ///         statement. Several of them have no LINQ operator at all (the metadata columns), and the
    ///         result must be the byte-exact stored JSON plus its metadata, not a deserialized document.
    ///         Composing the provider's fragments into the existing statement keeps every one of those by
    ///         construction, keeps the total and the page in the same WHERE, and is still one count and
    ///         one page query — no id round trip and no second read. No operator text is ever spliced
    ///         into SQL: the fragments are the provider's, and every value in them is a parameter.
    ///     </para>
    ///     <para>
    ///         Ordering is tenant first under <c>AllTenants</c>, then the query's ordering — which
    ///         <see cref="DocumentQueryCriteria.ApplyCriteriaTo{T}" /> tie-breaks by the identity member, so
    ///         equal keys still page deterministically — and the id alone without an ordering.
    ///     </para>
    /// </remarks>
    private sealed class DiagnosticsPageQuery
    {
        public required Storage.DocumentMapping Mapping { get; init; }
        public required string ConnectionString { get; init; }
        public required int PageNumber { get; init; }
        public required int PageSize { get; init; }

        /// <summary>An <c>IdEquals</c> that cannot be the stored identity type: no statement runs at all.</summary>
        public bool MatchesNothing { get; init; }

        public Func<SqlConnection, SqlBatch> BuildCount { get; init; } = null!;
        public Func<SqlConnection, SqlBatch> BuildPage { get; init; } = null!;
    }

    /// <summary>
    ///     Every refusal and every condition of the document query, decided before a connection opens. Null for
    ///     an unknown type name.
    /// </summary>
    private async Task<DiagnosticsPageQuery?> PrepareDiagnosticsQueryAsync(string documentTypeName,
        DocumentQueryOptions options)
    {
        // jasperfx#928: AllTenants together with a named tenant is a contradiction, not a precedence
        // question. Asserted first so it cannot be resolved by accident further down.
        options.AssertValidTenantScope();

        var resolved = ResolveForDiagnostics(documentTypeName);
        if (resolved == null) return null;

        var (mapping, requestedType) = resolved.Value;

        // jasperfx#1077: a metadata filter on a column this type's table does not have used to be
        // skipped, which returned the unfiltered page and total as though they were filtered. The ROOT's
        // mapping answers for a sub-class, because its table holds the sub-class's rows.
        options.AssertMetadataFiltersAreCaptured(requestedType.FullNameInCode(),
            mapping.Metadata.CorrelationId.Enabled,
            mapping.Metadata.CausationId.Enabled,
            mapping.Metadata.LastModifiedBy.Enabled);

        var pageNumber = Math.Max(1, options.PageNumber);
        var pageSize = Math.Max(1, options.PageSize);
        var offset = (pageNumber - 1) * pageSize;

        var tenantId = DocumentQueryOptions.NormalizeTenantId(options.TenantId);
        var conjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;

        // AllTenants on a conjoined store means NO tenant predicate; on a single-tenanted type every
        // row already is the whole set. Database-per-tenant would have to fan out, which this surface
        // does not do - refused rather than answered with one database's rows as if they were all.
        if (options.AllTenants
            && Options.Tenancy != null
            && Options.Tenancy.Cardinality != JasperFx.Descriptors.DatabaseCardinality.Single)
        {
            throw new DocumentCriteriaNotSupportedException(nameof(DocumentQueryOptions.AllTenants),
                "This store keeps a database per tenant, and document diagnostics read one database. "
                + "Query each tenant by TenantId instead.");
        }

        var conditions = new List<string>();
        var parameters = new List<Action<SqlParameterCollection>>();
        var matchesNothing = false;

        if (options.IdEquals.IsNotEmpty())
        {
            // jasperfx#870: convert to the STORED identity type instead of casting the column to text.
            // The old `cast(id as nvarchar(max)) = @id` could not use the primary-key index, and an
            // upper-case Guid missed.
            if (TryBindId(mapping, options.IdEquals!, out var bind))
            {
                conditions.Add("id = @id");
                parameters.Add(bind!);
            }
            else
            {
                // An id that cannot be the stored type matches nothing - an empty page, not an error.
                matchesNothing = true;
            }
        }

        if (conjoined && !options.AllTenants)
        {
            var effective = tenantId ?? DefaultTenantId();
            conditions.Add("tenant_id = @tenant");
            parameters.Add(p => p.AddVarChar("@tenant", effective));
        }

        // Hierarchies: naming a sub-class returns only its rows; naming the root returns every row in
        // the table, because each of those rows IS a root.
        var filterByDocType = mapping.IsHierarchy() && requestedType != mapping.DocumentType;
        if (filterByDocType)
        {
            var alias = mapping.AliasFor(requestedType);
            conditions.Add("doc_type = @doc_type");
            parameters.Add(p => p.AddVarChar("@doc_type", alias));
        }

        if (mapping.DeleteStyle == DeleteStyle.SoftDelete && !options.IncludeSoftDeleted)
        {
            conditions.Add("is_deleted = 0");
        }

        // Captured, or refused above.
        if (options.CorrelationId != null)
        {
            conditions.Add("correlation_id = @correlation");
            parameters.Add(p => p.AddVarChar("@correlation", options.CorrelationId!));
        }

        if (options.CausationId != null)
        {
            conditions.Add("causation_id = @causation");
            parameters.Add(p => p.AddVarChar("@causation", options.CausationId!));
        }

        if (options.LastModifiedBy != null)
        {
            conditions.Add("last_modified_by = @last_modified_by");
            parameters.Add(p => p.AddVarChar("@last_modified_by", options.LastModifiedBy!));
        }

        var where = conditions.Count > 0 ? " where " + string.Join(" and ", conditions) : "";

        void Bind(SqlParameterCollection collection)
        {
            foreach (var p in parameters) p(collection);
        }

        var table = mapping.QualifiedTableName;
        var selection = SelectionFor(mapping);

        // #813 (jasperfx#1058): resolved before anything else is decided, so a column this type cannot
        // order by is refused before a connection opens rather than silently dropped.
        var metadataOrder = MetadataOrderingFor(mapping, options.OrderByMetadata);

        // Under AllTenants the page orders by tenant FIRST, so a page boundary cannot repeat or skip a
        // row as tenants interleave. A metadata key comes next, and the id breaks every remaining tie.
        var tenantFirst = conjoined && options.AllTenants;
        var order = (tenantFirst ? "tenant_id, " : "") + (metadataOrder == null ? "" : metadataOrder + ", ") + "id";

        if (options.HasCriteria())
        {
            // Translated BEFORE a connection is opened: a refusal costs no round trip.
            var criteria = await TranslateCriteriaForDiagnosticsAsync(requestedType, mapping, options)
                .ConfigureAwait(false);
            var fromAndWhere = $"{table}{where}";
            var hasConditions = conditions.Count > 0;

            return new DiagnosticsPageQuery
            {
                Mapping = mapping,
                ConnectionString = ConnectionStringFor(tenantId),
                PageNumber = pageNumber,
                PageSize = pageSize,
                MatchesNothing = matchesNothing,
                BuildCount = conn => BuildCriteriaBatch(conn, options, $"select count_big(*) from {fromAndWhere}",
                    criteria, hasConditions, Bind, orderBy: null),
                BuildPage = conn => BuildCriteriaBatch(conn, options, $"select {selection} from {fromAndWhere}",
                    criteria, hasConditions, Bind,
                    orderBy: builder => AppendCriteriaOrdering(builder, criteria, tenantFirst, metadataOrder, offset,
                        pageSize))
            };
        }

        return new DiagnosticsPageQuery
        {
            Mapping = mapping,
            ConnectionString = ConnectionStringFor(tenantId),
            PageNumber = pageNumber,
            PageSize = pageSize,
            MatchesNothing = matchesNothing,
            BuildCount = conn => PlainBatch(conn, $"select count_big(*) from {table}{where}", Bind),
            BuildPage = conn => PlainBatch(conn,
                $"select {selection} from {table}{where} " +
                $"order by {order} offset {offset} rows fetch next {pageSize} rows only", Bind)
        };
    }

    private static SqlBatch PlainBatch(SqlConnection conn, string sql, Action<SqlParameterCollection> bind)
    {
        var batch = new SqlBatch(conn);
        var command = new SqlBatchCommand(sql);
        bind(command.Parameters);
        batch.BatchCommands.Add(command);
        return batch;
    }

    /// <summary>
    ///     One command: <paramref name="head" /> (which already carries this surface's own conditions),
    ///     then the criteria's WHERE fragments, then <paramref name="orderBy" />. The fragments bind
    ///     positional parameters through the builder; the scoping conditions keep their named, typed
    ///     ones (#363 varchar binding), so the two cannot collide.
    /// </summary>
    private static SqlBatch BuildCriteriaBatch(SqlConnection conn, DocumentQueryOptions options, string head,
        Linq.SqlGeneration.Statement criteria, bool hasConditions, Action<SqlParameterCollection> bind,
        Action<Weasel.SqlServer.BatchBuilder>? orderBy)
    {
        var batch = new SqlBatch(conn);
        try
        {
            var builder = new Weasel.SqlServer.BatchBuilder(batch);
            builder.Append(head);

            var first = !hasConditions;
            foreach (var fragment in criteria.Wheres)
            {
                builder.Append(first ? " where (" : " and (");
                fragment.Apply(builder);
                builder.Append(")");
                first = false;
            }

            orderBy?.Invoke(builder);
            builder.Compile();
        }
        catch (Exception e) when (DocumentQueryCriteria.IsTranslationFailure(e))
        {
            // A fragment that refuses only when it is rendered is still the provider refusing the shape.
            batch.Dispose();
            throw options.Untranslatable(e);
        }

        bind(batch.BatchCommands[0].Parameters);
        return batch;
    }

    private static void AppendCriteriaOrdering(Weasel.SqlServer.BatchBuilder builder,
        Linq.SqlGeneration.Statement criteria, bool tenantFirst, string? metadataOrder, int offset, int pageSize)
    {
        builder.Append(" order by ");
        if (tenantFirst) builder.Append("tenant_id, ");

        // jasperfx#1058: the metadata key leads, and the query's own ordering breaks its ties.
        if (metadataOrder != null) builder.Append(metadataOrder + ", ");

        if (criteria.OrderBys.Count == 0)
        {
            builder.Append("id");
        }
        else
        {
            // A key that repeats an earlier one is dropped: it can never change the order, and SQL Server
            // refuses the statement outright ("a column has been specified more than once in the order
            // by list"). It is the ordinary case, not an edge one — an ordering that already names the
            // id is tie-broken by the id again.
            var seen = new HashSet<string>(StringComparer.Ordinal);
            var first = true;
            foreach (var term in criteria.OrderBys)
            {
                if (term.Literal is not null && !seen.Add(term.Literal)) continue;

                if (!first) builder.Append(", ");
                term.Apply(builder);
                first = false;
            }
        }

        builder.Append($" offset {offset} rows fetch next {pageSize} rows only");
    }

    /// <summary>
    ///     #813 (jasperfx#1058): the ORDER BY key for <see cref="DocumentQueryOptions.OrderByMetadata" />,
    ///     or null when none was asked for. Every column here is fixed text chosen from an enum, never
    ///     caller input.
    /// </summary>
    /// <remarks>
    ///     <c>version</c> is always the numeric <c>bigint</c> column (Decision D2) — the one
    ///     <see cref="StoredDocument.Version" /> reports — even for a Guid-versioned type, whose Guid
    ///     lives in <c>guid_version</c>; so Version always has an order here. <c>created_at</c> exists on
    ///     every table, but this surface reports it only where the mapping tracks it, and ordering by a
    ///     column that every row reports as null would be ordering by something the caller cannot see —
    ///     refused instead, as the contract asks.
    /// </remarks>
    private static string? MetadataOrderingFor(Storage.DocumentMapping mapping, DocumentMetadataOrder? order)
    {
        if (order == null) return null;

        var column = order.Field switch
        {
            DocumentMetadataField.LastModified => "last_modified",
            DocumentMetadataField.Version => "version",
            DocumentMetadataField.Created when mapping.Metadata.CreatedAt.Enabled => "created_at",
            DocumentMetadataField.Created => throw new DocumentCriteriaNotSupportedException(
                nameof(DocumentQueryOptions.OrderByMetadata),
                $"'{mapping.DocumentType.FullNameInCode()}' does not track its created time."),
            _ => throw new DocumentCriteriaNotSupportedException(nameof(DocumentQueryOptions.OrderByMetadata),
                $"'{order.Field}' is not a metadata column Polecat can order by.")
        };

        return order.Descending ? column + " desc" : column;
    }

    private static System.Reflection.MethodInfo? _translateCriteria;

    /// <summary>
    ///     Compose the criteria onto <c>Query&lt;T&gt;()</c> for <paramref name="requestedType" /> and hand
    ///     back the provider's translation, or throw the contract's refusal.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "jasperfx#869: criteria are operator-typed text applied to a runtime document type; DocumentQueryCriteria.ApplyCriteriaTo refuses outright when dynamic code is unavailable, and the document types reached here are the store's registered mappings, which Polecat's LINQ provider already requires to be preserved (see PolecatLinqQueryProvider). The explicit IDocumentStoreDiagnostics implementation cannot carry the annotation itself (IL2046).")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "jasperfx#869: guarded — DocumentQueryCriteria.ApplyCriteriaTo throws DocumentCriteriaNotSupportedException when RuntimeFeature.IsDynamicCodeSupported is false, before any generic instantiation is used. The explicit IDocumentStoreDiagnostics implementation cannot carry the annotation itself (IL3051).")]
    [UnconditionalSuppressMessage("Trimming", "IL2060",
        Justification = "jasperfx#869: closes DiagnosticsCriteria.TranslateFor<T> over a registered document type; see the IL2026 justification.")]
    private async Task<Linq.SqlGeneration.Statement> TranslateCriteriaForDiagnosticsAsync(Type requestedType,
        Storage.DocumentMapping mapping, DocumentQueryOptions options)
    {
        if (!DocumentQueryCriteria.IsAvailable)
        {
            // Lets ApplyCriteriaTo phrase the refusal, without closing a generic over the type first.
            options.ApplyCriteriaTo(Array.Empty<object>().AsQueryable());
        }

        try
        {
            // Looked up here rather than in a static initializer: a reflective reference to a
            // [RequiresUnreferencedCode] member from an initializer is itself an unsuppressable IL2026.
            _translateCriteria ??= typeof(DiagnosticsCriteria).GetMethod(nameof(DiagnosticsCriteria.TranslateFor),
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

            var translation = (Task<Linq.SqlGeneration.Statement>)_translateCriteria
                .MakeGenericMethod(requestedType).Invoke(null, [this, mapping, options])!;
            return await translation.ConfigureAwait(false);
        }
        catch (System.Reflection.TargetInvocationException e) when (e.InnerException is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(e.InnerException).Throw();
            throw;
        }
    }

    /// <summary>
    ///     One document by id, with its metadata. Single-tenant by design (jasperfx#928 leaves the
    ///     all-tenants scope to the query surface).
    /// </summary>
    /// <remarks>
    ///     Deliberately NOT filtered on <c>is_deleted</c>: a load by id is explicit, so a soft-deleted
    ///     row comes back flagged rather than hidden. Hiding it would tell a console the document does
    ///     not exist, which is a different and worse answer than "it is deleted".
    /// </remarks>
    async Task<StoredDocument?> IDocumentStoreDiagnostics.LoadDocumentAsync(
        string documentTypeName, string id, string? tenantId, CancellationToken token)
    {
        var resolved = ResolveForDiagnostics(documentTypeName);
        if (resolved == null) return null;

        var (mapping, requestedType) = resolved.Value;
        if (!TryBindId(mapping, id, out var bind)) return null;

        var normalized = DocumentQueryOptions.NormalizeTenantId(tenantId);
        var conjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;
        var filterByDocType = mapping.IsHierarchy() && requestedType != mapping.DocumentType;

        var conditions = new List<string> { "id = @id" };
        if (conjoined) conditions.Add("tenant_id = @tenant");
        if (filterByDocType) conditions.Add("doc_type = @doc_type");

        await using var conn = new SqlConnection(ConnectionStringFor(normalized));
        await conn.OpenAsync(token).ConfigureAwait(false);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"select top 1 {SelectionFor(mapping)} from {mapping.QualifiedTableName} " +
            $"where {string.Join(" and ", conditions)}";

        bind!(cmd.Parameters);
        if (conjoined) cmd.Parameters.AddVarChar("@tenant", normalized ?? DefaultTenantId());
        if (filterByDocType) cmd.Parameters.AddVarChar("@doc_type", mapping.AliasFor(requestedType));

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        return await reader.ReadAsync(token).ConfigureAwait(false)
            ? ReadStoredDocument(reader, mapping)
            : null;
    }

    private string DefaultTenantId()
        => Options.Tenancy?.DefaultTenantId ?? StorageConstants.DefaultTenantId;

    /// <summary>
    ///     The column list every diagnostics read shares, so the page and the load cannot drift into
    ///     reporting different metadata for the same row. Fixed ordinals, hence the null placeholders
    ///     for columns a given mapping does not carry.
    /// </summary>
    private static string SelectionFor(Storage.DocumentMapping mapping)
    {
        // cast(data as nvarchar(max)) guarantees the column comes back as JSON text whether it is
        // stored as nvarchar or the native json type.
        var columns = new List<string>
        {
            "cast(id as nvarchar(max))",
            "cast(data as nvarchar(max))",
            "version",
            "last_modified"
        };

        columns.Add(mapping.Metadata.CreatedAt.Enabled ? "created_at" : "cast(null as datetimeoffset)");
        columns.Add(mapping.TenancyStyle == TenancyStyle.Conjoined
            ? "tenant_id"
            : "cast(null as varchar(250))");

        if (mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            columns.Add("is_deleted");
            columns.Add("deleted_at");
        }
        else
        {
            columns.Add("cast(0 as bit)");
            columns.Add("cast(null as datetimeoffset)");
        }

        columns.Add(mapping.IsHierarchy() ? "doc_type" : "cast(null as varchar(250))");

        return string.Join(", ", columns);
    }

    private StoredDocument ReadStoredDocument(SqlDataReader reader, Storage.DocumentMapping mapping)
    {
        var docType = reader.IsDBNull(8) ? null : reader.GetString(8);

        return new StoredDocument(reader.GetString(0), reader.GetString(1))
        {
            Version = reader.IsDBNull(2) ? null : reader.GetValue(2).ToString(),
            LastModified = reader.IsDBNull(3) ? null : reader.GetDateTimeOffset(3),
            Created = reader.IsDBNull(4) ? null : reader.GetDateTimeOffset(4),
            TenantId = reader.IsDBNull(5) ? DefaultTenantId() : reader.GetString(5),
            IsDeleted = !reader.IsDBNull(6) && reader.GetBoolean(6),
            DeletedAt = reader.IsDBNull(7) ? null : reader.GetDateTimeOffset(7),
            DocumentType = docType == null
                ? mapping.DocumentType.FullNameInCode()
                : SafeTypeName(mapping, docType)
        };
    }

    private static string SafeTypeName(Storage.DocumentMapping mapping, string alias)
    {
        try
        {
            return mapping.TypeFor(alias).FullNameInCode();
        }
        catch (ArgumentOutOfRangeException)
        {
            // A doc_type this deployment does not know is data written by one that did. Reporting the
            // raw alias beats throwing out of a diagnostic read (#677).
            return alias;
        }
    }

    /// <summary>
    ///     Convert the text id to the mapping's STORED identity type so the comparison can seek the
    ///     primary key. Returns false when the text cannot be that type, which matches nothing.
    /// </summary>
    private static bool TryBindId(Storage.DocumentMapping mapping, string id, out Action<SqlParameterCollection>? bind)
    {
        var type = Nullable.GetUnderlyingType(mapping.InnerIdType) ?? mapping.InnerIdType;

        if (type == typeof(Guid))
        {
            if (!Guid.TryParse(id, out var guid)) { bind = null; return false; }
            bind = p => p.Add("@id", SqlDbType.UniqueIdentifier).Value = guid;
            return true;
        }

        if (type == typeof(int))
        {
            if (!int.TryParse(id, out var i)) { bind = null; return false; }
            bind = p => p.Add("@id", SqlDbType.Int).Value = i;
            return true;
        }

        if (type == typeof(long))
        {
            if (!long.TryParse(id, out var l)) { bind = null; return false; }
            bind = p => p.Add("@id", SqlDbType.BigInt).Value = l;
            return true;
        }

        // varchar id: VarChar rather than NVarChar so the comparison does not wrap the column in
        // CONVERT_IMPLICIT and scan (#363).
        bind = p => p.AddVarChar("@id", id);
        return true;
    }

    /// <summary>
    ///     The database holding this tenant's rows - its own under database-per-tenant tenancy,
    ///     otherwise the store's.
    /// </summary>
    private string ConnectionStringFor(string? tenantId)
    {
        if (tenantId == null) return Database.ConnectionString;
        return Options.Tenancy?.GetConnectionFactory(tenantId).ConnectionString ?? Database.ConnectionString;
    }

    /// <summary>
    ///     Resolve a type name to its mapping AND the type actually asked for, which differ when the
    ///     name is a registered sub-class: the mapping owns the table, the requested type decides the
    ///     <c>doc_type</c> filter.
    /// </summary>
    private (Storage.DocumentMapping Mapping, Type RequestedType)? ResolveForDiagnostics(string documentTypeName)
    {
        foreach (var mapping in MaterializeMappings())
        {
            if (NameMatches(mapping.DocumentType, documentTypeName)
                || string.Equals(mapping.DocumentType.Name.ToLowerInvariant(), documentTypeName,
                    StringComparison.OrdinalIgnoreCase))
            {
                return (mapping, mapping.DocumentType);
            }

            foreach (var sub in mapping.SubClasses)
            {
                if (NameMatches(sub.DocumentType, documentTypeName)
                    || string.Equals(sub.Alias, documentTypeName, StringComparison.OrdinalIgnoreCase))
                {
                    return (mapping, sub.DocumentType);
                }
            }
        }

        return null;
    }

    /// <summary>
    ///     The CLR type a diagnostics WRITE targets, or null when the name is unknown. A sub-class
    ///     resolves to the sub-class, because that is the type whose JSON the caller sent.
    /// </summary>
    internal Type? ResolveDiagnosticsWriteType(string documentTypeName)
        => ResolveForDiagnostics(documentTypeName)?.RequestedType;

    private static bool NameMatches(Type type, string name)
        => string.Equals(type.FullNameInCode(), name, StringComparison.OrdinalIgnoreCase)
           || string.Equals(type.FullName, name, StringComparison.OrdinalIgnoreCase)
           || string.Equals(type.Name, name, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Force lazy provider materialization (Schema.For&lt;T&gt; registrations + aggregate
    /// document types declared by projections) and return the resulting mappings — the
    /// same two-source dance <c>TryCreateUsage</c> performs, so the Explorer sees the same
    /// document set the descriptor snapshot does.
    /// </summary>
    private IEnumerable<Storage.DocumentMapping> MaterializeMappings()
    {
        var seenDocumentTypes = new HashSet<Type>();

        foreach (var expr in Options.Schema.Expressions)
        {
            var exprType = expr.GetType();
            if (!exprType.IsGenericType) continue;

            var documentType = exprType.GetGenericArguments()[0];
            if (seenDocumentTypes.Add(documentType))
            {
                Options.Providers.GetProvider(documentType);
            }
        }

        foreach (var aggregate in Options.Projections.All.OfType<JasperFx.Events.Aggregation.IAggregateProjection>())
        {
            if (seenDocumentTypes.Add(aggregate.AggregateType))
            {
                Options.Providers.GetProvider(aggregate.AggregateType);
            }
        }

        return Options.Providers.AllProviders.Select(p => p.Mapping);
    }
}

/// <summary>
///     jasperfx#869: the generic half of criteria translation, closed over the requested document type, and
///     the shapes Polecat refuses because its LINQ provider would answer them WRONGLY rather than fail.
/// </summary>
/// <remarks>
///     <para>
///         A separate class, not a member of <see cref="DocumentStore" />, on purpose: a
///         <c>[RequiresUnreferencedCode]</c> member on <see cref="DocumentStore" /> surfaces as IL2026 at
///         <c>SecondaryStoreProxyFactory</c>, which emits a subclass of the store and so keeps every member
///         of it reflectable.
///     </para>
///     <para>
///         <b>The refusals are measured, not guessed.</b> Each one was found by running the shape against a
///         seeded table and comparing with LINQ to objects running the same text, and each is pinned by
///         <c>document_diagnostics_criteria_shapes</c>, which also asserts the raw provider is STILL wrong —
///         so the day Polecat translates one of them correctly, that test says the rule can go. Shapes the
///         provider cannot translate are not listed: it throws, and the surface turns that into a refusal.
///     </para>
/// </remarks>
internal static class DiagnosticsCriteria
{
    internal const string EnumRangeRefusal =
        "compares an enum with <, <=, > or >=, but this store keeps enums as their NAMES, so it would compare the names alphabetically rather than the values. Use = or 'in @0' with the members you want.";

    internal const string EnumOrderingRefusal =
        "orders by an enum, but this store keeps enums as their NAMES, so the order would be alphabetical rather than by value. Order by another member.";

    /// <summary>The refusals that hold for every Polecat store.</summary>
    internal static DynamicQueryPolicy Policy { get; } = DynamicQueryPolicy.Default.WithRules(
        // Three-valued logic: NULL <> 'x' is unknown, so `Note != "x"`, `not (Note = "x")` and
        // `not Note.Contains("x")` drop every row whose member is null where the text read as C# keeps
        // them. Measured on Polecat (3 of 7 rows) before the shared rule existed; the guarded forms
        // (`... or Note = null`, `Note != null and ...`) translate correctly and stay allowed.
        DynamicQueryShapeRules.SqlNullSemantics());

    /// <summary>
    ///     <see cref="Policy" /> plus the refusals for a store whose serializer keeps enums as names
    ///     (<c>EnumStorage.AsString</c>), where equality and <c>in</c> still translate correctly but a range
    ///     comparison does not.
    /// </summary>
    internal static DynamicQueryPolicy EnumsAsNamesPolicy { get; } = Policy.WithRules(
        DynamicQueryShapeRules.For(node =>
            node is System.Linq.Expressions.BinaryExpression
            {
                NodeType: System.Linq.Expressions.ExpressionType.GreaterThan
                or System.Linq.Expressions.ExpressionType.GreaterThanOrEqual
                or System.Linq.Expressions.ExpressionType.LessThan
                or System.Linq.Expressions.ExpressionType.LessThanOrEqual
            } binary && (isEnum(binary.Left) || isEnum(binary.Right))
                ? EnumRangeRefusal
                : null));

    internal static DynamicQueryPolicy PolicyFor(StoreOptions options)
        => options.Serializer.EnumStorage == Weasel.Core.EnumStorage.AsString ? EnumsAsNamesPolicy : Policy;

    private static bool isEnum(System.Linq.Expressions.Expression expression)
    {
        while (expression is System.Linq.Expressions.UnaryExpression
               {
                   NodeType: System.Linq.Expressions.ExpressionType.Convert
                   or System.Linq.Expressions.ExpressionType.ConvertChecked
               } convert)
        {
            expression = convert.Operand;
        }

        return (Nullable.GetUnderlyingType(expression.Type) ?? expression.Type).IsEnum;
    }

    /// <summary>
    ///     An ordering by an enum stored as its name. Not a <see cref="IDynamicQueryShapeRule" />: a rule sees
    ///     one node at a time and cannot tell an ordering key from the same member inside a predicate, where
    ///     it is fine. So the composed ordering's key lambdas are read directly.
    /// </summary>
    private static void assertOrderingIsByValue(System.Linq.Expressions.Expression expression, StoreOptions storeOptions)
    {
        if (storeOptions.Serializer.EnumStorage != Weasel.Core.EnumStorage.AsString) return;

        while (expression is System.Linq.Expressions.MethodCallExpression call)
        {
            if (call.Method.DeclaringType == typeof(Queryable)
                && call.Method.Name is "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending"
                && call.Arguments[1] is System.Linq.Expressions.UnaryExpression { Operand: System.Linq.Expressions.LambdaExpression key }
                && isEnum(key.Body))
            {
                throw new DocumentCriteriaNotSupportedException(nameof(DocumentQueryOptions.OrderBy),
                    $"'{key.Body}' {EnumOrderingRefusal}");
            }

            expression = call.Arguments[0];
        }
    }

    [RequiresDynamicCode("jasperfx#869: applies Dynamic LINQ text to a runtime document type.")]
    [RequiresUnreferencedCode("jasperfx#869: applies Dynamic LINQ text to a runtime document type.")]
    internal static async Task<Linq.SqlGeneration.Statement> TranslateFor<T>(DocumentStore store,
        Storage.DocumentMapping mapping, DocumentQueryOptions options) where T : notnull
    {
        // The session is only the provider's host: nothing executes through it, and binding it to the
        // main database skips tenant routing, which the diagnostics surface does itself.
        await using var session = store.QuerySession(SessionOptions.ForDatabase(store.Database));
        var queryable = options.ApplyCriteriaTo(session.Query<T>(), mapping.IdMember.Name, PolicyFor(store.Options));
        assertOrderingIsByValue(queryable.Expression, store.Options);

        try
        {
            return ((PolecatLinqQueryProvider)queryable.Provider).TranslateCriteria(queryable.Expression);
        }
        catch (Exception e) when (DocumentQueryCriteria.IsTranslationFailure(e))
        {
            throw options.Untranslatable(e);
        }
    }
}
