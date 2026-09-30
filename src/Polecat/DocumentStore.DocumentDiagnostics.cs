using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using JasperFx.Documents;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;

using Polecat.Internal;
namespace Polecat;

public partial class DocumentStore : IDocumentStoreDiagnostics
{
    /// <summary>
    ///     jasperfx#870 - the store this diagnostics surface reads. Must equal the same store's
    ///     <c>IDocumentStoreUsageSource.Subject</c>, which is why both are this one expression: a
    ///     console pairs the two without a lookup table.
    /// </summary>
    Uri IDocumentStoreDiagnostics.Subject => Database.DatabaseUri;

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
            refs.Add(new DocumentTypeRef(
                mapping.DocumentType.FullNameInCode(),
                mapping.DocumentType.Name.ToLowerInvariant(),
                mapping.DatabaseSchemaName));

            foreach (var sub in mapping.SubClasses)
            {
                refs.Add(new DocumentTypeRef(
                    sub.DocumentType.FullNameInCode(),
                    sub.Alias,
                    mapping.DatabaseSchemaName));
            }
        }

        return Task.FromResult<IReadOnlyList<DocumentTypeRef>>(refs);
    }

    async Task<DocumentQueryResult> IDocumentStoreDiagnostics.QueryDocumentsAsync(
        string documentTypeName, DocumentQueryOptions options, CancellationToken token)
    {
        // jasperfx#928: AllTenants together with a named tenant is a contradiction, not a precedence
        // question. Asserted first so it cannot be resolved by accident further down.
        options.AssertValidTenantScope();

        // jasperfx#870: a criterion we cannot apply is REFUSED. Returning the unfiltered page is the
        // one wrong answer - a console cannot tell it apart from a filter that matched every row.
        // Polecat does not translate Dynamic LINQ yet (jasperfx#869).
        if (options.Where.IsNotEmpty())
        {
            throw new DocumentCriteriaNotSupportedException(nameof(DocumentQueryOptions.Where),
                "Polecat does not yet translate a Dynamic LINQ predicate for document diagnostics (jasperfx#869). "
                + "Query without a Where, or use the store's own LINQ surface.");
        }

        if (options.OrderBy.IsNotEmpty())
        {
            throw new DocumentCriteriaNotSupportedException(nameof(DocumentQueryOptions.OrderBy),
                "Polecat does not yet translate a Dynamic LINQ ordering for document diagnostics (jasperfx#869). "
                + "Query without an OrderBy; rows come back ordered by id.");
        }

        var resolved = ResolveForDiagnostics(documentTypeName);
        if (resolved == null)
        {
            // An unknown type name reads as an empty page rather than failing.
            return new DocumentQueryResult(Array.Empty<StoredDocument>(), 0, options.PageNumber, options.PageSize);
        }

        var (mapping, requestedType) = resolved.Value;

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
        var parameters = new List<Action<SqlCommand>>();

        if (options.IdEquals.IsNotEmpty())
        {
            // jasperfx#870: convert to the STORED identity type instead of casting the column to text.
            // The old `cast(id as nvarchar(max)) = @id` could not use the primary-key index, and an
            // upper-case Guid missed.
            if (!TryBindId(mapping, options.IdEquals!, out var bind))
            {
                // An id that cannot be the stored type matches nothing - an empty page, not an error.
                return new DocumentQueryResult(Array.Empty<StoredDocument>(), 0, pageNumber, pageSize);
            }

            conditions.Add("id = @id");
            parameters.Add(bind!);
        }

        if (conjoined && !options.AllTenants)
        {
            var effective = tenantId ?? DefaultTenantId();
            conditions.Add("tenant_id = @tenant");
            parameters.Add(cmd => cmd.Parameters.AddVarChar("@tenant", effective));
        }

        // Hierarchies: naming a sub-class returns only its rows; naming the root returns every row in
        // the table, because each of those rows IS a root.
        var filterByDocType = mapping.IsHierarchy() && requestedType != mapping.DocumentType;
        if (filterByDocType)
        {
            var alias = mapping.AliasFor(requestedType);
            conditions.Add("doc_type = @doc_type");
            parameters.Add(cmd => cmd.Parameters.AddVarChar("@doc_type", alias));
        }

        if (mapping.DeleteStyle == DeleteStyle.SoftDelete && !options.IncludeSoftDeleted)
        {
            conditions.Add("is_deleted = 0");
        }

        if (options.CorrelationId != null && mapping.Metadata.CorrelationId.Enabled)
        {
            conditions.Add("correlation_id = @correlation");
            parameters.Add(cmd => cmd.Parameters.AddVarChar("@correlation", options.CorrelationId!));
        }

        if (options.CausationId != null && mapping.Metadata.CausationId.Enabled)
        {
            conditions.Add("causation_id = @causation");
            parameters.Add(cmd => cmd.Parameters.AddVarChar("@causation", options.CausationId!));
        }

        if (options.LastModifiedBy != null && mapping.Metadata.LastModifiedBy.Enabled)
        {
            conditions.Add("last_modified_by = @last_modified_by");
            parameters.Add(cmd => cmd.Parameters.AddVarChar("@last_modified_by", options.LastModifiedBy!));
        }

        var where = conditions.Count > 0 ? " where " + string.Join(" and ", conditions) : "";

        void Bind(SqlCommand command)
        {
            foreach (var p in parameters) p(command);
        }

        var table = mapping.QualifiedTableName;
        var selection = SelectionFor(mapping);

        // Under AllTenants the page orders by tenant FIRST, so a page boundary cannot repeat or skip a
        // row as tenants interleave.
        var order = conjoined && options.AllTenants ? "tenant_id, id" : "id";

        await using var conn = new SqlConnection(ConnectionStringFor(tenantId));
        await conn.OpenAsync(token).ConfigureAwait(false);

        long total;
        await using (var countCmd = conn.CreateCommand())
        {
            countCmd.CommandText = $"select count_big(*) from {table}{where}";
            Bind(countCmd);
            total = Convert.ToInt64(await countCmd.ExecuteScalarAsync(token).ConfigureAwait(false));
        }

        var rows = new List<StoredDocument>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText =
                $"select {selection} from {table}{where} " +
                $"order by {order} offset {offset} rows fetch next {pageSize} rows only";
            Bind(cmd);

            await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                rows.Add(ReadStoredDocument(reader, mapping));
            }
        }

        return new DocumentQueryResult(rows, total, pageNumber, pageSize);
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

        bind!(cmd);
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
    private static bool TryBindId(Storage.DocumentMapping mapping, string id, out Action<SqlCommand>? bind)
    {
        var type = Nullable.GetUnderlyingType(mapping.InnerIdType) ?? mapping.InnerIdType;

        if (type == typeof(Guid))
        {
            if (!Guid.TryParse(id, out var guid)) { bind = null; return false; }
            bind = cmd => cmd.Parameters.Add("@id", SqlDbType.UniqueIdentifier).Value = guid;
            return true;
        }

        if (type == typeof(int))
        {
            if (!int.TryParse(id, out var i)) { bind = null; return false; }
            bind = cmd => cmd.Parameters.Add("@id", SqlDbType.Int).Value = i;
            return true;
        }

        if (type == typeof(long))
        {
            if (!long.TryParse(id, out var l)) { bind = null; return false; }
            bind = cmd => cmd.Parameters.Add("@id", SqlDbType.BigInt).Value = l;
            return true;
        }

        // varchar id: VarChar rather than NVarChar so the comparison does not wrap the column in
        // CONVERT_IMPLICIT and scan (#363).
        bind = cmd => cmd.Parameters.AddVarChar("@id", id);
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
