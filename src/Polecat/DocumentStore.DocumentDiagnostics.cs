using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Reflection;
using JasperFx.Documents;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;

namespace Polecat;

public partial class DocumentStore : IDocumentStoreDiagnostics
{
    /// <summary>
    /// Store-agnostic, read-only document-query surface for monitoring consoles
    /// (CritterWatch #545). Mirrors the role <see cref="JasperFx.Events.IEventStore"/>
    /// plays for event streams: list the mapped document types, page their stored
    /// rows as raw JSON, and fetch one by id — without the console referencing
    /// Polecat directly. Reads the canonical <c>data</c> column straight off the
    /// document table so the JSON matches exactly what Polecat persisted.
    /// </summary>
    Task<IReadOnlyList<DocumentTypeRef>> IDocumentStoreDiagnostics.DocumentTypesAsync(
        CancellationToken token)
    {
        var refs = MaterializeMappings()
            .OrderBy(m => m.DocumentType.Name)
            .Select(m => new DocumentTypeRef(
                m.DocumentType.FullNameInCode(),
                m.DocumentType.Name.ToLowerInvariant(),
                m.DatabaseSchemaName))
            .ToList();

        return Task.FromResult<IReadOnlyList<DocumentTypeRef>>(refs);
    }

    async Task<DocumentQueryResult> IDocumentStoreDiagnostics.QueryDocumentsAsync(
        string documentTypeName, DocumentQueryOptions options, CancellationToken token)
    {
        var mapping = ResolveMappingForDiagnostics(documentTypeName);
        if (mapping == null)
        {
            return new DocumentQueryResult(Array.Empty<string>(), 0, options.PageNumber, options.PageSize);
        }

        var table = mapping.QualifiedTableName;

        var pageNumber = Math.Max(1, options.PageNumber);
        var pageSize = Math.Max(1, options.PageSize);
        var offset = (pageNumber - 1) * pageSize;

        // Tenant scoping (#475 / EVENT_STORE_EXPLORER_PLAN §3.1): Polecat is a
        // single-database store, so a tenant id filters the conjoined tenant_id column.
        var filterByTenant = options.TenantId != null && mapping.TenancyStyle == TenancyStyle.Conjoined;

        // #256: exact-match metadata filters, honored only when the option is set AND the document
        // type actually persists that opt-in column (#241). A filter on a disabled column would
        // reference a column that doesn't exist, so it is silently skipped — mirroring the event side.
        var filterCorrelation = options.CorrelationId != null && mapping.Metadata.CorrelationId.Enabled;
        var filterCausation = options.CausationId != null && mapping.Metadata.CausationId.Enabled;
        var filterLastModifiedBy = options.LastModifiedBy != null && mapping.Metadata.LastModifiedBy.Enabled;

        var conditions = new List<string>();
        if (options.IdEquals != null) conditions.Add("cast(id as nvarchar(max)) = @id");
        if (filterByTenant) conditions.Add("tenant_id = @tenant");
        if (filterCorrelation) conditions.Add("correlation_id = @correlation");
        if (filterCausation) conditions.Add("causation_id = @causation");
        if (filterLastModifiedBy) conditions.Add("last_modified_by = @last_modified_by");
        var where = conditions.Count > 0 ? " where " + string.Join(" and ", conditions) : "";

        void BindParameters(SqlCommand command)
        {
            if (options.IdEquals != null) command.Parameters.AddWithValue("@id", options.IdEquals);
            if (filterByTenant) command.Parameters.AddWithValue("@tenant", options.TenantId!);
            if (filterCorrelation) command.Parameters.AddWithValue("@correlation", options.CorrelationId!);
            if (filterCausation) command.Parameters.AddWithValue("@causation", options.CausationId!);
            if (filterLastModifiedBy) command.Parameters.AddWithValue("@last_modified_by", options.LastModifiedBy!);
        }

        await using var conn = new SqlConnection(Database.ConnectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);

        long total;
        await using (var countCmd = conn.CreateCommand())
        {
            countCmd.CommandText = $"select count_big(*) from {table}{where}";
            BindParameters(countCmd);
            total = Convert.ToInt64(await countCmd.ExecuteScalarAsync(token).ConfigureAwait(false));
        }

        var rows = new List<string>();
        await using (var cmd = conn.CreateCommand())
        {
            // cast(data as nvarchar(max)) guarantees the column comes back as JSON text
            // whether it is stored as nvarchar or the native json type.
            cmd.CommandText =
                $"select cast(data as nvarchar(max)) from {table}{where} " +
                $"order by id offset {offset} rows fetch next {pageSize} rows only";
            BindParameters(cmd);

            await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                rows.Add(reader.GetString(0));
            }
        }

        return new DocumentQueryResult(rows, total, pageNumber, pageSize);
    }

    async Task<string?> IDocumentStoreDiagnostics.LoadDocumentJsonAsync(
        string documentTypeName, string id, CancellationToken token)
    {
        var mapping = ResolveMappingForDiagnostics(documentTypeName);
        if (mapping == null)
        {
            return null;
        }

        var table = mapping.QualifiedTableName;

        await using var conn = new SqlConnection(Database.ConnectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"select top 1 cast(data as nvarchar(max)) from {table} where cast(id as nvarchar(max)) = @id";
        cmd.Parameters.AddWithValue("@id", id);

        var result = await cmd.ExecuteScalarAsync(token).ConfigureAwait(false);
        return result == null || result is DBNull ? null : (string)result;
    }

    private Storage.DocumentMapping? ResolveMappingForDiagnostics(string documentTypeName)
    {
        return MaterializeMappings().FirstOrDefault(m =>
            string.Equals(m.DocumentType.FullNameInCode(), documentTypeName, StringComparison.OrdinalIgnoreCase)
            || string.Equals(m.DocumentType.FullName, documentTypeName, StringComparison.OrdinalIgnoreCase)
            || string.Equals(m.DocumentType.Name, documentTypeName, StringComparison.OrdinalIgnoreCase)
            || string.Equals(m.DocumentType.Name.ToLowerInvariant(), documentTypeName, StringComparison.OrdinalIgnoreCase));
    }

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
