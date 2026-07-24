using System.Diagnostics.CodeAnalysis;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Metadata;
using Polecat.Storage;

namespace Polecat.Internal;

/// <summary>
///     #242: <see cref="DocumentMetadata" /> read API. Selects just the metadata columns for a row —
///     no document body deserialization — so callers can answer audit / concurrency questions
///     (who/when/under-what-context) cheaply. Mirrors Marten's <c>MetadataForAsync</c>.
/// </summary>
internal partial class QuerySession
{
    public Task<DocumentMetadata?> MetadataForAsync<T>(T document, CancellationToken token = default)
        where T : notnull
    {
        var provider = _providers.GetProvider<T>();
        var id = provider.Mapping.GetId(document);
        return MetadataForIdAsync(provider, id, token);
    }

    public Task<DocumentMetadata?> MetadataForAsync<T>(Guid id, CancellationToken token = default) where T : class
        => MetadataForIdAsync(_providers.GetProvider<T>(), id, token);

    public Task<DocumentMetadata?> MetadataForAsync<T>(string id, CancellationToken token = default) where T : class
        => MetadataForIdAsync(_providers.GetProvider<T>(), id, token);

    public Task<DocumentMetadata?> MetadataForAsync<T>(int id, CancellationToken token = default) where T : class
        => MetadataForIdAsync(_providers.GetProvider<T>(), id, token);

    public Task<DocumentMetadata?> MetadataForAsync<T>(long id, CancellationToken token = default) where T : class
        => MetadataForIdAsync(_providers.GetProvider<T>(), id, token);

    private async Task<DocumentMetadata?> MetadataForIdAsync(DocumentProvider provider, object id,
        CancellationToken token)
    {
        var mapping = provider.Mapping;
        await _tableEnsurer.EnsureTableAsync(provider, token);

        // Build the column list off the mapping. Note: no soft-delete filter — metadata is surfaced
        // even for soft-deleted rows so the caller can see Deleted/DeletedAt.
        // #234: tenant_id exists only on conjoined tables.
        var isConjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;
        var columns = new List<string> { "id", "version", "last_modified", "created_at", "dotnet_type" };
        if (isConjoined) columns.Add("tenant_id");
        if (mapping.UseOptimisticConcurrency) columns.Add("guid_version");
        if (mapping.IsHierarchy()) columns.Add("doc_type");
        if (mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            columns.Add("is_deleted");
            columns.Add("deleted_at");
        }

        foreach (var column in mapping.EnabledMetadataColumns) columns.Add(column.Name);

        var ordinals = new Dictionary<string, int>();
        for (var i = 0; i < columns.Count; i++) ordinals[columns[i]] = i;

        await using var cmd = new SqlCommand();
        cmd.CommandText =
            $"SELECT {string.Join(", ", columns)} FROM {mapping.QualifiedTableName} " +
            (isConjoined ? "WHERE id = @id AND tenant_id = @tenant_id" : "WHERE id = @id");
        cmd.Parameters.AddIdParameter("@id", id);
        if (isConjoined) cmd.Parameters.AddVarChar("@tenant_id", TenantId);

        Logger.OnBeforeExecute(cmd.CommandText);
        try
        {
            await using var reader = await ExecuteReaderAsync(cmd, token);
            Logger.LogSuccess(cmd.CommandText);

            if (!await reader.ReadAsync(token)) return null;

            var metadata = new DocumentMetadata(
                reader.GetValue(ordinals["id"]),
                reader.GetInt64(ordinals["version"]),
                reader.GetFieldValue<DateTimeOffset>(ordinals["last_modified"]),
                reader.GetFieldValue<DateTimeOffset>(ordinals["created_at"]),
                isConjoined && !reader.IsDBNull(ordinals["tenant_id"])
                    ? reader.GetString(ordinals["tenant_id"])
                    : StorageConstants.DefaultTenantId)
            {
                DotNetType = ReadOptionalString(reader, ordinals, "dotnet_type"),
                DocumentType = ReadOptionalString(reader, ordinals, "doc_type"),
                GuidVersion = ordinals.TryGetValue("guid_version", out var gv) && !reader.IsDBNull(gv)
                    ? reader.GetGuid(gv)
                    : null,
                Deleted = ordinals.TryGetValue("is_deleted", out var d) && !reader.IsDBNull(d) && reader.GetBoolean(d),
                DeletedAt = ordinals.TryGetValue("deleted_at", out var da) && !reader.IsDBNull(da)
                    ? reader.GetFieldValue<DateTimeOffset>(da)
                    : null,
                CorrelationId = ReadOptionalString(reader, ordinals, "correlation_id"),
                CausationId = ReadOptionalString(reader, ordinals, "causation_id"),
                LastModifiedBy = ReadOptionalString(reader, ordinals, "last_modified_by"),
                Headers = ordinals.TryGetValue("headers", out var h) && !reader.IsDBNull(h)
                    ? Serializer.FromJson<Dictionary<string, object>>(reader.GetString(h))
                    : null
            };

            return metadata;
        }
        catch (Exception ex)
        {
            Logger.LogFailure(cmd.CommandText, ex);
            throw;
        }
    }

    [SuppressMessage("Trimming", "IL2026", Justification = "Reads a metadata column as a string; no reflection.")]
    private static string? ReadOptionalString(System.Data.Common.DbDataReader reader,
        IReadOnlyDictionary<string, int> ordinals, string column)
    {
        return ordinals.TryGetValue(column, out var i) && !reader.IsDBNull(i) ? reader.GetString(i) : null;
    }
}
