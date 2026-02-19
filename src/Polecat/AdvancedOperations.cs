using System.Text;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Metadata;
using Polecat.Schema.Identity.Sequences;

namespace Polecat;

public class AdvancedOperations
{
    private readonly DocumentStore _store;

    internal AdvancedOperations(DocumentStore store)
    {
        _store = store;
    }

    public HiloSettings HiloSequenceDefaults => _store.Options.HiloSequenceDefaults;

    /// <summary>
    ///     Bulk insert documents with default settings (InsertsOnly, batch size 200, default tenant).
    /// </summary>
    public Task BulkInsertAsync<T>(IReadOnlyCollection<T> documents, CancellationToken token = default)
        where T : notnull
    {
        return BulkInsertAsync(documents, BulkInsertMode.InsertsOnly, 200, Tenancy.DefaultTenantId, token);
    }

    /// <summary>
    ///     Bulk insert documents with the specified mode and batch size.
    /// </summary>
    public Task BulkInsertAsync<T>(IReadOnlyCollection<T> documents, BulkInsertMode mode,
        int batchSize = 200, CancellationToken token = default) where T : notnull
    {
        return BulkInsertAsync(documents, mode, batchSize, Tenancy.DefaultTenantId, token);
    }

    /// <summary>
    ///     Bulk insert documents with full control over mode, batch size, and tenant.
    /// </summary>
    public async Task BulkInsertAsync<T>(IReadOnlyCollection<T> documents, BulkInsertMode mode,
        int batchSize, string tenantId, CancellationToken token = default) where T : notnull
    {
        if (documents.Count == 0) return;

        var provider = _store.GetProvider(typeof(T));
        var mapping = provider.Mapping;
        var serializer = _store.Options.Serializer;

        // Ensure the table exists
        var ensurer = new DocumentTableEnsurer(
            _store.Options.Tenancy!.GetConnectionFactory(tenantId), _store.Options);
        await ensurer.EnsureTableAsync(provider, token);

        // Pre-process: assign IDs, sync metadata, serialize
        var rows = new List<(object Id, string Json, string DotNetType)>(documents.Count);
        foreach (var doc in documents)
        {
            // Assign HiLo ID if needed
            if (mapping.IsNumericId && provider.Sequence != null)
            {
                var currentId = mapping.GetId(doc);
                if (mapping.IdType == typeof(int) && (int)currentId <= 0)
                {
                    mapping.SetId(doc, provider.Sequence.NextInt());
                }
                else if (mapping.IdType == typeof(long) && (long)currentId <= 0)
                {
                    mapping.SetId(doc, provider.Sequence.NextLong());
                }
            }

            // Sync metadata
            if (doc is ITracked tracked)
            {
                // No session-level correlation for bulk insert — leave as-is
            }

            if (doc is ITenanted tenanted)
            {
                tenanted.TenantId = tenantId;
            }

            var id = mapping.GetId(doc);
            var json = serializer.ToJson(doc);
            rows.Add((id, json, mapping.DotNetTypeName));
        }

        // Execute in batches
        var connFactory = _store.Options.Tenancy!.GetConnectionFactory(tenantId);
        await using var conn = connFactory.Create();
        await conn.OpenAsync(token);

        for (var offset = 0; offset < rows.Count; offset += batchSize)
        {
            var batch = rows.Skip(offset).Take(batchSize).ToList();
            await using var cmd = conn.CreateCommand();
            BuildBatchCommand(cmd, batch, mapping, tenantId, mode);
            await cmd.ExecuteNonQueryAsync(token);
        }
    }

    private static void BuildBatchCommand(
        SqlCommand cmd,
        List<(object Id, string Json, string DotNetType)> batch,
        Storage.DocumentMapping mapping,
        string tenantId,
        BulkInsertMode mode)
    {
        var sb = new StringBuilder();
        var paramIndex = 0;

        switch (mode)
        {
            case BulkInsertMode.InsertsOnly:
                foreach (var (id, json, dotnetType) in batch)
                {
                    var pId = $"@p{paramIndex++}";
                    var pData = $"@p{paramIndex++}";
                    var pType = $"@p{paramIndex++}";
                    var pTenant = $"@p{paramIndex++}";

                    sb.AppendLine(
                        $"INSERT INTO {mapping.QualifiedTableName} (id, data, version, last_modified, dotnet_type, tenant_id)");
                    sb.AppendLine(
                        $"VALUES ({pId}, {pData}, 1, SYSDATETIMEOFFSET(), {pType}, {pTenant});");

                    cmd.Parameters.AddWithValue(pId, id);
                    cmd.Parameters.AddWithValue(pData, json);
                    cmd.Parameters.AddWithValue(pType, dotnetType);
                    cmd.Parameters.AddWithValue(pTenant, tenantId);
                }

                break;

            case BulkInsertMode.IgnoreDuplicates:
                foreach (var (id, json, dotnetType) in batch)
                {
                    var pId = $"@p{paramIndex++}";
                    var pData = $"@p{paramIndex++}";
                    var pType = $"@p{paramIndex++}";
                    var pTenant = $"@p{paramIndex++}";

                    sb.AppendLine(
                        $"MERGE INTO {mapping.QualifiedTableName} WITH (HOLDLOCK) AS target");
                    sb.AppendLine(
                        $"USING (SELECT {pId} AS id, {pTenant} AS tenant_id) AS source");
                    sb.AppendLine(
                        "    ON target.id = source.id AND target.tenant_id = source.tenant_id");
                    sb.AppendLine("WHEN NOT MATCHED THEN");
                    sb.AppendLine(
                        $"    INSERT (id, data, version, last_modified, dotnet_type, tenant_id)");
                    sb.AppendLine(
                        $"    VALUES ({pId}, {pData}, 1, SYSDATETIMEOFFSET(), {pType}, {pTenant});");

                    cmd.Parameters.AddWithValue(pId, id);
                    cmd.Parameters.AddWithValue(pData, json);
                    cmd.Parameters.AddWithValue(pType, dotnetType);
                    cmd.Parameters.AddWithValue(pTenant, tenantId);
                }

                break;

            case BulkInsertMode.OverwriteExisting:
                foreach (var (id, json, dotnetType) in batch)
                {
                    var pId = $"@p{paramIndex++}";
                    var pData = $"@p{paramIndex++}";
                    var pType = $"@p{paramIndex++}";
                    var pTenant = $"@p{paramIndex++}";

                    sb.AppendLine(
                        $"MERGE INTO {mapping.QualifiedTableName} WITH (HOLDLOCK) AS target");
                    sb.AppendLine(
                        $"USING (SELECT {pId} AS id, {pTenant} AS tenant_id) AS source");
                    sb.AppendLine(
                        "    ON target.id = source.id AND target.tenant_id = source.tenant_id");
                    sb.AppendLine("WHEN MATCHED THEN");
                    sb.AppendLine(
                        $"    UPDATE SET data = {pData}, version = target.version + 1,");
                    sb.AppendLine(
                        $"        last_modified = SYSDATETIMEOFFSET(), dotnet_type = {pType}");
                    sb.AppendLine("WHEN NOT MATCHED THEN");
                    sb.AppendLine(
                        $"    INSERT (id, data, version, last_modified, dotnet_type, tenant_id)");
                    sb.AppendLine(
                        $"    VALUES ({pId}, {pData}, 1, SYSDATETIMEOFFSET(), {pType}, {pTenant});");

                    cmd.Parameters.AddWithValue(pId, id);
                    cmd.Parameters.AddWithValue(pData, json);
                    cmd.Parameters.AddWithValue(pType, dotnetType);
                    cmd.Parameters.AddWithValue(pTenant, tenantId);
                }

                break;
        }

        cmd.CommandText = sb.ToString();
    }

    public Task ResetHiloSequenceFloor<T>(long floor)
    {
        var sequence = _store.Sequences.SequenceFor(typeof(T));
        return sequence.SetFloor(floor);
    }

    /// <summary>
    ///     Delete all rows from all pc_doc_* tables in the configured schema.
    /// </summary>
    public async Task CleanAllDocumentsAsync(CancellationToken token = default)
    {
        var schema = _store.Options.DatabaseSchemaName;
        await using var conn = new SqlConnection(_store.Options.ConnectionString);
        await conn.OpenAsync(token);

        // Find all pc_doc_* tables in the schema
        await using var findCmd = conn.CreateCommand();
        findCmd.CommandText = $"""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = @schema AND TABLE_NAME LIKE 'pc_doc_%'
            ORDER BY TABLE_NAME;
            """;
        findCmd.Parameters.AddWithValue("@schema", schema);

        var tables = new List<string>();
        await using (var reader = await findCmd.ExecuteReaderAsync(token))
        {
            while (await reader.ReadAsync(token))
            {
                tables.Add(reader.GetString(0));
            }
        }

        foreach (var table in tables)
        {
            await using var deleteCmd = conn.CreateCommand();
            deleteCmd.CommandText = $"DELETE FROM [{schema}].[{table}];";
            await deleteCmd.ExecuteNonQueryAsync(token);
        }
    }

    /// <summary>
    ///     Delete all rows from the document table for type T.
    /// </summary>
    public async Task CleanAsync<T>(CancellationToken token = default)
    {
        var provider = _store.GetProvider(typeof(T));
        await using var conn = new SqlConnection(_store.Options.ConnectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM {provider.Mapping.QualifiedTableName};";
        await cmd.ExecuteNonQueryAsync(token);
    }

    /// <summary>
    ///     Delete all rows from event store tables (pc_events, pc_streams, pc_event_progression).
    /// </summary>
    public async Task CleanAllEventDataAsync(CancellationToken token = default)
    {
        var events = _store.Events;
        await using var conn = new SqlConnection(_store.Options.ConnectionString);
        await conn.OpenAsync(token);

        // Delete in FK-safe order: events first, then streams, then progression
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"DELETE FROM {events.EventsTableName};";
            await cmd.ExecuteNonQueryAsync(token);
        }

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"DELETE FROM {events.StreamsTableName};";
            await cmd.ExecuteNonQueryAsync(token);
        }

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"DELETE FROM {events.ProgressionTableName};";
            await cmd.ExecuteNonQueryAsync(token);
        }
    }
}
