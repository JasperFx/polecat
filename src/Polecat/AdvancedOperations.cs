using System.Text;
using Microsoft.Data.SqlClient;
using Polly;
using Polecat.Events.TestSupport;
using Polecat.Internal;
using Polecat.Metadata;
using Polecat.Schema.Identity.Sequences;
using Polecat.Storage;

namespace Polecat;

public class AdvancedOperations
{
    private readonly DocumentStore _store;
    private readonly ResiliencePipeline _resilience;

    internal AdvancedOperations(DocumentStore store)
    {
        _store = store;
        _resilience = store.Options.ResiliencePipeline;
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
            // Auto-assign Guid for strongly typed Guid wrappers when default
            if (mapping.IsStrongTypedId && mapping.InnerIdType == typeof(Guid))
            {
                var currentId = mapping.GetId(doc);
                if ((Guid)currentId == Guid.Empty)
                {
                    mapping.SetId(doc, Guid.NewGuid());
                }
            }
            // Assign HiLo ID if needed
            else if (mapping.IsNumericId && provider.Sequence != null)
            {
                var currentId = mapping.GetId(doc);
                if (mapping.InnerIdType == typeof(int) && (int)currentId <= 0)
                {
                    mapping.SetId(doc, provider.Sequence.NextInt());
                }
                else if (mapping.InnerIdType == typeof(long) && (long)currentId <= 0)
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

        // Execute in batches with Polly wrapping
        var connFactory = _store.Options.Tenancy!.GetConnectionFactory(tenantId);
        await _resilience.ExecuteAsync(async (_, ct) =>
        {
            await using var conn = connFactory.Create();
            await conn.OpenAsync(ct);

            for (var offset = 0; offset < rows.Count; offset += batchSize)
            {
                var batch = rows.Skip(offset).Take(batchSize).ToList();
                await using var cmd = conn.CreateCommand();
                BuildBatchCommand(cmd, batch, mapping, tenantId, mode);
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }, token);
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
                        $"INSERT INTO {mapping.QualifiedTableName} (id, data, version, last_modified, created_at, dotnet_type, tenant_id)");
                    sb.AppendLine(
                        $"VALUES ({pId}, {pData}, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), {pType}, {pTenant});");

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
                        $"    INSERT (id, data, version, last_modified, created_at, dotnet_type, tenant_id)");
                    sb.AppendLine(
                        $"    VALUES ({pId}, {pData}, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), {pType}, {pTenant});");

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
                        $"    INSERT (id, data, version, last_modified, created_at, dotnet_type, tenant_id)");
                    sb.AppendLine(
                        $"    VALUES ({pId}, {pData}, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), {pType}, {pTenant});");

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
        await _resilience.ExecuteAsync(async (_, ct) =>
        {
            await using var conn = new SqlConnection(_store.Options.ConnectionString);
            await conn.OpenAsync(ct);

            // Find all pc_doc_* tables in the schema
            await using var findCmd = conn.CreateCommand();
            findCmd.CommandText = $"""
                SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME LIKE 'pc_doc_%'
                ORDER BY TABLE_NAME;
                """;
            findCmd.Parameters.AddWithValue("@schema", schema);

            var tables = new List<string>();
            await using (var reader = await findCmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    tables.Add(reader.GetString(0));
                }
            }

            foreach (var table in tables)
            {
                await using var deleteCmd = conn.CreateCommand();
                deleteCmd.CommandText = $"DELETE FROM [{schema}].[{table}];";
                await deleteCmd.ExecuteNonQueryAsync(ct);
            }
        }, token);
    }

    /// <summary>
    ///     Delete all rows from the document table for type T.
    /// </summary>
    public async Task CleanAsync<T>(CancellationToken token = default)
    {
        var provider = _store.GetProvider(typeof(T));
        var tableName = provider.Mapping.QualifiedTableName;
        await _resilience.ExecuteAsync(async (_, ct) =>
        {
            await using var conn = new SqlConnection(_store.Options.ConnectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DELETE FROM {tableName};";
            await cmd.ExecuteNonQueryAsync(ct);
        }, token);
    }

    /// <summary>
    ///     Generate the full DDL script for all Polecat schema objects (event store tables + any registered document tables).
    /// </summary>
    public string ToDatabaseScript()
    {
        var sb = new StringBuilder();
        var writer = new StringWriter(sb);

        // Event store tables via Weasel
        foreach (var featureSchema in _store.Database.BuildFeatureSchemas())
        {
            foreach (var schemaObject in featureSchema.Objects)
            {
                schemaObject.WriteCreateStatement(new Weasel.SqlServer.SqlServerMigrator(), writer);
                writer.WriteLine();
                writer.WriteLine("GO");
                writer.WriteLine();
            }
        }

        // Document tables for already-registered providers
        foreach (var provider in _store.Options.Providers.AllProviders)
        {
            var table = new DocumentTable(provider.Mapping);
            table.WriteCreateStatement(new Weasel.SqlServer.SqlServerMigrator(), writer);
            writer.WriteLine();
            writer.WriteLine("GO");
            writer.WriteLine();
        }

        return sb.ToString();
    }

    /// <summary>
    ///     Write the full DDL creation script to a file.
    /// </summary>
    public async Task WriteCreationScriptToFileAsync(string path, CancellationToken token = default)
    {
        var script = ToDatabaseScript();
        await File.WriteAllTextAsync(path, script, token);
    }

    /// <summary>
    ///     Fetch the current size of the event store tables, including the current value
    ///     of the event sequence number.
    /// </summary>
    public async Task<Events.EventStoreStatistics> FetchEventStoreStatistics(CancellationToken token = default)
    {
        var events = _store.Events;
        var schema = events.DatabaseSchemaName;

        var sql = $"""
            SELECT COUNT(*) FROM [{schema}].[pc_events];
            SELECT COUNT(*) FROM [{schema}].[pc_streams];
            SELECT ISNULL(IDENT_CURRENT('[{schema}].[pc_events]'), 0);
            """;

        var statistics = new Events.EventStoreStatistics();

        await _resilience.ExecuteAsync(async (_, ct) =>
        {
            await using var conn = new SqlConnection(_store.Options.ConnectionString);
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            if (await reader.ReadAsync(ct))
                statistics.EventCount = reader.GetInt32(0);

            await reader.NextResultAsync(ct);
            if (await reader.ReadAsync(ct))
                statistics.StreamCount = reader.GetInt32(0);

            await reader.NextResultAsync(ct);
            if (await reader.ReadAsync(ct))
                statistics.EventSequenceNumber = Convert.ToInt64(reader.GetValue(0));
        }, token);

        return statistics;
    }

    /// <summary>
    ///     Configure and execute a batch masking of protected data for a subset of the events
    ///     in the event store. Used for GDPR right-to-erasure compliance.
    /// </summary>
    public Task ApplyEventDataMasking(Action<Events.Protected.IEventDataMasking> configure, CancellationToken token = default)
    {
        var masking = new Events.Protected.EventDataMasking(_store);
        configure(masking);
        return masking.ApplyAsync(token);
    }

    /// <summary>
    ///     Delete all rows from event store tables (pc_events, pc_streams, pc_event_progression)
    ///     and all natural key tables (pc_natural_key_*).
    /// </summary>
    public async Task CleanAllEventDataAsync(CancellationToken token = default)
    {
        var events = _store.Events;
        var schema = events.DatabaseSchemaName;
        await _resilience.ExecuteAsync(async (_, ct) =>
        {
            await using var conn = new SqlConnection(_store.Options.ConnectionString);
            await conn.OpenAsync(ct);

            // Delete natural key tables first (they reference streams)
            await using (var findCmd = conn.CreateCommand())
            {
                findCmd.CommandText = $"""
                    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = @schema AND TABLE_NAME LIKE 'pc_natural_key_%'
                    ORDER BY TABLE_NAME;
                    """;
                findCmd.Parameters.AddWithValue("@schema", schema);

                var nkTables = new List<string>();
                await using (var reader = await findCmd.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                    {
                        nkTables.Add(reader.GetString(0));
                    }
                }

                foreach (var table in nkTables)
                {
                    await using var deleteCmd = conn.CreateCommand();
                    deleteCmd.CommandText = $"DELETE FROM [{schema}].[{table}];";
                    await deleteCmd.ExecuteNonQueryAsync(ct);
                }
            }

            // Delete in FK-safe order: events first, then streams, then progression
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"DELETE FROM {events.EventsTableName};";
                await cmd.ExecuteNonQueryAsync(ct);
            }

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"DELETE FROM {events.StreamsTableName};";
                await cmd.ExecuteNonQueryAsync(ct);
            }

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"DELETE FROM {events.ProgressionTableName};";
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }, token);
    }

    /// <summary>
    ///     Run a projection scenario test that appends events and asserts against projected documents.
    ///     Automatically cleans data, starts the async daemon if needed, and waits for projections
    ///     to catch up before running assertions.
    /// </summary>
    public Task EventProjectionScenario(Action<ProjectionScenario> configuration, CancellationToken ct = default)
    {
        var scenario = new ProjectionScenario(_store);
        configuration(scenario);
        return scenario.Execute(ct);
    }
}
