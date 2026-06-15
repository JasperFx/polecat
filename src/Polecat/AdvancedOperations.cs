using System.Diagnostics.CodeAnalysis;
using System.Text;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polly;
using Polecat.Events.TestSupport;
using Polecat.Internal;
using Polecat.Metadata;
using Polecat.Projections.Flattened;
using Polecat.Schema.Identity.Sequences;
using Polecat.Storage;
using Weasel.Core;

namespace Polecat;

[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: BulkInsertAsync / BulkInsertWithVersionAsync stream documents through ISerializer.ToJson. Document types T flow in from caller code (BulkInsertAsync<T>(docs)) and are preserved per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.ToJson is annotated RDC. AOT consumers supply a source-generator-backed ISerializer impl.")]
public class AdvancedOperations
{
    private readonly DocumentStore _store;
    private readonly ResiliencePipeline _resilience;
    private IDocumentCleaner? _cleaner;

    internal AdvancedOperations(DocumentStore store)
    {
        _store = store;
        _resilience = store.Options.ResiliencePipeline;
    }

    public HiloSettings HiloSequenceDefaults => _store.Options.HiloSequenceDefaults;

    /// <summary>
    ///     The schema-scoped clean/reset surface for this store. Mirrors Marten's
    ///     <c>AdvancedOperations.Clean</c>. All operations target only this store's
    ///     configured schema (<see cref="StoreOptions.DatabaseSchemaName" />).
    /// </summary>
    public IDocumentCleaner Clean => _cleaner ??= new Internal.PolecatDocumentCleaner(this);

    /// <summary>
    ///     Delete all current document and event data for this store (scoped to the store's
    ///     configured schema), then re-apply any registered <see cref="StoreOptions.InitialData" />.
    ///     Mirrors Marten's <c>AdvancedOperations.ResetAllData</c>. The per-store scoping is what
    ///     lets an ancillary store (<c>AddPolecatStore&lt;T&gt;</c> with its own schema) reset its
    ///     own data on boot without touching the host application's data (polecat#191).
    /// </summary>
    public async Task ResetAllData(CancellationToken cancellation = default)
    {
        await CleanAllDocumentsAsync(cancellation).ConfigureAwait(false);
        await CleanAllEventDataAsync(cancellation).ConfigureAwait(false);

        foreach (var initialData in _store.Options.InitialData)
        {
            await initialData.Populate(_store, cancellation).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Bulk insert documents with default settings (InsertsOnly, batch size 200, default tenant).
    /// </summary>
    public Task BulkInsertAsync<T>(IReadOnlyCollection<T> documents, CancellationToken token = default)
        where T : notnull
    {
        return BulkInsertAsync(documents, BulkInsertMode.InsertsOnly, 200, JasperFx.StorageConstants.DefaultTenantId, token);
    }

    /// <summary>
    ///     Bulk insert documents with the specified mode and batch size.
    /// </summary>
    public Task BulkInsertAsync<T>(IReadOnlyCollection<T> documents, BulkInsertMode mode,
        int batchSize = 200, CancellationToken token = default) where T : notnull
    {
        return BulkInsertAsync(documents, mode, batchSize, JasperFx.StorageConstants.DefaultTenantId, token);
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
        var ensurer = _store.ResolveTableEnsurer(tenantId);
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
                // No session-level correlation for bulk insert � leave as-is
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
        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (factory, allRows, size, docMapping, tenant, insertMode) = state;
            await using var conn = factory.Create();
            await conn.OpenAsync(ct);

            for (var offset = 0; offset < allRows.Count; offset += size)
            {
                var batch = allRows.Skip(offset).Take(size).ToList();
                await using var cmd = conn.CreateCommand();
                BuildBatchCommand(cmd, batch, docMapping, tenant, insertMode);
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }, (connFactory, rows, batchSize, mapping, tenantId, mode), token);
    }

    /// <summary>
    ///     Bulk insert documents with per-document expected versions, using
    ///     <see cref="BulkInsertMode.OverwriteIfVersionMatches"/> semantics. Each
    ///     incoming row is paired with the version the caller expects to find
    ///     in the table; the merge updates only when the stored version equals
    ///     the expected version, inserts when no row exists, and throws
    ///     <see cref="ConcurrencyException"/> when a row exists but the stored
    ///     version does not match. Closes
    ///     <see href="https://github.com/JasperFx/polecat/issues/48">polecat#48</see>.
    /// </summary>
    public Task BulkInsertWithVersionAsync<T>(
        IReadOnlyCollection<(T Document, long ExpectedVersion)> documents,
        CancellationToken token = default) where T : notnull
    {
        return BulkInsertWithVersionAsync(documents, 200, JasperFx.StorageConstants.DefaultTenantId, token);
    }

    /// <summary>
    ///     Bulk insert documents with per-document expected versions and a
    ///     custom batch size. See
    ///     <see cref="BulkInsertWithVersionAsync{T}(System.Collections.Generic.IReadOnlyCollection{System.ValueTuple{T,long}},System.Threading.CancellationToken)"/>.
    /// </summary>
    public Task BulkInsertWithVersionAsync<T>(
        IReadOnlyCollection<(T Document, long ExpectedVersion)> documents,
        int batchSize,
        CancellationToken token = default) where T : notnull
    {
        return BulkInsertWithVersionAsync(documents, batchSize, JasperFx.StorageConstants.DefaultTenantId, token);
    }

    /// <summary>
    ///     Bulk insert documents with per-document expected versions, custom
    ///     batch size, and explicit tenant id.
    /// </summary>
    public async Task BulkInsertWithVersionAsync<T>(
        IReadOnlyCollection<(T Document, long ExpectedVersion)> documents,
        int batchSize,
        string tenantId,
        CancellationToken token = default) where T : notnull
    {
        if (documents.Count == 0) return;

        var provider = _store.GetProvider(typeof(T));
        var mapping = provider.Mapping;
        var serializer = _store.Options.Serializer;

        var ensurer = _store.ResolveTableEnsurer(tenantId);
        await ensurer.EnsureTableAsync(provider, token);

        // Pre-process. The id-assignment and metadata sync mirrors the
        // versionless path; the only extra piece is carrying the expected
        // version forward into the batched MERGE.
        var rows = new List<(object Id, string Json, string DotNetType, long ExpectedVersion)>(documents.Count);
        foreach (var (doc, expectedVersion) in documents)
        {
            if (mapping.IsStrongTypedId && mapping.InnerIdType == typeof(Guid))
            {
                var currentId = mapping.GetId(doc);
                if ((Guid)currentId == Guid.Empty)
                {
                    mapping.SetId(doc, Guid.NewGuid());
                }
            }
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

            if (doc is ITenanted tenanted)
            {
                tenanted.TenantId = tenantId;
            }

            var id = mapping.GetId(doc);
            var json = serializer.ToJson(doc);
            rows.Add((id, json, mapping.DotNetTypeName, expectedVersion));
        }

        var connFactory = _store.Options.Tenancy!.GetConnectionFactory(tenantId);

        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (factory, allRows, size, docMapping, tenant) = state;
            await using var conn = factory.Create();
            await conn.OpenAsync(ct);

            for (var offset = 0; offset < allRows.Count; offset += size)
            {
                var batch = allRows.Skip(offset).Take(size).ToList();
                await using var cmd = conn.CreateCommand();
                BuildVersionCheckedBatchCommand(cmd, batch, docMapping, tenant);

                // Read the OUTPUTed inserted.id stream to discover which rows
                // the MERGE actually touched. Any incoming id missing from
                // the output set is a version-mismatch (the WHEN MATCHED AND
                // ... predicate failed and the row was neither updated nor
                // inserted). One ConcurrencyException per batch, thrown for
                // the first missing id we find — matches the per-row pattern
                // in UpdateOperation / UpsertOperation rather than the
                // aggregate pattern.
                //
                // Each MERGE-OUTPUT in the batched command produces its own
                // result set; iterate all of them via NextResultAsync.
                var touched = new HashSet<object>();
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    do
                    {
                        while (await reader.ReadAsync(ct))
                        {
                            touched.Add(reader.GetValue(0));
                        }
                    } while (await reader.NextResultAsync(ct));
                }

                foreach (var row in batch)
                {
                    if (!touched.Contains(row.Id))
                    {
                        throw new ConcurrencyException(typeof(T), row.Id);
                    }
                }
            }
        }, (connFactory, rows, batchSize, mapping, tenantId), token);
    }

    private static void BuildVersionCheckedBatchCommand(
        SqlCommand cmd,
        List<(object Id, string Json, string DotNetType, long ExpectedVersion)> batch,
        Storage.DocumentMapping mapping,
        string tenantId)
    {
        var sb = new StringBuilder();
        var paramIndex = 0;

        foreach (var (id, json, dotnetType, expectedVersion) in batch)
        {
            var pId = $"@p{paramIndex++}";
            var pData = $"@p{paramIndex++}";
            var pType = $"@p{paramIndex++}";
            var pTenant = $"@p{paramIndex++}";
            var pExpected = $"@p{paramIndex++}";

            // MERGE pattern from the polecat#48 audit body. The expected_version
            // is part of the USING projection rather than a free-standing
            // parameter so SQL Server's optimizer can see it as a column on the
            // source rowset (matters when this is extended to set-based MERGEs).
            sb.AppendLine(
                $"MERGE INTO {mapping.QualifiedTableName} WITH (HOLDLOCK) AS target");
            sb.AppendLine(
                $"USING (SELECT {pId} AS id, {pTenant} AS tenant_id, {pExpected} AS expected_version) AS source");
            sb.AppendLine(
                "    ON target.id = source.id AND target.tenant_id = source.tenant_id");
            sb.AppendLine("WHEN MATCHED AND target.version = source.expected_version THEN");
            sb.AppendLine(
                $"    UPDATE SET data = {pData}, version = target.version + 1,");
            sb.AppendLine(
                $"        last_modified = SYSDATETIMEOFFSET(), dotnet_type = {pType}");
            sb.AppendLine("WHEN NOT MATCHED THEN");
            sb.AppendLine(
                $"    INSERT (id, data, version, last_modified, created_at, dotnet_type, tenant_id)");
            sb.AppendLine(
                $"    VALUES ({pId}, {pData}, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), {pType}, {pTenant})");
            sb.AppendLine("OUTPUT inserted.id;");

            cmd.Parameters.AddWithValue(pId, id);
            cmd.Parameters.AddWithValue(pData, json);
            cmd.Parameters.AddWithValue(pType, dotnetType);
            cmd.Parameters.AddWithValue(pTenant, tenantId);
            cmd.Parameters.AddWithValue(pExpected, expectedVersion);
        }

        cmd.CommandText = sb.ToString();
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

            case BulkInsertMode.OverwriteIfVersionMatches:
                // OverwriteIfVersionMatches is not reachable through the
                // versionless BulkInsertAsync surface — it requires a
                // per-document expected version that this method has no
                // way to receive. Callers should switch to
                // BulkInsertWithVersionAsync, which threads (T, long)
                // pairs through a sibling code path.
                throw new InvalidOperationException(
                    "BulkInsertMode.OverwriteIfVersionMatches requires a per-document expected version " +
                    "and is not available through BulkInsertAsync. " +
                    "Call BulkInsertWithVersionAsync(IReadOnlyCollection<(T document, long expectedVersion)>, ...) instead.");
        }

        cmd.CommandText = sb.ToString();
    }

    public Task ResetHiloSequenceFloor<T>(long floor)
    {
        var sequence = _store.Sequences.SequenceFor(typeof(T));
        return sequence.SetFloor(floor);
    }

    /// <summary>
    ///     Delete all rows from all pc_doc_* tables in the configured schema, plus the custom tables
    ///     owned by any registered <see cref="FlatTableProjection" /> (their projected document-like
    ///     data is not stored in pc_doc_* tables, so it is cleaned explicitly — polecat#181).
    /// </summary>
    public async Task CleanAllDocumentsAsync(CancellationToken token = default)
    {
        var schema = _store.Options.DatabaseSchemaName;
        var connStr = _store.Options.ConnectionString;
        var flatTables = CollectFlatTableNames();
        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, schemaName, flatTableNames) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            // Find all pc_doc_* tables in the schema
            await using var findCmd = conn.CreateCommand();
            findCmd.CommandText = $"""
                SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME LIKE 'pc_doc_%'
                ORDER BY TABLE_NAME;
                """;
            findCmd.Parameters.AddWithValue("@schema", schemaName);

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
                deleteCmd.CommandText = $"DELETE FROM [{schemaName}].[{table}];";
                await deleteCmd.ExecuteNonQueryAsync(ct);
            }

            await DeleteFlatTablesAsync(conn, flatTableNames, ct);
        }, (connStr, schema, flatTables), token);
    }

    /// <summary>
    ///     Drop all Polecat schema objects in this store's configured schema — every
    ///     <c>pc_*</c> table plus any <see cref="FlatTableProjection" /> table. Unlike the
    ///     <c>CleanAll*</c> methods (which only delete rows), this removes the tables
    ///     themselves. Mirrors Marten's <c>IDocumentCleaner.CompletelyRemoveAllAsync</c>
    ///     (polecat#191). Foreign keys are dropped first so the tables can be removed in any
    ///     order; missing tables are ignored so this is safe to call repeatedly.
    /// </summary>
    public async Task CompletelyRemoveAllAsync(CancellationToken token = default)
    {
        var schema = _store.Options.DatabaseSchemaName;
        var connStr = _store.Options.ConnectionString;
        var flatTables = CollectFlatTableNames();
        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, schemaName, flatTableNames) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            // Drop every foreign key on pc_* tables first so the tables themselves can be
            // dropped in any order ([_] escapes the LIKE wildcard to mean a literal underscore).
            await using (var dropFks = conn.CreateCommand())
            {
                dropFks.CommandText = """
                    DECLARE @sql nvarchar(max) = N'';
                    SELECT @sql += N'ALTER TABLE [' + s.name + N'].[' + t.name + N'] DROP CONSTRAINT [' + fk.name + N'];'
                    FROM sys.foreign_keys fk
                    JOIN sys.tables t ON fk.parent_object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = @schema AND t.name LIKE 'pc[_]%';
                    IF @sql <> N'' EXEC sp_executesql @sql;
                    """;
                dropFks.Parameters.AddWithValue("@schema", schemaName);
                await dropFks.ExecuteNonQueryAsync(ct);
            }

            // Find every pc_* base table in the schema and drop it.
            var tables = new List<string>();
            await using (var findCmd = conn.CreateCommand())
            {
                findCmd.CommandText = """
                    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = @schema AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE 'pc[_]%'
                    ORDER BY TABLE_NAME;
                    """;
                findCmd.Parameters.AddWithValue("@schema", schemaName);
                await using var reader = await findCmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    tables.Add(reader.GetString(0));
                }
            }

            foreach (var table in tables)
            {
                await using var dropCmd = conn.CreateCommand();
                dropCmd.CommandText = $"DROP TABLE IF EXISTS [{schemaName}].[{table}];";
                await dropCmd.ExecuteNonQueryAsync(ct);
            }

            // FlatTableProjection tables may live outside the pc_ prefix; drop them explicitly.
            foreach (var qualified in flatTableNames)
            {
                await using var dropCmd = conn.CreateCommand();
                dropCmd.CommandText = $"IF OBJECT_ID('{qualified}', 'U') IS NOT NULL DROP TABLE {qualified};";
                await dropCmd.ExecuteNonQueryAsync(ct);
            }
        }, (connStr, schema, flatTables), token);
    }

    /// <summary>
    ///     The schema-qualified tables owned by registered <see cref="FlatTableProjection" /> sources.
    /// </summary>
    private string[] CollectFlatTableNames()
    {
        return _store.Options.Projections.All
            .OfType<FlatTableProjection>()
            .Select(p => p.Table.Identifier.QualifiedName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static async Task DeleteFlatTablesAsync(SqlConnection conn, string[] qualifiedNames,
        CancellationToken ct)
    {
        foreach (var qualified in qualifiedNames)
        {
            await using var cmd = conn.CreateCommand();
            // Guard against the table not existing yet (the projection ensures it lazily).
            cmd.CommandText = $"IF OBJECT_ID('{qualified}', 'U') IS NOT NULL DELETE FROM {qualified};";
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>
    ///     Delete all rows from the document table for type T.
    /// </summary>
    public async Task CleanAsync<T>(CancellationToken token = default)
    {
        var provider = _store.GetProvider(typeof(T));
        var tableName = provider.Mapping.QualifiedTableName;
        var connStr = _store.Options.ConnectionString;
        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, qualifiedTableName) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF OBJECT_ID('{qualifiedTableName}', 'U') IS NOT NULL DELETE FROM {qualifiedTableName};";
            await cmd.ExecuteNonQueryAsync(ct);
        }, (connStr, tableName), token);
    }

    /// <summary>
    ///     Generate the full DDL script for all Polecat schema objects (event store tables, document tables, HiLo).
    /// </summary>
    public string ToDatabaseScript()
    {
        var sb = new StringBuilder();
        var writer = new StringWriter(sb);
        var migrator = new Weasel.SqlServer.SqlServerMigrator();

        // All schema objects (event store + documents + hilo) via Weasel feature schemas
        foreach (var featureSchema in _store.Database.BuildFeatureSchemas())
        {
            foreach (var schemaObject in featureSchema.Objects)
            {
                schemaObject.WriteCreateStatement(migrator, writer);
                writer.WriteLine();
                writer.WriteLine("GO");
                writer.WriteLine();
            }
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
        var connStr = _store.Options.ConnectionString;

        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, sqlText, stats) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sqlText;
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            if (await reader.ReadAsync(ct))
                stats.EventCount = reader.GetInt32(0);

            await reader.NextResultAsync(ct);
            if (await reader.ReadAsync(ct))
                stats.StreamCount = reader.GetInt32(0);

            await reader.NextResultAsync(ct);
            if (await reader.ReadAsync(ct))
                stats.EventSequenceNumber = Convert.ToInt64(reader.GetValue(0));
        }, (connStr, sql, statistics), token);

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
        var connStr = _store.Options.ConnectionString;
        var eventsTable = events.EventsTableName;
        var streamsTable = events.StreamsTableName;
        var progressionTable = events.ProgressionTableName;
        var flatTables = CollectFlatTableNames();

        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, schemaName, evtTable, strmTable, progTable, flatTableNames) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            // Flat-table projection output is derived from events; clear it alongside the event data
            // so a reset leaves no stale projected rows (mirrors the natural-key table cleanup below).
            await DeleteFlatTablesAsync(conn, flatTableNames, ct);

            // Delete natural key tables first (they reference streams)
            await using (var findCmd = conn.CreateCommand())
            {
                findCmd.CommandText = $"""
                    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = @schema AND TABLE_NAME LIKE 'pc_natural_key_%'
                    ORDER BY TABLE_NAME;
                    """;
                findCmd.Parameters.AddWithValue("@schema", schemaName);

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
                    deleteCmd.CommandText = $"DELETE FROM [{schemaName}].[{table}];";
                    await deleteCmd.ExecuteNonQueryAsync(ct);
                }
            }

            // Delete in FK-safe order: events first, then streams, then progression.
            // Guard against missing tables so this is safe to call before the event
            // store schema has been created (e.g. during bootstrap of a fresh database).
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"IF OBJECT_ID('{evtTable}', 'U') IS NOT NULL DELETE FROM {evtTable};";
                await cmd.ExecuteNonQueryAsync(ct);
            }

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"IF OBJECT_ID('{strmTable}', 'U') IS NOT NULL DELETE FROM {strmTable};";
                await cmd.ExecuteNonQueryAsync(ct);
            }

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"IF OBJECT_ID('{progTable}', 'U') IS NOT NULL DELETE FROM {progTable};";
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }, (connStr, schema, eventsTable, streamsTable, progressionTable, flatTables), token);
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
