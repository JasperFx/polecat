using System.Collections.Concurrent;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Schema.Identity.Sequences;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal;

/// <summary>
///     Ensures document tables exist on demand. Uses Weasel SchemaMigration to create
///     or update tables, and tracks which types have been ensured.
/// </summary>
internal class DocumentTableEnsurer
{
    private readonly ConcurrentDictionary<Type, bool> _ensured = new();
    private readonly ConcurrentDictionary<Type, bool> _fksEnsured = new();
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly ConnectionFactory _connectionFactory;
    private readonly StoreOptions _options;
    private DocumentProviderRegistry? _providerRegistry;

    public DocumentTableEnsurer(ConnectionFactory connectionFactory, StoreOptions options)
    {
        _connectionFactory = connectionFactory;
        _options = options;
    }

    internal void SetProviderRegistry(DocumentProviderRegistry registry)
    {
        _providerRegistry = registry;
    }

    private bool _hiloTableEnsured;

    public async Task EnsureTableAsync(DocumentProvider provider, CancellationToken token)
    {
        var docType = provider.Mapping.DocumentType;

        if (_ensured.ContainsKey(docType))
        {
            return;
        }

        // #219: honor the user's explicit opt-out. AutoCreate.None means "I manage the schema",
        // so never create/alter tables implicitly on first use (mirrors Marten).
        if (_options.AutoCreateSchemaObjects == AutoCreate.None)
        {
            _ensured.TryAdd(docType, true);
            return;
        }

        await _semaphore.WaitAsync(token);
        try
        {
            // Double-check after acquiring lock
            if (_ensured.ContainsKey(docType))
            {
                return;
            }

            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(token);

            var migrator = new SqlServerMigrator();

            // Ensure pc_hilo table for numeric ID types
            if (provider.Mapping.IsNumericId && !_hiloTableEnsured)
            {
                var hiloTable = new HiloTable(provider.Mapping.DatabaseSchemaName);
                var hiloMigration = await SchemaMigration.DetermineAsync(conn, token, hiloTable);
                await migrator.ApplyAllAsync(conn, hiloMigration, AutoCreate.CreateOrUpdate, ct: token);
                _hiloTableEnsured = true;
            }

            // Decision D2 widened the document `version` column to bigint. Existing tables created
            // before this carry an int column; widen it in place before Weasel diffs the table.
            // SQL Server rejects ALTER COLUMN while a default constraint references the column, and
            // Weasel's diff emits only a bare ALTER COLUMN — so this drops the default, widens (data
            // preserved — never a drop/recreate), then restores the default. No-op once bigint.
            await WidenVersionColumnIfNeededAsync(conn, provider.Mapping.QualifiedTableName, token);

            // Use Weasel SchemaMigration to create or update the document table.
            // #255: an externally-managed partitioned table is provisioned ONCE (CreateOnly) and
            // never reconciled afterward, so a later schema apply can't clobber the partitions the
            // app/DBA manages at runtime (SPLIT new months, SWITCH/DROP old ones for retention).
            var table = new DocumentTable(provider.Mapping);
            var autoCreate = provider.Mapping.Partitioning is { ExternallyManaged: true }
                ? AutoCreate.CreateOnly
                : AutoCreate.CreateOrUpdate;
            var migration = await SchemaMigration.DetermineAsync(conn, token, table);
            await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: token);

            // Create custom indexes (computed columns + index)
            // Computed columns are not modeled in Weasel, so they remain as supplementary DDL.
            // Each statement executed separately so computed columns are visible
            // before filtered indexes reference them.
            foreach (var index in provider.Mapping.Indexes)
            {
                foreach (var statement in index.ToDdlStatements(provider.Mapping))
                {
                    await using var indexCmd = conn.CreateCommand();
                    indexCmd.CommandText = statement;
                    await indexCmd.ExecuteNonQueryAsync(token);
                }
            }

            _ensured.TryAdd(docType, true);
        }
        finally
        {
            _semaphore.Release();
        }

        // Create foreign keys (deferred — referenced tables must exist first)
        await EnsureForeignKeysAsync(provider, token);
    }

    private async Task EnsureForeignKeysAsync(DocumentProvider provider, CancellationToken token)
    {
        if (provider.Mapping.ForeignKeys.Count == 0) return;
        if (_fksEnsured.ContainsKey(provider.Mapping.DocumentType)) return;
        if (_providerRegistry == null) return;

        // Ensure all referenced tables exist BEFORE acquiring the FK semaphore
        // to avoid deadlock (EnsureTableAsync also acquires _semaphore)
        foreach (var fk in provider.Mapping.ForeignKeys)
        {
            var refProvider = _providerRegistry.GetProvider(fk.ReferenceDocumentType);
            await EnsureTableAsync(refProvider, token);
        }

        await _semaphore.WaitAsync(token);
        try
        {
            if (_fksEnsured.ContainsKey(provider.Mapping.DocumentType)) return;

            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(token);

            foreach (var fk in provider.Mapping.ForeignKeys)
            {
                var refProvider = _providerRegistry.GetProvider(fk.ReferenceDocumentType);
                foreach (var statement in fk.ToDdlStatements(provider.Mapping, refProvider.Mapping))
                {
                    await using var fkCmd = conn.CreateCommand();
                    fkCmd.CommandText = statement;
                    await fkCmd.ExecuteNonQueryAsync(token);
                }
            }

            _fksEnsured.TryAdd(provider.Mapping.DocumentType, true);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    ///     Normalizes an existing document <c>version</c> column to the Decision D2 shape:
    ///     <c>bigint NOT NULL</c> with no default constraint. Drops any lingering default (the model
    ///     carries none, and a default would block the ALTER below), then widens an int column to
    ///     bigint in place — never a drop/recreate, so rows are preserved. Idempotent: a no-op once
    ///     the table is absent or already bigint with no default.
    /// </summary>
    private static async Task WidenVersionColumnIfNeededAsync(SqlConnection conn, string qualifiedTableName,
        CancellationToken token)
    {
        // The table name is derived from the document type (not user input), so it is inlined
        // directly; only the discovered default-constraint name needs dynamic SQL (and SQL Server
        // forbids function calls inside EXEC(...), hence the SET-then-EXEC pattern).
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            DECLARE @oid int = OBJECT_ID('{qualifiedTableName}');
            IF @oid IS NOT NULL
            BEGIN
                DECLARE @df sysname, @sql nvarchar(max);
                SELECT @df = dc.name FROM sys.default_constraints dc
                    JOIN sys.columns col ON col.default_object_id = dc.object_id
                    WHERE dc.parent_object_id = @oid AND col.name = 'version';
                IF @df IS NOT NULL
                BEGIN
                    SET @sql = 'ALTER TABLE {qualifiedTableName} DROP CONSTRAINT ' + QUOTENAME(@df);
                    EXEC(@sql);
                END
                IF EXISTS (
                    SELECT 1 FROM sys.columns c
                    WHERE c.object_id = @oid AND c.name = 'version' AND TYPE_NAME(c.system_type_id) = 'int')
                    ALTER TABLE {qualifiedTableName} ALTER COLUMN [version] bigint NOT NULL;
            END
            """;
        await cmd.ExecuteNonQueryAsync(token);
    }

    public async Task EnsureTablesAsync(IEnumerable<DocumentProvider> providers, CancellationToken token)
    {
        foreach (var provider in providers)
        {
            await EnsureTableAsync(provider, token);
        }
    }

    private volatile bool _eventStoreEnsured;
    private readonly SemaphoreSlim _eventStoreSemaphore = new(1, 1);

    /// <summary>
    ///     #219: ensures the event store schema (streams / events / progression tables, plus any tag
    ///     and natural-key tables) exists on first usage of the event store — the event-sourcing
    ///     analogue of EnsureTableAsync for documents. Idempotent and applied once per process; a
    ///     no-op under AutoCreate.None so the user's manual-schema opt-out is respected.
    /// </summary>
    public async Task EnsureEventStoreSchemaAsync(CancellationToken token)
    {
        if (_eventStoreEnsured) return;

        if (_options.AutoCreateSchemaObjects == AutoCreate.None)
        {
            _eventStoreEnsured = true;
            return;
        }

        await _eventStoreSemaphore.WaitAsync(token);
        try
        {
            if (_eventStoreEnsured) return;

            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(token);

            var migrator = new SqlServerMigrator();

            // Mirror PolecatDatabase.BuildFeatureSchemas: the event store feature owns the natural-key
            // tables for aggregate projections that declare one.
            var naturalKeys = _options.Projections.All
                .OfType<JasperFx.Events.Aggregation.IAggregateProjection>()
                .Where(p => p.NaturalKeyDefinition != null)
                .Select(p => p.NaturalKeyDefinition!)
                .ToList();

            var eventSchema = new Events.Schema.EventStoreFeatureSchema(_options.EventGraph, naturalKeys);
            var migration = await SchemaMigration.DetermineAsync(conn, token, eventSchema.Objects);
            await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: token);

            _eventStoreEnsured = true;
        }
        finally
        {
            _eventStoreSemaphore.Release();
        }
    }
}
