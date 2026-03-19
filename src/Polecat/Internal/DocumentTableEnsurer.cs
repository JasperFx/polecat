using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Polecat.Metadata;
using Polecat.Storage;

namespace Polecat.Internal;

/// <summary>
///     Ensures document tables exist on demand. Uses Weasel to create tables
///     if they don't exist, and tracks which types have been ensured.
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

        await _semaphore.WaitAsync(token);
        try
        {
            // Double-check after acquiring lock
            if (_ensured.ContainsKey(docType))
            {
                return;
            }

            var table = new DocumentTable(provider.Mapping);

            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(token);

            // Ensure pc_hilo table for numeric ID types
            if (provider.Mapping.IsNumericId && !_hiloTableEnsured)
            {
                var hiloDdl = BuildHiloTableDdl(provider.Mapping.DatabaseSchemaName);
                await using var hiloCmd = conn.CreateCommand();
                hiloCmd.CommandText = hiloDdl;
                await hiloCmd.ExecuteNonQueryAsync(token);
                _hiloTableEnsured = true;
            }

            // Use raw DDL with IF NOT EXISTS for safety
            var ddl = BuildCreateTableDdl(provider.Mapping);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = ddl;
            await cmd.ExecuteNonQueryAsync(token);

            // Ensure created_at column exists (migration for tables created before this column was added)
            await using var migrateCmd = conn.CreateCommand();
            migrateCmd.CommandText = BuildAddMissingColumnsDdl(provider.Mapping);
            await migrateCmd.ExecuteNonQueryAsync(token);

            // Create custom indexes (computed columns + index)
            // Each statement executed separately so computed columns are visible
            // before filtered indexes reference them
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

    public async Task EnsureTablesAsync(IEnumerable<DocumentProvider> providers, CancellationToken token)
    {
        foreach (var provider in providers)
        {
            await EnsureTableAsync(provider, token);
        }
    }

    private static string BuildHiloTableDdl(string schema)
    {
        return $"""
            IF NOT EXISTS (SELECT 1 FROM sys.tables t
                           JOIN sys.schemas s ON t.schema_id = s.schema_id
                           WHERE s.name = '{schema}' AND t.name = 'pc_hilo')
            BEGIN
                IF SCHEMA_ID('{schema}') IS NULL
                    EXEC('CREATE SCHEMA [{schema}]');

                CREATE TABLE [{schema}].[pc_hilo] (
                    entity_name varchar(250) NOT NULL PRIMARY KEY,
                    hi_value bigint NOT NULL DEFAULT 0
                );
            END
            """;
    }

    private static string BuildCreateTableDdl(DocumentMapping mapping)
    {
        var schema = mapping.DatabaseSchemaName;
        var table = mapping.TableName;
        var innerIdType = mapping.InnerIdType;
        var idType = innerIdType == typeof(Guid) ? "uniqueidentifier"
            : innerIdType == typeof(int) ? "int"
            : innerIdType == typeof(long) ? "bigint"
            : "varchar(250)";
        var isConjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;
        var isSoftDelete = mapping.DeleteStyle == DeleteStyle.SoftDelete;
        var softDeleteCols = isSoftDelete
            ? @"
                    is_deleted bit NOT NULL DEFAULT 0,
                    deleted_at datetimeoffset NULL,"
            : "";
        var guidVersionCol = mapping.UseOptimisticConcurrency
            ? @"
                    guid_version uniqueidentifier NOT NULL DEFAULT NEWID(),"
            : "";
        var docTypeCol = mapping.IsHierarchy()
            ? @"
                    doc_type varchar(250) NOT NULL DEFAULT 'base',"
            : "";

        if (isConjoined)
        {
            return $@"
                IF NOT EXISTS (SELECT 1 FROM sys.tables t
                               JOIN sys.schemas s ON t.schema_id = s.schema_id
                               WHERE s.name = '{schema}' AND t.name = '{table}')
                BEGIN
                    IF SCHEMA_ID('{schema}') IS NULL
                        EXEC('CREATE SCHEMA [{schema}]');

                    CREATE TABLE [{schema}].[{table}] (
                        tenant_id varchar(250) NOT NULL,
                        id {idType} NOT NULL,
                        data nvarchar(max) NOT NULL,
                        version int NOT NULL DEFAULT 1,
                        last_modified datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET(),
                        created_at datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET(),
                        dotnet_type varchar(500) NULL,{docTypeCol}{softDeleteCols}{guidVersionCol}
                        CONSTRAINT pk_{table} PRIMARY KEY (tenant_id, id)
                    );
                END";
        }

        return $@"
            IF NOT EXISTS (SELECT 1 FROM sys.tables t
                           JOIN sys.schemas s ON t.schema_id = s.schema_id
                           WHERE s.name = '{schema}' AND t.name = '{table}')
            BEGIN
                IF SCHEMA_ID('{schema}') IS NULL
                    EXEC('CREATE SCHEMA [{schema}]');

                CREATE TABLE [{schema}].[{table}] (
                    id {idType} NOT NULL PRIMARY KEY,
                    data nvarchar(max) NOT NULL,
                    version int NOT NULL DEFAULT 1,
                    last_modified datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET(),
                    created_at datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET(),
                    dotnet_type varchar(500) NULL,{docTypeCol}{softDeleteCols}{guidVersionCol}
                    tenant_id varchar(250) NOT NULL DEFAULT '*DEFAULT*'
                );
            END";
    }

    private static string BuildAddMissingColumnsDdl(DocumentMapping mapping)
    {
        var schema = mapping.DatabaseSchemaName;
        var table = mapping.TableName;
        return $"""
            IF NOT EXISTS (SELECT 1 FROM sys.columns
                           WHERE object_id = OBJECT_ID('[{schema}].[{table}]')
                             AND name = 'created_at')
            BEGIN
                ALTER TABLE [{schema}].[{table}]
                    ADD created_at datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET();
            END
            """;
    }

}
