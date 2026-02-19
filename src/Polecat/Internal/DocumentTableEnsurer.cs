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
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly ConnectionFactory _connectionFactory;
    private readonly StoreOptions _options;

    public DocumentTableEnsurer(ConnectionFactory connectionFactory, StoreOptions options)
    {
        _connectionFactory = connectionFactory;
        _options = options;
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

            _ensured.TryAdd(docType, true);
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
        var idType = mapping.IdType == typeof(Guid) ? "uniqueidentifier"
            : mapping.IdType == typeof(int) ? "int"
            : mapping.IdType == typeof(long) ? "bigint"
            : "varchar(250)";
        var isConjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;
        var isSoftDelete = mapping.DeleteStyle == DeleteStyle.SoftDelete;
        var softDeleteCols = isSoftDelete
            ? @"
                    is_deleted bit NOT NULL DEFAULT 0,
                    deleted_at datetimeoffset NULL,"
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
                        dotnet_type varchar(500) NULL,{softDeleteCols}
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
                    dotnet_type varchar(500) NULL,{softDeleteCols}
                    tenant_id varchar(250) NOT NULL DEFAULT '*DEFAULT*'
                );
            END";
    }
}
