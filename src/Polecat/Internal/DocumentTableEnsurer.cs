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

            // Use Weasel SchemaMigration to create or update the document table
            var table = new DocumentTable(provider.Mapping);
            var migration = await SchemaMigration.DetermineAsync(conn, token, table);
            await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: token);

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

    public async Task EnsureTablesAsync(IEnumerable<DocumentProvider> providers, CancellationToken token)
    {
        foreach (var provider in providers)
        {
            await EnsureTableAsync(provider, token);
        }
    }
}
