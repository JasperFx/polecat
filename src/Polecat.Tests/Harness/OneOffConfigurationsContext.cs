using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Storage;

namespace Polecat.Tests.Harness;

/// <summary>
///     Base class for tests that need completely custom store configuration per test class.
///     Each test class gets its own schema (named after the class) for full isolation.
///     Does NOT share a fixture with other test classes.
/// </summary>
public abstract class OneOffConfigurationsContext : IAsyncLifetime
{
    private readonly string _schemaName;
    private PolecatDatabase? _database;
    private DocumentStore? _store;
    protected readonly List<IDisposable> Disposables = new();
    protected readonly List<IAsyncDisposable> AsyncDisposables = new();

    protected OneOffConfigurationsContext()
    {
        _schemaName = GetType().Name.ToLowerInvariant();
    }

    protected PolecatDatabase theDatabase
    {
        get
        {
            if (_database == null)
            {
                ConfigureStore(_ => { });
            }

            return _database!;
        }
    }

    protected DocumentStore theStore
    {
        get
        {
            if (_store == null)
            {
                ConfigureStore(_ => { });
            }

            return _store!;
        }
    }

    /// <summary>
    ///     Configure a custom store for this test class. Drops existing schema first.
    /// </summary>
    protected void ConfigureStore(Action<StoreOptions> configure)
    {
        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            AutoCreateSchemaObjects = AutoCreate.All,
            DatabaseSchemaName = _schemaName
        };

        configure(options);

        _store = new DocumentStore(options);
        _database = _store.Database;
    }

    /// <summary>
    ///     Creates a new SqlConnection to the test database.
    /// </summary>
    protected async Task<SqlConnection> OpenConnectionAsync()
    {
        var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        Disposables.Add(conn);
        return conn;
    }

    public virtual async Task InitializeAsync()
    {
        // Drop the schema if it exists for a clean slate
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF SCHEMA_ID('{_schemaName}') IS NOT NULL
            BEGIN
                DECLARE @sql NVARCHAR(MAX) = '';
                SELECT @sql += 'DROP TABLE IF EXISTS ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';' + CHAR(13)
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{_schemaName}';
                EXEC sp_executesql @sql;
            END
            """;
        await cmd.ExecuteNonQueryAsync();

        if (_database != null)
        {
            await _database.ApplyAllConfiguredChangesToDatabaseAsync();
        }
    }

    public virtual async Task DisposeAsync()
    {
        foreach (var disposable in AsyncDisposables)
        {
            await disposable.DisposeAsync();
        }

        foreach (var disposable in Disposables)
        {
            disposable.Dispose();
        }

        _store?.Dispose();
    }
}
