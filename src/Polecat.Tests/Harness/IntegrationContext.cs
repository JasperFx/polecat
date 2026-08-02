using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Storage;

namespace Polecat.Tests.Harness;

/// <summary>
///     Base class for Polecat integration tests. Mirrors Marten's IntegrationContext pattern.
///     Provides access to the shared database, store, and a lightweight session.
/// </summary>
[Collection("integration")]
public abstract class IntegrationContext : IAsyncLifetime
{
    private readonly DefaultStoreFixture _fixture;
    private PolecatDatabase? _database;
    private DocumentStore? _customStore;
    private IDocumentSession? _session;
    protected readonly List<IDisposable> Disposables = new();
    protected readonly List<IAsyncDisposable> AsyncDisposables = new();

    protected IntegrationContext(DefaultStoreFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>
    ///     The shared database instance (or a custom one if StoreOptions was called).
    /// </summary>
    protected PolecatDatabase theDatabase => _database ?? _fixture.Database;

    /// <summary>
    ///     The DocumentStore for this test.
    /// </summary>
    protected DocumentStore theStore => _customStore ?? _fixture.Store;

    /// <summary>
    ///     A lightweight document session. Created on first access.
    /// </summary>
    protected IDocumentSession theSession
    {
        get
        {
            if (_session == null)
            {
                _session = theStore.LightweightSession();
                AsyncDisposables.Add(_session);
            }

            return _session;
        }
    }

    /// <summary>
    ///     Creates a custom DocumentStore for this test with unique configuration.
    ///     The schema name defaults to the test class name for isolation.
    /// </summary>
    /// <param name="configure">Applies the test's configuration to the new store's options.</param>
    /// <param name="cleanAll">
    ///     When true (the default), every document table in the configured schema is emptied after the
    ///     schema is applied, so the test starts from a known-empty store. The suite isolates by
    ///     <see cref="Polecat.StoreOptions.DatabaseSchemaName" /> inside one shared <c>master</c> database
    ///     rather than by database, so without this a test's rows survive into the next local run and any
    ///     assertion on an absolute count drifts upward. CI never sees that because it provisions a fresh
    ///     server per run. Pass <c>false</c> only when a test deliberately reconfigures the store mid-test
    ///     and needs the data written by the previous configuration to survive.
    /// </param>
    protected async Task<string> StoreOptions(Action<StoreOptions> configure, bool cleanAll = true)
    {
        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            AutoCreateSchemaObjects = AutoCreate.All,
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        };

        configure(options);

        _customStore = new DocumentStore(options);
        _database = _customStore.Database;
        await _database.ApplyAllConfiguredChangesToDatabaseAsync();

        if (cleanAll)
        {
            await _customStore.Advanced.Clean.DeleteAllDocumentsAsync();
        }

        // Reset session so subsequent access uses the new store
        _session = null;

        return options.DatabaseSchemaName;
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

    public virtual ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public virtual async ValueTask DisposeAsync()
    {
        foreach (var disposable in AsyncDisposables)
        {
            await disposable.DisposeAsync();
        }

        foreach (var disposable in Disposables)
        {
            disposable.Dispose();
        }

        _customStore?.Dispose();
    }
}
