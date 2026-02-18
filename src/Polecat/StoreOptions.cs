using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using Polecat.Internal;
using Polecat.Projections;
using Polecat.Serialization;

namespace Polecat;

/// <summary>
///     Configuration options for a Polecat DocumentStore.
/// </summary>
public class StoreOptions
{
    public const int DefaultTimeout = 30;

    private string _connectionString = string.Empty;
    private string _databaseSchemaName = "dbo";
    private IPolecatSerializer? _serializer;
    private AutoCreate? _autoCreate;

    public StoreOptions()
    {
    }

    /// <summary>
    ///     The connection string to the SQL Server database.
    /// </summary>
    public string ConnectionString
    {
        get => _connectionString;
        set => _connectionString = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>
    ///     The default database schema name. Defaults to "dbo".
    /// </summary>
    public string DatabaseSchemaName
    {
        get => _databaseSchemaName;
        set => _databaseSchemaName = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>
    ///     Whether Polecat should attempt to create or update database schema objects at runtime.
    ///     Defaults to CreateOrUpdate for development convenience.
    /// </summary>
    public AutoCreate AutoCreateSchemaObjects
    {
        get => _autoCreate ?? AutoCreate.CreateOrUpdate;
        set => _autoCreate = value;
    }

    /// <summary>
    ///     Default command timeout in seconds.
    /// </summary>
    public int CommandTimeout { get; set; } = DefaultTimeout;

    /// <summary>
    ///     Configure the event store options.
    /// </summary>
    public EventStoreOptions Events { get; } = new();

    /// <summary>
    ///     Configure projections for the event store.
    /// </summary>
    public PolecatProjectionOptions Projections { get; } = new();

    /// <summary>
    ///     Settings for the async projection daemon.
    /// </summary>
    public DaemonSettings DaemonSettings { get; } = new();

    /// <summary>
    ///     Get or set the serializer. Defaults to PolecatSerializer (System.Text.Json).
    /// </summary>
    public IPolecatSerializer Serializer
    {
        get => _serializer ??= new PolecatSerializer();
        set => _serializer = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>
    ///     Set by ApplyAllDatabaseChangesOnStartup(). Used by the hosted service.
    /// </summary>
    internal bool ShouldApplyChangesOnStartup { get; set; }

    internal ConnectionFactory CreateConnectionFactory()
    {
        if (string.IsNullOrWhiteSpace(_connectionString))
        {
            throw new InvalidOperationException(
                "A connection string must be configured. Set StoreOptions.ConnectionString.");
        }

        return new ConnectionFactory(_connectionString);
    }
}

/// <summary>
///     Configuration specific to the event store.
/// </summary>
public class EventStoreOptions
{
    /// <summary>
    ///     Controls whether streams are identified by Guid or string.
    ///     Defaults to AsGuid.
    /// </summary>
    public StreamIdentity StreamIdentity { get; set; } = StreamIdentity.AsGuid;

    /// <summary>
    ///     Controls the tenancy style for the event store.
    ///     Defaults to Single (no multi-tenancy).
    /// </summary>
    public TenancyStyle TenancyStyle { get; set; } = TenancyStyle.Single;

    /// <summary>
    ///     Override the database schema name for event store tables.
    ///     If null, uses the StoreOptions.DatabaseSchemaName.
    /// </summary>
    public string? DatabaseSchemaName { get; set; }

    /// <summary>
    ///     Enable tracking of correlation id metadata on events.
    /// </summary>
    public bool EnableCorrelationId { get; set; }

    /// <summary>
    ///     Enable tracking of causation id metadata on events.
    /// </summary>
    public bool EnableCausationId { get; set; }

    /// <summary>
    ///     Enable tracking of custom headers metadata on events.
    /// </summary>
    public bool EnableHeaders { get; set; }
}

/// <summary>
///     Controls how event store tables handle multi-tenancy.
/// </summary>
public enum TenancyStyle
{
    /// <summary>
    ///     Single tenant, no tenant_id filtering.
    /// </summary>
    Single,

    /// <summary>
    ///     All tenants share the same tables with tenant_id column discrimination.
    /// </summary>
    Conjoined
}
