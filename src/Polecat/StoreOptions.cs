using System.Text.Json;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using Polecat.Events;
using Polecat.Internal;
using Polecat.Projections;
using Polecat.Schema.Identity.Sequences;
using Polecat.Serialization;
using Polecat.Logging;
using Polecat.Metadata;
using Polecat.Storage;

namespace Polecat;

/// <summary>
///     Configuration options for a Polecat DocumentStore.
/// </summary>
public class StoreOptions
{
    public const int DefaultTimeout = 30;

    private string _connectionString = string.Empty;
    private string _databaseSchemaName = "dbo";
    private ISerializer? _serializer;
    private AutoCreate? _autoCreate;

    public StoreOptions()
    {
        EventGraph = new EventGraph(this);
        Projections = new PolecatProjectionOptions(EventGraph);
        Projections.SetStoreOptions(this);
    }

    /// <summary>
    ///     The event graph configuration and registry. Created at construction time
    ///     so projections can register event types during configuration.
    /// </summary>
    internal EventGraph EventGraph { get; }

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
    public PolecatProjectionOptions Projections { get; }

    /// <summary>
    ///     Settings for the async projection daemon.
    /// </summary>
    public DaemonSettings DaemonSettings { get; } = new();

    /// <summary>
    ///     Global default settings for HiLo sequence identity generation.
    ///     Applied to all numeric-id document types unless overridden by [HiloSequence] attribute.
    /// </summary>
    public HiloSettings HiloSequenceDefaults { get; } = new();

    /// <summary>
    ///     Document storage policies (e.g., soft deletes).
    /// </summary>
    public StorePolicies Policies { get; } = new();

    /// <summary>
    ///     Global session listeners applied to all sessions.
    /// </summary>
    public List<IDocumentSessionListener> Listeners { get; } = new();

    /// <summary>
    ///     The store-level logger for SQL command logging and session tracking.
    ///     Defaults to NullPolecatLogger (no-op).
    /// </summary>
    public IPolecatLogger Logger { get; set; } = NullPolecatLogger.Instance;

    /// <summary>
    ///     Get or set the serializer. Defaults to PolecatSerializer (System.Text.Json).
    /// </summary>
    public ISerializer Serializer
    {
        get => _serializer ??= new Serializer();
        set => _serializer = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>
    ///     Set by ApplyAllDatabaseChangesOnStartup(). Used by the hosted service.
    /// </summary>
    internal bool ShouldApplyChangesOnStartup { get; set; }

    /// <summary>
    ///     Internal access to the document provider registry. Set by DocumentStore during construction.
    /// </summary>
    internal DocumentProviderRegistry Providers { get; set; } = null!;

    /// <summary>
    ///     The tenancy strategy. Defaults to DefaultTenancy (single database).
    ///     Set via MultiTenantedDatabases() for separate database per tenant.
    /// </summary>
    internal ITenancy? Tenancy { get; set; }

    /// <summary>
    ///     Configure separate database multi-tenancy. Each tenant gets its own
    ///     SQL Server database with full schema isolation.
    /// </summary>
    public void MultiTenantedDatabases(Action<SeparateDatabaseTenancy> configure)
    {
        var tenancy = new SeparateDatabaseTenancy(this);
        configure(tenancy);
        Tenancy = tenancy;
    }

    /// <summary>
    ///     Configure the serialization settings for the document store.
    /// </summary>
    public void ConfigureSerialization(
        EnumStorage enumStorage = EnumStorage.AsInteger,
        Casing casing = Casing.CamelCase,
        CollectionStorage collectionStorage = CollectionStorage.Default,
        NonPublicMembersStorage nonPublicMembersStorage = NonPublicMembersStorage.Default,
        Action<JsonSerializerOptions>? configure = null)
    {
        var serializer = new Serializer();
        serializer.Casing = casing;
        serializer.EnumStorage = enumStorage;
        serializer.CollectionStorage = collectionStorage;
        serializer.NonPublicMembersStorage = nonPublicMembersStorage;
        if (configure != null) serializer.Configure(configure);
        Serializer = serializer;
    }

    /// <summary>
    ///     Configure the serialization settings with a custom base JsonSerializerOptions.
    /// </summary>
    public void ConfigureSerialization(
        JsonSerializerOptions options,
        EnumStorage enumStorage = EnumStorage.AsInteger,
        Casing casing = Casing.CamelCase,
        CollectionStorage collectionStorage = CollectionStorage.Default,
        NonPublicMembersStorage nonPublicMembersStorage = NonPublicMembersStorage.Default,
        Action<JsonSerializerOptions>? configure = null)
    {
        var serializer = new Serializer(options);
        serializer.Casing = casing;
        serializer.EnumStorage = enumStorage;
        serializer.CollectionStorage = collectionStorage;
        serializer.NonPublicMembersStorage = nonPublicMembersStorage;
        if (configure != null) serializer.Configure(configure);
        Serializer = serializer;
    }

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
