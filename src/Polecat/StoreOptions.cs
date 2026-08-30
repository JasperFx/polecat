using System.Text.Json;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Fetching;
using JasperFx.Events.Tags;
using Polly;
using Polecat.Events;
using Polecat.Internal;
using Polecat.Projections;
using Polecat.Resilience;
using Polecat.Schema.Identity.Sequences;
using Polecat.Serialization;
using Polecat.Internal.OpenTelemetry;
using Polecat.Logging;
using Polecat.Metadata;
using Polecat.Storage;
using Weasel.Core;

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
        Policies = new StorePolicies(this);
        EventGraph = new EventGraph(this);
        Events.EventGraph = EventGraph;
        Projections = new PolecatProjectionOptions(EventGraph);
        Projections.SetStoreOptions(this);
        ResiliencePipeline = new ResiliencePipelineBuilder().AddPolecatDefaults().Build();
    }

    /// <summary>
    ///     The event graph configuration and registry. Created at construction time
    ///     so projections can register event types during configuration.
    /// </summary>
    public EventGraph EventGraph { get; }

    /// <summary>
    ///     The connection string to the SQL Server database.
    /// </summary>
    public string ConnectionString
    {
        get => _connectionString;
        set => _connectionString = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>
    ///     Supply the connection string to the SQL Server database. Provided for API
    ///     parity with Marten's <c>StoreOptions.Connection(string)</c> — equivalent to
    ///     setting <see cref="ConnectionString"/> directly.
    /// </summary>
    public void Connection(string connectionString)
    {
        ConnectionString = connectionString;
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
    ///     A logical name for this store, used to build a distinct <see cref="JasperFx.Events.IEventStore" />
    ///     identity (and the store-usage descriptor) so multiple Polecat stores in one application are
    ///     distinguishable. Defaults to "Main"; ancillary stores registered via <c>AddPolecatStore&lt;T&gt;</c>
    ///     set this to the marker type name. Mirrors Marten's <c>StoreOptions.StoreName</c>.
    /// </summary>
    public string StoreName { get; set; } = "Main";

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
    ///     Apply host-level <see cref="JasperFxOptions" /> (registered via
    ///     <c>AddJasperFx</c>) to this store before the <see cref="DocumentStore" /> is
    ///     constructed. Called from both the primary (<c>AddPolecat</c>) and ancillary
    ///     (<c>AddPolecatStore&lt;T&gt;</c>) registration paths. Mirrors Marten's
    ///     <c>StoreOptions.ReadJasperFxOptions</c>.
    /// </summary>
    /// <summary>
    ///     #345: buffered copy of <see cref="JasperFxOptions.ApplicationAssemblyReuseWarning" /> (jasperfx#543 /
    ///     GH-3521). Non-null only when this host adopted an earlier host's process-pinned application
    ///     assembly that differs from its own registration assembly — the quiet, order-dependent failure
    ///     mode in a multi-host test process. <see cref="Internal.PolecatActivator" /> logs it once at
    ///     startup. JasperFx only detects the condition; consumers surface it.
    /// </summary>
    internal string? ApplicationAssemblyReuseWarning { get; set; }

    internal void ReadJasperFxOptions(JasperFxOptions? options)
    {
        if (options == null) return;

        // CritterWatch / advanced-tooling opt-in: when the JasperFx host turns on
        // EnableAdvancedTracking, every Polecat DocumentStore in the container
        // (primary + ancillary) opts into extended progression tracking so downstream
        // tools (CritterWatch in particular) see the richer per-shard state.
        if (options.EnableAdvancedTracking)
        {
            Events.EnableExtendedProgressionTracking = true;
        }

        // #345: buffer JasperFx's GH-3521 application-assembly-reuse warning so PolecatActivator can
        // log it once at startup. ??= so the first non-null value (primary or ancillary path) wins and
        // a later null never clobbers it.
        ApplicationAssemblyReuseWarning ??= options.ApplicationAssemblyReuseWarning;
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
    ///     Configure document schema mappings including sub-class hierarchies.
    /// </summary>
    public SchemaConfiguration Schema { get; } = new();

    /// <summary>
    ///     Document storage policies (e.g., soft deletes, tenant-partitioned documents).
    /// </summary>
    public StorePolicies Policies { get; }

    /// <summary>
    ///     Global session listeners applied to all sessions.
    /// </summary>
    public List<IDocumentSessionListener> Listeners { get; } = new();

    /// <summary>
    ///     Global store-agnostic post-commit listeners applied to all sessions — #485 /
    ///     jasperfx#679's <see cref="JasperFx.Events.Documents.IDocumentCommitListener" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         A separate collection from <see cref="Listeners" /> rather than a widening of it,
    ///         because neither type can implement the other. Polecat's
    ///         <see cref="IDocumentSessionListener" /> also declares <c>BeforeSaveChangesAsync</c>,
    ///         which the shared contract deliberately does not abstract, and every parameter of the
    ///         two <c>AfterCommitAsync</c> signatures differs
    ///         (<see cref="IDocumentSession" /> vs <c>IDocumentSessionOperations</c>,
    ///         <see cref="IChangeSet" /> vs <c>IDocumentChangeSet</c>). Inheritance cannot bridge
    ///         that, so the alternative to a second list is asking every consumer to write the same
    ///         adapter.
    ///     </para>
    ///     <para>
    ///         Registered here on <see cref="StoreOptions" /> rather than swept out of the DI
    ///         container. A DI sweep would only serve stores built by <c>AddPolecat</c>, and
    ///         <c>new DocumentStore(options)</c> is a first-class construction path — the document
    ///         compliance fixture itself uses it — so a container-only registration would leave the
    ///         contract unreachable for exactly the callers most likely to be embedding Polecat.
    ///         It would also be a new pattern for Polecat, which does not register
    ///         <c>IDocumentSessionFactory</c> in DI at all.
    ///     </para>
    /// </remarks>
    public List<JasperFx.Events.Documents.IDocumentCommitListener> CommitListeners { get; } = new();

    /// <summary>
    ///     The store-level logger for SQL command logging and session tracking.
    ///     Defaults to NullPolecatLogger (no-op).
    /// </summary>
    public IPolecatLogger Logger { get; set; } = NullPolecatLogger.Instance;

    /// <summary>
    ///     OpenTelemetry tracing and metrics configuration.
    /// </summary>
    public OpenTelemetryOptions OpenTelemetry { get; } = new();

    private JasperFx.Events.IDocumentSchemaResolver? _schema;

    /// <summary>
    ///     Resolves the database table name backing a document, projection, or event-store
    ///     table — qualified (<c>[schema].[table]</c>) or bare. The single cross-store
    ///     "where does this document live" surface (jasperfx#333) for schema inspection,
    ///     diagnostics, and projection-coordinator activity tags. (Named SchemaResolver
    ///     because <see cref="Schema"/> is already Polecat's SchemaConfiguration.)
    /// </summary>
    public JasperFx.Events.IDocumentSchemaResolver SchemaResolver
        => _schema ??= new Internal.PolecatDocumentSchemaResolver(this);

    /// <summary>
    ///     Collection of IInitialData instances that will be populated on startup
    ///     after schema migration completes.
    /// </summary>
    public InitialDataCollection InitialData { get; } = new();

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

    // #273: back-reference to the store's PolecatDatabase (set by DocumentStore construction,
    // same pattern as Providers above) so sessions can expose it through the shared
    // Weasel.Storage.IStorageSession.Database seam.
    internal Storage.PolecatDatabase? StorageDatabase { get; set; }

    /// <summary>
    ///     The Polly resilience pipeline used for all SQL execution.
    ///     Defaults to retry on transient SQL Server errors.
    /// </summary>
    internal ResiliencePipeline ResiliencePipeline { get; set; }

    /// <summary>
    ///     The tenancy strategy. Defaults to DefaultTenancy (single database).
    ///     Set via MultiTenantedDatabases() for separate database per tenant.
    /// </summary>
    public ITenancy? Tenancy { get; set; }

    /// <summary>
    ///     Option to enable or disable usage of the default tenant when using multi-tenanted
    ///     documents. Configuring a database-per-tenant tenancy —
    ///     <see cref="MultiTenantedDatabases" /> or <see cref="MultiTenantedMasterTable" /> — turns
    ///     this off automatically, so a session or daemon opened with no tenant fails loudly with a
    ///     <see cref="Exceptions.DefaultTenantUsageDisabledException" /> rather than silently
    ///     landing on whichever database happens to back
    ///     <see cref="ConnectionString" />. A default tenant is never required to configure
    ///     database-per-tenant multi-tenancy.
    ///     <para>
    ///     Marten carries this as <c>StoreOptions.Advanced.DefaultTenantUsageEnabled</c>; Polecat
    ///     has no <c>Advanced</c> sub-object and flattens it onto <see cref="StoreOptions" />, as
    ///     with the other <c>Advanced.*</c> members it mirrors. polecat#514.
    ///     </para>
    /// </summary>
    public bool DefaultTenantUsageEnabled { get; set; } = true;

    /// <summary>
    ///     Custom projection storage providers registered by extensions (e.g., EF Core).
    ///     Keyed by document type, returns a factory that creates IProjectionStorage instances.
    /// </summary>
    internal Dictionary<Type, Func<Internal.DocumentSessionBase, string, object>> CustomProjectionStorageProviders { get; } = new();

    /// <summary>
    ///     When true (the default), Polecat uses the SQL Server 2025 native <c>json</c>
    ///     data type for document bodies, event data, headers, and snapshots.
    ///     Set to false to fall back to <c>nvarchar(max)</c> for pre-2025 SQL Server instances.
    /// </summary>
    public bool UseNativeJsonType { get; set; } = true;

    /// <summary>
    ///     Resolved SQL column type for JSON storage based on <see cref="UseNativeJsonType"/>.
    /// </summary>
    internal string JsonColumnType => UseNativeJsonType ? "json" : "nvarchar(max)";

    /// <summary>
    ///     Additional SQL Server tables to be managed by this DocumentStore alongside
    ///     Polecat's own schema objects. Used by extensions like EF Core integration
    ///     to register entity tables for Weasel schema migration.
    /// </summary>
    public List<Weasel.Core.ISchemaObject> ExtendedSchemaObjects { get; } = new();

    /// <summary>
    ///     Replace the default Polly resilience pipeline with a custom one.
    /// </summary>
    public void ConfigurePolly(Action<ResiliencePipelineBuilder> configure)
    {
        var builder = new ResiliencePipelineBuilder();
        configure(builder);
        ResiliencePipeline = builder.Build();
    }

    /// <summary>
    ///     Extend the default Polly resilience pipeline with additional strategies.
    ///     The default transient retry is applied first, then your additions.
    /// </summary>
    public void ExtendPolly(Action<ResiliencePipelineBuilder> configure)
    {
        var builder = new ResiliencePipelineBuilder();
        builder.AddPolecatDefaults();
        configure(builder);
        ResiliencePipeline = builder.Build();
    }

    /// <summary>
    ///     Configure separate database multi-tenancy. Each tenant gets its own
    ///     SQL Server database with full schema isolation.
    /// </summary>
    public void MultiTenantedDatabases(Action<SeparateDatabaseTenancy> configure)
    {
        // Marten's MultiTenantedDatabases() does the same. Every tenant has its own database, so
        // there is no coherent database for the default tenant to mean — asking for one is a
        // configuration mistake, not a fallback.
        DefaultTenantUsageEnabled = false;

        var tenancy = new SeparateDatabaseTenancy(this);
        configure(tenancy);
        Tenancy = tenancy;
    }

    /// <summary>
    ///     Configure dynamic separate-database multi-tenancy backed by a master "control plane" table.
    ///     The master table on <paramref name="masterConnectionString" /> maps each tenant id to its
    ///     connection string, and tenants can be added/removed/enabled/disabled at runtime via the
    ///     returned <see cref="MasterTableTenancy" />. This is the Polecat equivalent of Marten's
    ///     <c>MultiTenantedDatabasesViaMasterTable</c>.
    /// </summary>
    /// <param name="masterConnectionString">Connection string to the control-plane database that holds the tenant registry.</param>
    /// <param name="schemaName">Schema for the master table; defaults to <see cref="DatabaseSchemaName" />.</param>
    /// <param name="configure">Optional hook to configure the tenancy before it is assigned.</param>
    public MasterTableTenancy MultiTenantedMasterTable(string masterConnectionString,
        string? schemaName = null, Action<MasterTableTenancy>? configure = null)
    {
        // Matches Marten's MultiTenantedDatabasesWithMasterDatabaseTable — see the note on
        // MultiTenantedDatabases above.
        DefaultTenantUsageEnabled = false;

        var tenancy = new MasterTableTenancy(this, masterConnectionString, schemaName ?? DatabaseSchemaName);
        configure?.Invoke(tenancy);
        Tenancy = tenancy;
        return tenancy;
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

    /// <summary>
    ///     Register a custom value type — a "strong typed identifier" wrapping a single <see cref="Guid" />,
    ///     <c>string</c>, <c>int</c> or <c>long</c>.
    ///     <para>
    ///     Polecat discovers these on its own, so calling this is never required. It exists because
    ///     Marten's <c>StoreOptions.RegisterValueType&lt;T&gt;()</c> does, and store-configuration source
    ///     shared across both — a single file compiled once per store — has to build against either
    ///     (polecat#459). Calling it here resolves the type eagerly and validates it, so a type that
    ///     cannot be a value wrapper fails at configuration time rather than at the first query.
    ///     </para>
    /// </summary>
    /// <typeparam name="TValueType">The wrapper type, e.g. <c>record struct OrderId(Guid Value)</c>.</typeparam>
    /// <returns>The resolved value type metadata, matching Marten's return type.</returns>
    /// <exception cref="JasperFx.Core.Reflection.InvalidValueTypeException">
    ///     <typeparamref name="TValueType" /> is not a usable value wrapper.
    /// </exception>
    public JasperFx.Core.Reflection.ValueTypeInfo RegisterValueType<TValueType>() where TValueType : notnull
        => ValueTypes.Register(typeof(TValueType));

    /// <summary>
    ///     Register a custom value type by <see cref="Type" />. See
    ///     <see cref="RegisterValueType{TValueType}" /> for why this exists.
    /// </summary>
    /// <exception cref="JasperFx.Core.Reflection.InvalidValueTypeException">
    ///     <paramref name="type" /> is not a usable value wrapper.
    /// </exception>
    public JasperFx.Core.Reflection.ValueTypeInfo RegisterValueType(Type type)
        => ValueTypes.Register(type);

    internal ConnectionFactory CreateConnectionFactory()
    {
        // #514: a configured tenancy already names every database the store will touch, so it can
        // seed the store's own connection string. Requiring the application to ALSO nominate one of
        // the tenant databases at the top level was ceremony that made that tenant's database
        // quietly special — and the store would throw on startup without it.
        if (string.IsNullOrWhiteSpace(_connectionString) && Tenancy?.SeedConnectionString is { } seeded)
        {
            _connectionString = seeded;
        }

        if (string.IsNullOrWhiteSpace(_connectionString))
        {
            throw new InvalidOperationException(
                "A connection string must be configured. Set StoreOptions.ConnectionString, or configure a tenancy (MultiTenantedDatabases / MultiTenantedMasterTable) that supplies one.");
        }

        return new ConnectionFactory(_connectionString);
    }
}

/// <summary>
///     Configuration specific to the event store.
/// </summary>
public class EventStoreOptions : IEventStoreInstrumentation
{
    internal EventGraph? EventGraph { get; set; }

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

    /// <summary>
    ///     #237: enable tracking of the user / last-modified-by metadata on events, persisted to the
    ///     opt-in <c>user_name</c> column (maps <see cref="JasperFx.Events.IEvent.UserName" />).
    ///     Mirrors Marten's opt-in <c>user_name</c> event-metadata column. Populated from the
    ///     session's <c>LastModifiedBy</c> when the event doesn't already carry one.
    /// </summary>
    public bool EnableUserName { get; set; }

    /// <summary>
    ///     Run inline projections' RaiseSideEffects (and other projection side effects) when an inline
    ///     projection is applied during SaveChangesAsync. Off by default; mirrors Marten's
    ///     <c>StoreOptions.Events.EnableSideEffectsOnInlineProjections</c>. CritterWatch relies on this so
    ///     its inline ServiceSummary projection can publish SignalR notifications via the Wolverine outbox.
    /// </summary>
    public bool EnableSideEffectsOnInlineProjections { get; set; }

    /// <summary>
    ///     Opt into extended columns on the event progression table for CritterWatch alerting.
    ///     Adds nullable heartbeat, agent_status, pause_reason, running_on_node,
    ///     warning_behind_threshold, critical_behind_threshold, and the #368 classified-failure columns
    ///     (failure_category, failure_event_sequence, failure_event_type, failure_event_tenant_id) that
    ///     carry <see cref="JasperFx.Events.Daemon.ShardFailure" /> onto the row. This is the
    ///     Polecat-named alias for <see cref="IEventStoreInstrumentation.ExtendedProgressionEnabled" />;
    ///     both read and write the same setting.
    /// </summary>
    public bool EnableExtendedProgressionTracking { get; set; }

    /// <summary>
    ///     <see cref="JasperFx.Events.IEventStoreInstrumentation" /> surface (jasperfx#424). The
    ///     storage-agnostic toggle CritterWatch uses to enable extended projection-daemon monitoring
    ///     without referencing Polecat types. Backed by <see cref="EnableExtendedProgressionTracking" />.
    /// </summary>
    bool IEventStoreInstrumentation.ExtendedProgressionEnabled
    {
        get => EnableExtendedProgressionTracking;
        set => EnableExtendedProgressionTracking = value;
    }

    /// <summary>
    ///     <see cref="IEventStoreInstrumentation.AppendObserver" /> (jasperfx 2.15.0). Optional observer
    ///     invoked, best-effort after each successful <c>SaveChanges</c> commit, with the events appended
    ///     in that unit of work — so storage-agnostic lifecycle tooling (CritterWatch#500) can record
    ///     runtime-observed "appends" edges. Each <see cref="IEvent" /> carries event type, stream
    ///     id/key, aggregate type, tenant id, and timestamp. Combine observers with <c>+=</c>.
    /// </summary>
    public Action<IReadOnlyList<IEvent>>? AppendObserver { get; set; }

    /// <summary>
    ///     Outbox factory the projection daemon asks for an
    ///     <see cref="Polecat.Events.Aggregation.IMessageBatch"/> when a
    ///     projection in the current batch publishes a side-effect message.
    ///     Defaults to <see cref="Polecat.Events.Aggregation.NulloMessageOutbox"/>
    ///     so apps that don't integrate a message bus pay zero overhead.
    ///     Wolverine.Polecat (and other downstream integrations) plug their
    ///     own implementation in here.
    /// </summary>
    public Polecat.Events.Aggregation.IMessageOutbox MessageOutbox { get; set; }
        = Polecat.Events.Aggregation.NulloMessageOutbox.Instance;

    /// <summary>
    ///     Pre-register an event type with the event store. Mirrors Marten's
    ///     <c>StoreOptions.Events.AddEventType&lt;TEvent&gt;()</c>. Not strictly necessary — event types are
    ///     registered on the fly as they are appended — but pre-registration can help with asynchronous
    ///     projections where the daemon process hasn't yet encountered the event type, and lets the
    ///     event type name alias be resolved before the first append.
    /// </summary>
    public void AddEventType<TEvent>() where TEvent : notnull
    {
        EventGraph!.AddEventType(typeof(TEvent));
    }

    /// <summary>
    ///     Pre-register an event type with the event store. Mirrors Marten's
    ///     <c>StoreOptions.Events.AddEventType(Type)</c>.
    /// </summary>
    public void AddEventType(Type eventType)
    {
        EventGraph!.AddEventType(eventType);
    }

    /// <summary>
    ///     Pre-register several event types with the event store in one call. Mirrors Marten's
    ///     <c>StoreOptions.Events.AddEventTypes(IEnumerable&lt;Type&gt;)</c>.
    /// </summary>
    public void AddEventTypes(IEnumerable<Type> eventTypes)
    {
        foreach (var eventType in eventTypes) EventGraph!.AddEventType(eventType);
    }

    /// <summary>
    ///     #424: register a policy for how to remove or mask protected information for an event type
    ///     <typeparamref name="T" /> or any event type assignable to it, mutating the event data in
    ///     place. Rules are applied by <c>IDocumentStore.Advanced.ApplyEventDataMasking(...)</c>.
    ///     Mirrors Marten's <c>opts.Events.AddMaskingRuleForProtectedInformation&lt;T&gt;(...)</c> —
    ///     the rule was previously only reachable through the <c>StoreOptions.EventGraph</c> escape
    ///     hatch, which is not how the rest of the store is configured.
    /// </summary>
    public void AddMaskingRuleForProtectedInformation<T>(Action<T> action) where T : notnull
    {
        EventGraph!.AddMaskingRuleForProtectedInformation(action);
    }

    /// <summary>
    ///     #424: register a policy for how to remove or mask protected information for an event type
    ///     <typeparamref name="T" /> or any event type assignable to it, replacing the event data
    ///     with a new instance — the overload records and other immutable event types need. Mirrors
    ///     Marten's <c>opts.Events.AddMaskingRuleForProtectedInformation&lt;T&gt;(...)</c>.
    /// </summary>
    public void AddMaskingRuleForProtectedInformation<T>(Func<T, T> func) where T : notnull
    {
        EventGraph!.AddMaskingRuleForProtectedInformation(func);
    }

    /// <summary>
    ///     #478 / jasperfx#674: the second-level cache of aggregate snapshots behind
    ///     <c>FetchForWriting</c>. Off for every aggregate type by default.
    /// </summary>
    /// <remarks>
    ///     Reach for <see cref="CacheAggregatesForWriting{T}" /> to enroll a type; this is here for
    ///     the rest of the surface — <c>SizeLimit</c>, and <c>Cache</c> for supplying your own
    ///     implementation instead of the bounded node-local default.
    /// </remarks>
    public AggregateWriteCacheOptions AggregateWriteCaching => EventGraph!.AggregateWriteCaching;

    /// <summary>
    ///     #478 / jasperfx#674: cache <typeparamref name="T" />'s snapshot across
    ///     <c>FetchForWriting</c> calls, so a fetch folds only the events committed since the cached
    ///     baseline instead of re-reading the stream from the beginning.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Per aggregate type rather than store-wide, because the win is proportional to how
    ///         often one stream is fetched for writing — real on a hot aggregate under high message
    ///         volume, and only overhead on an aggregate written once.
    ///     </para>
    ///     <para>
    ///         The cached snapshot is a <b>baseline</b> and nothing more: the stream version and
    ///         every event after the baseline are still read on every call, and optimistic
    ///         concurrency is untouched. Turning this on is unobservable except in latency.
    ///     </para>
    /// </remarks>
    public EventStoreOptions CacheAggregatesForWriting<T>(int sizeLimit = 1000) where T : class
    {
        EventGraph!.CacheAggregatesForWriting<T>(sizeLimit);
        return this;
    }

    /// <summary>
    ///     #388 / #475: store-wide fallback <see cref="JasperFx.Events.IEventBinarySerializer" /> for
    ///     event types marked with <see cref="JasperFx.Events.BinaryEventAttribute" /> that have no
    ///     explicit per-type registration via <see cref="UseBinarySerializer{TEvent}" />. Null by
    ///     default — every event type stays on the JSON path.
    /// </summary>
    /// <remarks>
    ///     ⚠️ #475 widened this from Polecat's own interface to the shared one JasperFx 2.50.0 promoted
    ///     out of Marten, and <b>removed</b> <c>Polecat.Events.IEventBinarySerializer</c> and
    ///     <c>Polecat.Events.BinaryEventAttribute</c> rather than keeping them as deriving aliases: a
    ///     consumer with both <c>JasperFx.Events</c> and <c>Polecat.Events</c> in scope got CS0104 on
    ///     the bare name, which is the exact shape the promotion exists to serve. A serializer written
    ///     against #388 needs its interface reference re-pointed at <c>JasperFx.Events</c> — a compile
    ///     error, never a silent behavior change. In return a consumer compiling one body of source
    ///     against Marten, Polecat and Fisher writes one implementation instead of three identical ones.
    /// </remarks>
    public JasperFx.Events.IEventBinarySerializer? DefaultBinarySerializer
    {
        get => EventGraph!.DefaultBinarySerializer;
        set => EventGraph!.DefaultBinarySerializer = value;
    }

    /// <summary>
    ///     #388 / #475: opt <typeparamref name="TEvent" /> into binary serialization — its payload is
    ///     written to the <c>pc_events.bdata</c> column instead of <c>data</c>, and read back through
    ///     the same serializer. Per event type rather than store-wide, so JSON and binary rows coexist
    ///     in one table and the feature can be switched on (or back off) for an existing store with no
    ///     data migration.
    /// </summary>
    /// <inheritdoc cref="DefaultBinarySerializer" path="/remarks" />
    public EventStoreOptions UseBinarySerializer<TEvent>(JasperFx.Events.IEventBinarySerializer serializer)
        where TEvent : notnull
    {
        EventGraph!.UseBinarySerializer<TEvent>(serializer);
        return this;
    }

    /// <summary>
    ///     Register a tag type for Dynamic Consistency Boundary (DCB) support.
    ///     Creates a tag table with an auto-generated suffix.
    /// </summary>
    public ITagTypeRegistration RegisterTagType<TTag>() where TTag : notnull
    {
        return EventGraph!.RegisterTagType<TTag>();
    }

    /// <summary>
    ///     Register a tag type with an explicit table suffix for DCB support.
    /// </summary>
    public ITagTypeRegistration RegisterTagType<TTag>(string tableSuffix) where TTag : notnull
    {
        return EventGraph!.RegisterTagType<TTag>(tableSuffix);
    }
}
