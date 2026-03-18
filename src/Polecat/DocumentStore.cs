using JasperFx.Events.Projections;
using Polecat.Events;
using Polecat.Internal;
using Polecat.Internal.Sessions;
using Polecat.Schema.Identity.Sequences;
using Polecat.Storage;

namespace Polecat;

/// <summary>
///     The main entry point for Polecat. Creates and manages document sessions.
///     Typically created via DocumentStore.For() and registered as a singleton.
/// </summary>
public partial class DocumentStore : IDocumentStore
{
    private readonly ConnectionFactory _connectionFactory;
    private readonly DocumentProviderRegistry _providers;
    private readonly DocumentTableEnsurer _tableEnsurer;
    private Lazy<IInlineProjection<IDocumentSession>[]> _inlineProjections;

    public DocumentStore(StoreOptions options)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));
        _connectionFactory = options.CreateConnectionFactory();
        _providers = new DocumentProviderRegistry(options);
        _tableEnsurer = new DocumentTableEnsurer(_connectionFactory, options);
        _tableEnsurer.SetProviderRegistry(_providers);
        Database = new PolecatDatabase(options);

        // Wire up sequence factory for HiLo ID generation
        Sequences = new SequenceFactory(options, _connectionFactory);
        _providers.SetSequenceFactory(Sequences);
        options.Providers = _providers;

        // Initialize default tenancy if not already configured
        Options.Tenancy ??= new DefaultTenancy(_connectionFactory, Database);

        // Auto-discover aggregate types with source-generated evolvers
        options.Projections.DiscoverGeneratedEvolvers(AppDomain.CurrentDomain.GetAssemblies());

        // Initialize projection graph — builds async shard registry
        options.Projections.AssertValidity(options);

        _inlineProjections = new Lazy<IInlineProjection<IDocumentSession>[]>(
            () => options.Projections.BuildInlineProjections());

        Advanced = new AdvancedOperations(this);
    }

    public StoreOptions Options { get; }
    public AdvancedOperations Advanced { get; }

    /// <summary>
    ///     The Weasel database for schema management (default tenant).
    /// </summary>
    public PolecatDatabase Database { get; }

    /// <summary>
    ///     The sequence factory for HiLo ID generation.
    /// </summary>
    internal SequenceFactory Sequences { get; }

    /// <summary>
    ///     The event graph configuration and registry.
    /// </summary>
    internal EventGraph Events => Database.Events;

    internal IInlineProjection<IDocumentSession>[] InlineProjections => _inlineProjections.Value;

    internal DocumentProvider GetProvider(Type documentType) => _providers.GetProvider(documentType);

    /// <summary>
    ///     Create a DocumentStore with inline configuration.
    /// </summary>
    public static DocumentStore For(Action<StoreOptions> configure)
    {
        var options = new StoreOptions();
        configure(options);
        return new DocumentStore(options);
    }

    /// <summary>
    ///     Create a DocumentStore with just a connection string.
    /// </summary>
    public static DocumentStore For(string connectionString)
    {
        return For(opts => opts.ConnectionString = connectionString);
    }

    private ConnectionFactory ResolveConnectionFactory(string tenantId)
    {
        return Options.Tenancy!.GetConnectionFactory(tenantId);
    }

    private DocumentTableEnsurer ResolveTableEnsurer(string tenantId)
    {
        var factory = ResolveConnectionFactory(tenantId);
        // For default tenancy, the factory is the same so we reuse the shared ensurer
        if (ReferenceEquals(factory, _connectionFactory)) return _tableEnsurer;
        var ensurer = new DocumentTableEnsurer(factory, Options);
        ensurer.SetProviderRegistry(_providers);
        return ensurer;
    }

    public IDocumentSession LightweightSession()
    {
        return LightweightSession(new SessionOptions());
    }

    public IDocumentSession LightweightSession(SessionOptions options)
    {
        var factory = ResolveConnectionFactory(options.TenantId);
        var ensurer = ResolveTableEnsurer(options.TenantId);
        var timeout = options.Timeout ?? Options.CommandTimeout;
        var lifetime = new TransactionalConnection(factory, timeout);
        return new LightweightSession(
            Options,
            lifetime,
            _providers,
            ensurer,
            Events,
            InlineProjections,
            options.TenantId,
            options.Listeners);
    }

    public IDocumentSession IdentitySession()
    {
        return IdentitySession(new SessionOptions());
    }

    public IDocumentSession IdentitySession(SessionOptions options)
    {
        var factory = ResolveConnectionFactory(options.TenantId);
        var ensurer = ResolveTableEnsurer(options.TenantId);
        var timeout = options.Timeout ?? Options.CommandTimeout;
        var lifetime = new TransactionalConnection(factory, timeout);
        return new IdentityMapDocumentSession(
            Options,
            lifetime,
            _providers,
            ensurer,
            Events,
            InlineProjections,
            options.TenantId,
            options.Listeners);
    }

    public IQuerySession QuerySession()
    {
        return QuerySession(new SessionOptions());
    }

    public IQuerySession QuerySession(SessionOptions options)
    {
        var factory = ResolveConnectionFactory(options.TenantId);
        var ensurer = ResolveTableEnsurer(options.TenantId);
        var timeout = options.Timeout ?? Options.CommandTimeout;
        var lifetime = new AutoClosingLifetime(factory, timeout);
        return new Internal.QuerySession(
            Options,
            lifetime,
            _providers,
            ensurer,
            Events,
            options.TenantId);
    }

    public IDocumentSession OpenSession(SessionOptions options)
    {
        return options.Tracking switch
        {
            DocumentTracking.IdentityOnly => IdentitySession(options),
            _ => LightweightSession(options)
        };
    }

    public async Task<IDocumentSession> OpenSessionAsync(SessionOptions options, CancellationToken token = default)
    {
        var factory = ResolveConnectionFactory(options.TenantId);
        var ensurer = ResolveTableEnsurer(options.TenantId);
        var timeout = options.Timeout ?? Options.CommandTimeout;
        var lifetime = new TransactionalConnection(factory, timeout);

        if (options.IsolationLevel != System.Data.IsolationLevel.ReadCommitted)
        {
            await lifetime.BeginTransactionAsync(options.IsolationLevel, token);
        }

        return options.Tracking switch
        {
            DocumentTracking.IdentityOnly => new IdentityMapDocumentSession(
                Options, lifetime, _providers, ensurer, Events, InlineProjections, options.TenantId, options.Listeners),
            _ => new LightweightSession(
                Options, lifetime, _providers, ensurer, Events, InlineProjections, options.TenantId, options.Listeners)
        };
    }

    public void Dispose()
    {
        // Nothing to dispose at the store level currently
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
