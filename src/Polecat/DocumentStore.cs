using JasperFx.Events.Projections;
using Polecat.Events;
using Polecat.Internal;
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
        Database = new PolecatDatabase(options);

        // Initialize default tenancy if not already configured
        Options.Tenancy ??= new DefaultTenancy(_connectionFactory, Database);

        // Initialize projection graph — builds async shard registry
        options.Projections.AssertValidity(options);

        _inlineProjections = new Lazy<IInlineProjection<IDocumentSession>[]>(
            () => options.Projections.BuildInlineProjections());
    }

    public StoreOptions Options { get; }

    /// <summary>
    ///     The Weasel database for schema management (default tenant).
    /// </summary>
    public PolecatDatabase Database { get; }

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
        return new DocumentTableEnsurer(factory, Options);
    }

    public IDocumentSession LightweightSession()
    {
        return LightweightSession(new SessionOptions());
    }

    public IDocumentSession LightweightSession(SessionOptions options)
    {
        var factory = ResolveConnectionFactory(options.TenantId);
        var ensurer = ReferenceEquals(factory, _connectionFactory) ? _tableEnsurer : new DocumentTableEnsurer(factory, Options);
        return new LightweightSession(
            Options,
            factory,
            _providers,
            ensurer,
            Events,
            InlineProjections,
            options.TenantId);
    }

    public IDocumentSession IdentitySession()
    {
        return IdentitySession(new SessionOptions());
    }

    public IDocumentSession IdentitySession(SessionOptions options)
    {
        var factory = ResolveConnectionFactory(options.TenantId);
        var ensurer = ReferenceEquals(factory, _connectionFactory) ? _tableEnsurer : new DocumentTableEnsurer(factory, Options);
        return new IdentityMapDocumentSession(
            Options,
            factory,
            _providers,
            ensurer,
            Events,
            InlineProjections,
            options.TenantId);
    }

    public IQuerySession QuerySession()
    {
        return QuerySession(new SessionOptions());
    }

    public IQuerySession QuerySession(SessionOptions options)
    {
        var factory = ResolveConnectionFactory(options.TenantId);
        var ensurer = ReferenceEquals(factory, _connectionFactory) ? _tableEnsurer : new DocumentTableEnsurer(factory, Options);
        return new Internal.QuerySession(
            Options,
            factory,
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
