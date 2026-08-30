using JasperFx.Descriptors;
using JasperFx.MultiTenancy;
using Polecat.Internal;

namespace Polecat.Storage;

/// <summary>
///     Abstracts tenant-to-database routing. Implementations determine whether
///     all tenants share one database or each gets a separate one.
/// </summary>
public interface ITenancy
{
    DatabaseCardinality Cardinality { get; }
    string DefaultTenantId { get; }
    ConnectionFactory GetConnectionFactory(string tenantId);
    PolecatDatabase GetDatabase(string tenantId);
    IReadOnlyList<PolecatDatabase> AllDatabases();

    /// <summary>
    ///     Asynchronously resolve every tenant database. Dynamic tenancies
    ///     (e.g. <see cref="MasterTableTenancy" />) query their control table
    ///     here; static tenancies just return <see cref="AllDatabases" />. Mirrors
    ///     Marten's <c>ITenancy.BuildDatabases()</c> and is used by
    ///     <c>PolecatSystemPart.FindResources()</c> so JasperFx's
    ///     <c>AddResourceSetupOnStartup()</c> can provision every tenant schema.
    /// </summary>
    Task<IReadOnlyList<PolecatDatabase>> BuildDatabasesAsync(CancellationToken token = default);

    /// <summary>
    ///     A connection string this tenancy can nominate for the store's own
    ///     <see cref="StoreOptions.ConnectionString" /> when the application did not set one.
    ///     Configuring a database-per-tenant tenancy already names every database the store will
    ///     ever touch, so requiring the application to ALSO nominate one of them as a top level
    ///     connection string is pure ceremony — and picking one arbitrarily (as users were doing)
    ///     makes that tenant's database quietly special. Returns null when the tenancy has nothing
    ///     to offer, which leaves the existing "a connection string must be configured" error in
    ///     place. polecat#514.
    ///     <para>
    ///     Default-implemented so existing <see cref="ITenancy" /> implementations outside this
    ///     repo keep compiling.
    ///     </para>
    /// </summary>
    string? SeedConnectionString => null;
}

/// <summary>
///     Default tenancy for single database and conjoined multi-tenancy.
///     All tenants share the same database and connection.
/// </summary>
internal class DefaultTenancy : ITenancy
{
    private readonly ConnectionFactory _factory;
    private readonly PolecatDatabase _database;

    public DefaultTenancy(ConnectionFactory factory, PolecatDatabase database)
    {
        _factory = factory;
        _database = database;
    }

    public DatabaseCardinality Cardinality => DatabaseCardinality.Single;
    public string DefaultTenantId => JasperFx.StorageConstants.DefaultTenantId;
    public ConnectionFactory GetConnectionFactory(string tenantId) => _factory;
    public PolecatDatabase GetDatabase(string tenantId) => _database;
    public IReadOnlyList<PolecatDatabase> AllDatabases() => [_database];

    public Task<IReadOnlyList<PolecatDatabase>> BuildDatabasesAsync(CancellationToken token = default) =>
        Task.FromResult(AllDatabases());

    // DefaultTenancy is only ever constructed FROM the store's connection string, so it has nothing
    // to seed back.
    public string? SeedConnectionString => null;
}

/// <summary>
///     Separate database tenancy — each tenant gets its own SQL Server database.
///     Statically configured via AddTenant() during store setup.
/// </summary>
public class SeparateDatabaseTenancy : ITenancy
{
    private readonly Dictionary<string, ConnectionFactory> _factories = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, PolecatDatabase> _databases = new(StringComparer.OrdinalIgnoreCase);
    private readonly StoreOptions _options;

    internal SeparateDatabaseTenancy(StoreOptions options)
    {
        _options = options;
    }

    DatabaseCardinality ITenancy.Cardinality => DatabaseCardinality.StaticMultiple;
    string ITenancy.DefaultTenantId => JasperFx.StorageConstants.DefaultTenantId;

    /// <summary>
    ///     Register a tenant with its connection string.
    /// </summary>
    public void AddTenant(string tenantId, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        _factories[tenantId] = new ConnectionFactory(connectionString);
    }

    ConnectionFactory ITenancy.GetConnectionFactory(string tenantId)
    {
        if (_factories.TryGetValue(tenantId, out var factory)) return factory;
        throw new UnknownTenantIdException(tenantId);
    }

    PolecatDatabase ITenancy.GetDatabase(string tenantId)
    {
        if (_databases.TryGetValue(tenantId, out var database)) return database;

        if (!_factories.TryGetValue(tenantId, out var factory))
            throw new UnknownTenantIdException(tenantId);

        database = new PolecatDatabase(_options, factory.ConnectionString, $"Polecat_{tenantId}");
        _databases[tenantId] = database;
        return database;
    }

    IReadOnlyList<PolecatDatabase> ITenancy.AllDatabases()
    {
        // Ensure all databases are materialized
        foreach (var tenantId in _factories.Keys)
        {
            ((ITenancy)this).GetDatabase(tenantId);
        }

        return _databases.Values.ToList();
    }

    Task<IReadOnlyList<PolecatDatabase>> ITenancy.BuildDatabasesAsync(CancellationToken token) =>
        Task.FromResult(((ITenancy)this).AllDatabases());

    // The first tenant registered, matching how Marten's StaticMultiTenancy nominates the first
    // AddSingleTenantDatabase call as its Default. It only backs schema modelling and the store's
    // own Database property — nothing routes to it, because DefaultTenantUsageEnabled is off.
    string? ITenancy.SeedConnectionString =>
        _factories.Values.FirstOrDefault()?.ConnectionString;
}
