using JasperFx.Descriptors;
using Polecat.Internal;

namespace Polecat.Storage;

/// <summary>
///     Abstracts tenant-to-database routing. Implementations determine whether
///     all tenants share one database or each gets a separate one.
/// </summary>
internal interface ITenancy
{
    DatabaseCardinality Cardinality { get; }
    string DefaultTenantId { get; }
    ConnectionFactory GetConnectionFactory(string tenantId);
    PolecatDatabase GetDatabase(string tenantId);
    IReadOnlyList<PolecatDatabase> AllDatabases();
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
    public string DefaultTenantId => Tenancy.DefaultTenantId;
    public ConnectionFactory GetConnectionFactory(string tenantId) => _factory;
    public PolecatDatabase GetDatabase(string tenantId) => _database;
    public IReadOnlyList<PolecatDatabase> AllDatabases() => [_database];
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
    string ITenancy.DefaultTenantId => Tenancy.DefaultTenantId;

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
        throw new Exceptions.UnknownTenantException(tenantId);
    }

    PolecatDatabase ITenancy.GetDatabase(string tenantId)
    {
        if (_databases.TryGetValue(tenantId, out var database)) return database;

        if (!_factories.TryGetValue(tenantId, out var factory))
            throw new Exceptions.UnknownTenantException(tenantId);

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
}
