using JasperFx.Core;
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
    ///     Describe every database backing this store for the store-agnostic
    ///     <see cref="DatabaseUsage" /> descriptor that <c>IEventStore.TryCreateUsage()</c> publishes.
    ///     Mirrors Marten's <c>ITenancy.DescribeDatabasesAsync()</c>.
    /// </summary>
    /// <remarks>
    ///     polecat#675. The descriptor used to be hand-built in
    ///     <c>DocumentStore.TryCreateUsage()</c> with <c>Cardinality = Single</c> and an empty
    ///     <see cref="DatabaseUsage.Databases" /> hard-coded, so a database-per-tenant store
    ///     described itself as single-database while <c>AllDatabases()</c> — reading this same
    ///     tenancy — returned every tenant. That disagreement is not cosmetic: Wolverine's
    ///     <c>EventStoreAgents.SupportedAgentsAsync</c> enumerates event-subscription agents from
    ///     <see cref="DatabaseUsage.Databases" />, so every tenant database's async projections
    ///     collapsed onto the main database and nothing scheduled them. Making the tenancy — the one
    ///     component that knows the answer — own the description is what keeps the two from drifting
    ///     apart again.
    ///     <para>
    ///     Default-implemented off <see cref="Cardinality" /> and <see cref="AllDatabases" /> so
    ///     <see cref="ITenancy" /> implementations outside this repo keep compiling; the built-in
    ///     tenancies override it to fill in <c>DatabaseDescriptor.TenantIds</c>, which only the
    ///     implementation knows.
    ///     </para>
    /// </remarks>
    async ValueTask<DatabaseUsage> DescribeDatabasesAsync(CancellationToken token = default)
    {
        var databases = AllDatabases();

        if (Cardinality == DatabaseCardinality.Single)
        {
            // A tenancy that calls itself Single but does not return exactly one database routes
            // through GetDatabase(DefaultTenantId) instead of taking whatever came first. Carried over
            // verbatim from PolecatDatabaseSource, which made this decision deliberately before #675
            // moved the description here: on that shape "the first of several" is a guess, and the
            // default tenant's database is the answer the tenancy itself would give.
            var main = databases.Count == 1
                ? databases[0]
                : GetDatabase(DefaultTenantId);

            return new DatabaseUsage
            {
                Cardinality = DatabaseCardinality.Single,
                // #703: DescribeAsync, not Describe. A conjoined store that sequences events per
                // tenant IS single-database, so its tenants can only ever be reported here — and a
                // Wolverine-managed host reads them off this descriptor to decide whether to fan a
                // shard out per tenant. Describe() cannot fill them: the list lives in
                // pc_tenant_partitions, so answering takes a round trip.
                MainDatabase = await main.DescribeAsync(token).ConfigureAwait(false)
            };
        }

        var described = new List<DatabaseDescriptor>(databases.Count);
        foreach (var database in databases)
        {
            described.Add(await database.DescribeAsync(token).ConfigureAwait(false));
        }

        return new DatabaseUsage
        {
            Cardinality = Cardinality,
            Databases = described
        };
    }

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

    /// <summary>
    ///     #703: DescribeAsync, not Describe. A conjoined store that sequences events per tenant is
    ///     single-database, so this override is the ONLY place its tenants can be reported — and a
    ///     Wolverine-managed host reads them off here to decide whether to fan a shard out per tenant.
    ///     Answering takes a round trip (the list lives in pc_tenant_partitions), which is why the
    ///     synchronous Describe() cannot do it and why this override existed as a gap rather than as a
    ///     deliberate simplification.
    /// </summary>
    public async ValueTask<DatabaseUsage> DescribeDatabasesAsync(CancellationToken token = default) =>
        new()
        {
            Cardinality = DatabaseCardinality.Single,
            MainDatabase = await _database.DescribeAsync(token).ConfigureAwait(false)
        };

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

    ValueTask<DatabaseUsage> ITenancy.DescribeDatabasesAsync(CancellationToken token)
    {
        // One descriptor per tenant, because that is one descriptor per PolecatDatabase: this
        // tenancy builds a database per REGISTERED TENANT (identified "Polecat_{tenantId}"), so two
        // tenants pointed at the same physical database are still two databases everywhere else in
        // Polecat — AllDatabases(), the resource model, the daemon's per-database coordination.
        // Marten's StaticMultiTenancy collapses them, because there a database is registered
        // directly and tenants are attached to it. Agreeing with AllDatabases() is the point of
        // this method; diverging from it here would recreate polecat#675 in a subtler shape.
        var descriptors = new List<DatabaseDescriptor>();
        foreach (var tenantId in _factories.Keys)
        {
            var descriptor = ((ITenancy)this).GetDatabase(tenantId).Describe();
            descriptor.TenantIds.Fill(tenantId);
            descriptors.Add(descriptor);
        }

        return new ValueTask<DatabaseUsage>(new DatabaseUsage
        {
            Cardinality = DatabaseCardinality.StaticMultiple,
            Databases = descriptors
        });
    }

    // The first tenant registered, matching how Marten's StaticMultiTenancy nominates the first
    // AddSingleTenantDatabase call as its Default. It only backs schema modelling and the store's
    // own Database property — nothing routes to it, because DefaultTenantUsageEnabled is off.
    string? ITenancy.SeedConnectionString =>
        _factories.Values.FirstOrDefault()?.ConnectionString;
}
