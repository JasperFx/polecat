using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using JasperFx.Descriptors;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polly;
using Weasel.Core;
using Weasel.Core.MultiTenancy;
using Weasel.SqlServer;
using Weasel.SqlServer.MultiTenancy;

namespace Polecat.Storage;

/// <summary>
///     Dynamic separate-database multi-tenancy backed by a master "control plane" table that maps
///     <c>tenant_id</c> &rarr; connection string. Unlike <see cref="SeparateDatabaseTenancy" /> (whose
///     tenant list is fixed at registration time), tenants can be added, removed, enabled, and disabled
///     at runtime without restarting the service. This is the Polecat (SQL Server) equivalent of
///     Marten's <c>MasterTableTenancy</c> and the surface CritterWatch uses for dynamic tenant
///     management.
/// </summary>
/// <remarks>
///     <para>
///         #807: the control-table runtime — the resolved-database cache, eviction of tenants disabled or
///         removed on another node (#871, #849), telling a disabled tenant from an unknown one (#655), and
///         the whole <see cref="IDynamicTenantSource{T}" /> lifecycle (#377) — is Weasel's
///         <see cref="MasterTableTenancyBase{TDatabase,TDataSource}" /> (weasel#809, Weasel 10.0.0-alpha.2),
///         shared with Marten and Fisher. The SQL is <see cref="SqlServerMasterTenantTableDialect" />,
///         which renders exactly the statements this class used to carry. What stays here is Polecat's:
///         building a <see cref="PolecatDatabase" />, the resilience pipeline, and creating the table
///         through a Weasel migration under the startup migration lock.
///     </para>
///     <para>
///         Three things are kept as they were rather than taking the base's default, because each one
///         would otherwise change behaviour on an upgrade: tenant ids compare case-insensitively, as
///         they always have here (and as the table's default collation does); the table is created by
///         <see cref="TenantsTable" /> through Weasel rather than by the dialect's guarded DDL string;
///         and no <c>Application Name</c> is written into a tenant's connection string.
///     </para>
/// </remarks>
public class MasterTableTenancy : MasterTableTenancyBase<PolecatDatabase, DbDataSource>, ITenancy
{
    private readonly StoreOptions _options;
    private readonly ResiliencePipeline _resilience;
    private readonly SemaphoreSlim _provisioning = new(1, 1);

    private volatile bool _masterTableEnsured;

    internal MasterTableTenancy(StoreOptions options, string masterConnectionString, string schemaName)
        : base(
            BuildOptions(masterConnectionString,
                string.IsNullOrWhiteSpace(schemaName) ? options.DatabaseSchemaName : schemaName),
            SqlServerMasterTenantTableDialect.DefaultTableName,
            SqlServerMasterTenantTableDialect.Instance,
            StringComparer.OrdinalIgnoreCase)
    {
        _options = options;
        _resilience = options.ResiliencePipeline;
    }

    private static MasterTableTenancyOptions<DbDataSource> BuildOptions(string masterConnectionString,
        string schemaName)
    {
        ArgumentNullException.ThrowIfNull(masterConnectionString);

        return new MasterTableTenancyOptions<DbDataSource>(SqlServerProvider.Instance)
        {
            ConnectionString = masterConnectionString,
            SchemaName = schemaName,

            // The base would stamp the entry assembly's name on every tenant connection string. Polecat
            // never has, and doing it now would give each tenant a different string from the one its
            // operator registered — and a separate connection pool from any other use of that string.
            ApplicationName = string.Empty
        };
    }

    /// <summary>
    ///     The schema the control table lives in.
    /// </summary>
    public string SchemaName => Options.SchemaName;

    string ITenancy.DefaultTenantId => StorageConstants.DefaultTenantId;

    /// <summary>
    ///     The control-plane database. Master table tenancy always has one, so an application never
    ///     needs to set StoreOptions.ConnectionString as well. polecat#514.
    /// </summary>
    public string? SeedConnectionString => Options.ConnectionString;

    ConnectionFactory ITenancy.GetConnectionFactory(string tenantId)
    {
        return new ConnectionFactory(Resolve(tenantId).ConnectionString);
    }

    PolecatDatabase ITenancy.GetDatabase(string tenantId)
    {
        return Resolve(tenantId);
    }

    IReadOnlyList<PolecatDatabase> ITenancy.AllDatabases()
    {
        // Prime the cache from the master table if nothing has been loaded yet so the
        // resource model / daemon coordinator sees the current tenant set.
        var databases = AllDatabases();
        return databases.Count == 0
            ? BuildDatabasesAsync().GetAwaiter().GetResult()
            : databases;
    }

    /// <summary>
    ///     #764: the database for a tenant registered and enabled in the master table, or null for an
    ///     unknown or disabled one. A read of the registry and nothing else: no tenant row is written and
    ///     no tenant database is created, and an unknown tenant is not cached, so a tenant added later
    ///     resolves on the next call.
    /// </summary>
    public ValueTask<PolecatDatabase?> FindDatabaseAsync(string tenantId, CancellationToken token = default)
    {
        return TryFindDatabaseAsync(tenantId, token);
    }

    /// <summary>
    ///     polecat#675 — describe the tenant databases currently registered in the master table.
    ///     Reads the control table first, so the descriptor reflects tenants added since the store
    ///     was built. Mirrors Marten's <c>MasterTableTenancy.DescribeDatabasesAsync()</c>.
    /// </summary>
    public async ValueTask<DatabaseUsage> DescribeDatabasesAsync(CancellationToken token = default)
    {
        await BuildDatabasesAsync(token).ConfigureAwait(false);

        // One descriptor per tenant — one per PolecatDatabase, matching AllDatabases(). See the note
        // on SeparateDatabaseTenancy.DescribeDatabasesAsync for why these are not collapsed by
        // physical database the way Marten's are.
        var descriptors = new List<DatabaseDescriptor>();
        foreach (var tenantId in AllTenantIds())
        {
            var database = FindCachedDatabase(tenantId);
            if (database is null) continue;

            var descriptor = database.Describe();
            descriptor.TenantIds.Fill(tenantId);
            descriptors.Add(descriptor);
        }

        return new DatabaseUsage
        {
            Cardinality = DatabaseCardinality.DynamicMultiple,
            Databases = descriptors
        };
    }

    // It is deliberately important *not* to silently swallow the lookup here — an unknown or disabled
    // tenant must refuse the session, same as SeparateDatabaseTenancy. #655: the two are told apart,
    // because they call for different actions.
    private PolecatDatabase Resolve(string tenantId)
    {
        if (FindCachedDatabase(tenantId) is { } cached) return cached;

        var resolution = ResolveTenantAsync(tenantId).AsTask().GetAwaiter().GetResult();
        return resolution.Database ?? throw CreateTenantRefusal(tenantId, resolution.Status);
    }

    protected override DbDataSource BuildDataSource(string connectionString)
    {
        return SqlClientFactory.Instance.CreateDataSource(connectionString);
    }

    protected override PolecatDatabase BuildDatabase(string tenantId, string connectionString)
    {
        return new PolecatDatabase(_options, connectionString, $"Polecat_{tenantId}");
    }

    /// <summary>
    ///     #655 / jasperfx#875. A missing row and a disabled row both used to raise
    ///     <see cref="UnknownTenantIdException" />, so the operator who had just run CritterWatch's
    ///     <c>disable_tenant</c> — or the on-call engineer after them — read "Unknown tenant id
    ///     'acme'" about a tenant that was still registered and had been turned off on purpose.
    ///     <see cref="DisabledTenantException" /> derives from <see cref="UnknownTenantIdException" />,
    ///     so nothing that catches the old type stops catching; the message just stops being wrong.
    /// </summary>
    protected override UnknownTenantIdException CreateTenantRefusal(string tenantId, MasterTenantStatus status)
    {
        return status == MasterTenantStatus.Disabled
            ? new DisabledTenantException(tenantId)
            : new UnknownTenantIdException(tenantId);
    }

    /// <summary>
    ///     Every control-plane read and write runs through <see cref="StoreOptions.ResiliencePipeline" />.
    /// </summary>
    protected override async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> operation,
        CancellationToken token)
    {
        return await _resilience.ExecuteAsync(static async (op, ct) => await op(ct).ConfigureAwait(false),
            operation, token).ConfigureAwait(false);
    }

    /// <summary>
    ///     Read at the moment the table is needed, not when the tenancy was configured, so an application
    ///     that sets <see cref="StoreOptions.AutoCreateSchemaObjects" /> after calling
    ///     <see cref="StoreOptions.MultiTenantedMasterTable" /> still gets what it asked for.
    /// </summary>
    protected override AutoCreate EffectiveAutoCreate => Options.AutoCreate ?? _options.AutoCreateSchemaObjects;

    /// <summary>
    ///     Create the master tenant-registry table on first use, through <see cref="TenantsTable" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Every piece of DDL in Polecat goes through Weasel, so this replaces the base's guarded
    ///         CREATE string with a migration of the table.
    ///     </para>
    ///     <para>
    ///         #883: under the migration lock, because the semaphore below covers one tenancy in one
    ///         process, and two replicas starting together both found the table missing and the loser
    ///         failed on the CREATE. Nothing has to worry about a transaction here: this runs as a session
    ///         is being opened, and a process has finished it before its first session exists.
    ///     </para>
    ///     <para>
    ///         #267: <see cref="AutoCreate.None" /> means the control table is the operator's to provision;
    ///         no DDL is emitted on what may be a least-privilege connection, and a read against a missing
    ///         table surfaces as a clean "invalid object" error.
    ///     </para>
    /// </remarks>
    protected override async Task ProvisionControlTableAsync(CancellationToken token)
    {
        if (_masterTableEnsured) return;

        if (EffectiveAutoCreate == AutoCreate.None)
        {
            _masterTableEnsured = true;
            return;
        }

        await _provisioning.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_masterTableEnsured) return;

            await _resilience.ExecuteAsync(static async (state, ct) =>
            {
                var (connectionString, options, schema) = state;
                await using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync(ct).ConfigureAwait(false);

                await using var migrationLock = (await SchemaMigrationLock.HoldForFirstUseAsync(
                    options, conn, insideTransaction: false, ct, schema).ConfigureAwait(false));

                var migration = await SchemaMigration.DetermineAsync(conn, ct, new TenantsTable(schema))
                    .ConfigureAwait(false);
                await new SqlServerMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct)
                    .ConfigureAwait(false);
            }, (Options.ConnectionString!, _options, Options.SchemaName), token).ConfigureAwait(false);

            _masterTableEnsured = true;
        }
        finally
        {
            _provisioning.Release();
        }
    }
}
