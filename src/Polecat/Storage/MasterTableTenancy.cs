using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Descriptors;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polly;

namespace Polecat.Storage;

/// <summary>
///     Dynamic separate-database multi-tenancy backed by a master "control plane" table that maps
///     <c>tenant_id</c> &rarr; connection string. Unlike <see cref="SeparateDatabaseTenancy" /> (whose
///     tenant list is fixed at registration time), tenants can be added, removed, enabled, and disabled
///     at runtime without restarting the service. This is the Polecat (SQL Server) equivalent of
///     Marten's <c>MasterTableTenancy</c> and the surface CritterWatch uses for dynamic tenant
///     management.
/// </summary>
public class MasterTableTenancy : ITenancy
{
    /// <summary>
    ///     The master tenant-registry table name (unqualified).
    /// </summary>
    public const string TableName = "pc_tenants";

    private readonly StoreOptions _options;
    private readonly string _masterConnectionString;
    private readonly string _schemaName;
    private readonly ResiliencePipeline _resilience;

    private readonly ConcurrentDictionary<string, Entry> _cache =
        new(StringComparer.OrdinalIgnoreCase);

    private volatile bool _masterTableEnsured;
    private readonly SemaphoreSlim _ensureLock = new(1, 1);

    internal MasterTableTenancy(StoreOptions options, string masterConnectionString, string schemaName)
    {
        _options = options;
        _masterConnectionString = masterConnectionString
            ?? throw new ArgumentNullException(nameof(masterConnectionString));
        _schemaName = string.IsNullOrWhiteSpace(schemaName) ? options.DatabaseSchemaName : schemaName;
        _resilience = options.ResiliencePipeline;
    }

    /// <summary>
    ///     The fully-qualified master table name, e.g. <c>[dbo].[pc_tenants]</c>.
    /// </summary>
    public string QualifiedTableName => $"[{_schemaName}].[{TableName}]";

    DatabaseCardinality ITenancy.Cardinality => DatabaseCardinality.DynamicMultiple;
    string ITenancy.DefaultTenantId => StorageConstants.DefaultTenantId;

    ConnectionFactory ITenancy.GetConnectionFactory(string tenantId)
    {
        return ResolveEntry(tenantId).Factory;
    }

    PolecatDatabase ITenancy.GetDatabase(string tenantId)
    {
        return ResolveEntry(tenantId).Database;
    }

    IReadOnlyList<PolecatDatabase> ITenancy.AllDatabases()
    {
        // Prime the cache from the master table if nothing has been loaded yet so the
        // resource model / daemon coordinator sees the current tenant set.
        if (_cache.IsEmpty)
        {
            BuildDatabasesAsync().GetAwaiter().GetResult();
        }

        return _cache.Values.Select(x => x.Database).ToList();
    }

    private Entry ResolveEntry(string tenantId)
    {
        if (_cache.TryGetValue(tenantId, out var cached)) return cached;

        // It is deliberately important *not* to silently swallow the lookup here — an unknown or
        // disabled tenant must surface as UnknownTenantIdException, same as SeparateDatabaseTenancy.
        var connectionString = LookupConnectionStringAsync(tenantId).GetAwaiter().GetResult()
            ?? throw new UnknownTenantIdException(tenantId);

        return _cache.GetOrAdd(tenantId, id => CreateEntry(id, connectionString));
    }

    private Entry CreateEntry(string tenantId, string connectionString)
    {
        var factory = new ConnectionFactory(connectionString);
        var database = new PolecatDatabase(_options, connectionString, $"Polecat_{tenantId}");
        return new Entry(factory, database);
    }

    /// <summary>
    ///     Read every enabled tenant from the master table, (re)building the in-memory database cache,
    ///     and return the full set of tenant databases the tenancy currently knows about. Mirrors
    ///     Marten's <c>BuildDatabases()</c>.
    /// </summary>
    public async Task<IReadOnlyList<PolecatDatabase>> BuildDatabasesAsync(CancellationToken token = default)
    {
        await EnsureMasterTableAsync(token).ConfigureAwait(false);

        var rows = await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, table) = state;
            var list = new List<(string TenantId, string ConnectionString)>();

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT tenant_id, connection_string FROM {table} WHERE is_disabled = 0;";

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                list.Add((reader.GetString(0), reader.GetString(1)));
            }

            return list;
        }, (_masterConnectionString, QualifiedTableName), token).ConfigureAwait(false);

        foreach (var (tenantId, connectionString) in rows)
        {
            _cache.AddOrUpdate(
                tenantId,
                id => CreateEntry(id, connectionString),
                (id, existing) => existing.Factory.ConnectionString == connectionString
                    ? existing
                    : CreateEntry(id, connectionString));
        }

        return _cache.Values.Select(x => x.Database).ToList();
    }

    /// <summary>
    ///     Add (or update) a tenant record mapping <paramref name="tenantId" /> to
    ///     <paramref name="connectionString" />. The tenant is immediately available to new sessions.
    /// </summary>
    public async Task AddDatabaseRecordAsync(string tenantId, string connectionString,
        CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        await EnsureMasterTableAsync(token).ConfigureAwait(false);

        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (masterConn, table, tenantId, connectionString) = state;
            await using var conn = new SqlConnection(masterConn);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            // Upsert: keep AddDatabaseRecordAsync idempotent and re-enable on re-add.
            cmd.CommandText = $"""
                MERGE {table} AS target
                USING (SELECT @id AS tenant_id) AS source
                ON target.tenant_id = source.tenant_id
                WHEN MATCHED THEN
                    UPDATE SET connection_string = @conn, is_disabled = 0
                WHEN NOT MATCHED THEN
                    INSERT (tenant_id, connection_string, is_disabled) VALUES (@id, @conn, 0);
                """;
            cmd.Parameters.AddWithValue("@id", tenantId);
            cmd.Parameters.AddWithValue("@conn", connectionString);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, (_masterConnectionString, QualifiedTableName, tenantId, connectionString), token).ConfigureAwait(false);

        // Eagerly cache so the tenant is usable immediately.
        _cache[tenantId] = CreateEntry(tenantId, connectionString);
    }

    /// <summary>
    ///     Remove a tenant record entirely. The tenant's database is left untouched.
    /// </summary>
    public async Task DeleteDatabaseRecordAsync(string tenantId, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        await EnsureMasterTableAsync(token).ConfigureAwait(false);
        await ExecuteByTenantAsync(
            $"DELETE FROM {QualifiedTableName} WHERE tenant_id = @id;", tenantId, token).ConfigureAwait(false);

        _cache.TryRemove(tenantId, out _);
    }

    /// <summary>
    ///     Enable a previously-disabled tenant. The tenant becomes routable again on next access.
    /// </summary>
    public async Task EnableTenantAsync(string tenantId, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        await EnsureMasterTableAsync(token).ConfigureAwait(false);
        await ExecuteByTenantAsync(
            $"UPDATE {QualifiedTableName} SET is_disabled = 0 WHERE tenant_id = @id;", tenantId, token)
            .ConfigureAwait(false);

        // Lazily reloaded into the cache on next access.
    }

    /// <summary>
    ///     Disable a tenant without deleting its record. Disabled tenants are evicted from the cache
    ///     and rejected (as <see cref="UnknownTenantIdException" />) until re-enabled.
    /// </summary>
    public async Task DisableTenantAsync(string tenantId, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        await EnsureMasterTableAsync(token).ConfigureAwait(false);
        await ExecuteByTenantAsync(
            $"UPDATE {QualifiedTableName} SET is_disabled = 1 WHERE tenant_id = @id;", tenantId, token)
            .ConfigureAwait(false);

        _cache.TryRemove(tenantId, out _);
    }

    /// <summary>
    ///     The tenant ids that are currently disabled.
    /// </summary>
    public async Task<IReadOnlyList<string>> AllDisabledAsync(CancellationToken token = default)
    {
        await EnsureMasterTableAsync(token).ConfigureAwait(false);

        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, table) = state;
            var list = new List<string>();

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT tenant_id FROM {table} WHERE is_disabled = 1;";

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                list.Add(reader.GetString(0));
            }

            return (IReadOnlyList<string>)list;
        }, (_masterConnectionString, QualifiedTableName), token).ConfigureAwait(false);
    }

    private async Task<string?> LookupConnectionStringAsync(string tenantId, CancellationToken token = default)
    {
        await EnsureMasterTableAsync(token).ConfigureAwait(false);

        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, table, tenantId) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"SELECT connection_string FROM {table} WHERE tenant_id = @id AND is_disabled = 0;";
            cmd.Parameters.AddWithValue("@id", tenantId);

            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }, (_masterConnectionString, QualifiedTableName, tenantId), token).ConfigureAwait(false);
    }

    private async Task ExecuteByTenantAsync(string sql, string tenantId, CancellationToken token)
    {
        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, sql, tenantId) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("@id", tenantId);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, (_masterConnectionString, sql, tenantId), token).ConfigureAwait(false);
    }

    /// <summary>
    ///     Create the master tenant-registry table on first use. This is a one-time control-plane
    ///     DDL operation (same documented exception as <c>DocumentTableEnsurer</c>) and is wrapped in
    ///     the resilience pipeline.
    /// </summary>
    private async Task EnsureMasterTableAsync(CancellationToken token)
    {
        if (_masterTableEnsured) return;

        // #267: honor AutoCreate.None — when the user manages the schema, the pc_tenants control
        // table is theirs to provision; never emit DDL at runtime on a least-privilege connection.
        // A later read against a missing table surfaces as a clean "invalid object" error.
        if (_options.AutoCreateSchemaObjects == AutoCreate.None)
        {
            _masterTableEnsured = true;
            return;
        }

        await _ensureLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_masterTableEnsured) return;

            await _resilience.ExecuteAsync(static async (state, ct) =>
            {
                var (connectionString, schema, table) = state;
                await using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync(ct).ConfigureAwait(false);

                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"""
                    IF SCHEMA_ID(@schema) IS NULL
                        EXEC('CREATE SCHEMA [' + @schema + ']');
                    IF OBJECT_ID(@qualified, 'U') IS NULL
                        CREATE TABLE {table} (
                            tenant_id NVARCHAR(200) NOT NULL CONSTRAINT pk_pc_tenants PRIMARY KEY,
                            connection_string NVARCHAR(MAX) NOT NULL,
                            is_disabled BIT NOT NULL CONSTRAINT df_pc_tenants_is_disabled DEFAULT 0
                        );
                    """;
                cmd.Parameters.AddWithValue("@schema", schema);
                cmd.Parameters.AddWithValue("@qualified", table);
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }, (_masterConnectionString, _schemaName, QualifiedTableName), token).ConfigureAwait(false);

            _masterTableEnsured = true;
        }
        finally
        {
            _ensureLock.Release();
        }
    }

    private readonly record struct Entry(ConnectionFactory Factory, PolecatDatabase Database);
}
