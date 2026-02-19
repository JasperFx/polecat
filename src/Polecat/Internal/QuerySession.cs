using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Batching;
using Polecat.Events;
using Polecat.Internal.Batching;
using Polecat.Linq;
using Polecat.Logging;
using Polecat.Metadata;
using Polecat.Serialization;

namespace Polecat.Internal;

/// <summary>
///     Read-only query session. Opens a lazy connection and executes load queries.
/// </summary>
internal class QuerySession : IQuerySession
{
    private readonly ConnectionFactory _connectionFactory;
    protected readonly DocumentProviderRegistry _providers;
    protected readonly DocumentTableEnsurer _tableEnsurer;
    protected readonly EventGraph _eventGraph;
    private SqlConnection? _connection;
    private QueryEventStore? _events;

    public QuerySession(
        StoreOptions options,
        ConnectionFactory connectionFactory,
        DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer,
        EventGraph eventGraph,
        string tenantId)
    {
        Options = options;
        Serializer = options.Serializer;
        TenantId = tenantId;
        _connectionFactory = connectionFactory;
        _providers = providers;
        _tableEnsurer = tableEnsurer;
        _eventGraph = eventGraph;
        Logger = options.Logger.StartSession(this);
    }

    internal StoreOptions Options { get; }
    internal DocumentProviderRegistry Providers => _providers;
    public string TenantId { get; }
    public ISerializer Serializer { get; }
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? LastModifiedBy { get; set; }
    public int RequestCount { get; internal set; }
    public IPolecatSessionLogger Logger { get; set; }
    internal virtual SqlTransaction? ActiveTransaction { get => null; set { } }

    public IQueryEventStore Events => _events ??= new QueryEventStore(this, _eventGraph, Options);

    internal async Task<SqlConnection> GetConnectionAsync(CancellationToken token)
    {
        if (_connection == null)
        {
            _connection = _connectionFactory.Create();
            await _connection.OpenAsync(token);
        }

        return _connection;
    }

    /// <summary>
    ///     Enlists a command in the active transaction if one exists.
    /// </summary>
    internal void EnlistInTransaction(SqlCommand cmd)
    {
        if (ActiveTransaction != null) cmd.Transaction = ActiveTransaction;
    }

    public async Task<T?> LoadAsync<T>(Guid id, CancellationToken token = default) where T : class
    {
        return await LoadInternalAsync<T>(id, token);
    }

    public async Task<T?> LoadAsync<T>(string id, CancellationToken token = default) where T : class
    {
        return await LoadInternalAsync<T>(id, token);
    }

    public async Task<T?> LoadAsync<T>(int id, CancellationToken token = default) where T : class
    {
        return await LoadInternalAsync<T>(id, token);
    }

    public async Task<T?> LoadAsync<T>(long id, CancellationToken token = default) where T : class
    {
        return await LoadInternalAsync<T>(id, token);
    }

    protected virtual async Task<T?> LoadInternalAsync<T>(object id, CancellationToken token) where T : class
    {
        var provider = _providers.GetProvider<T>();
        await _tableEnsurer.EnsureTableAsync(provider, token);

        var conn = await GetConnectionAsync(token);
        await using var cmd = conn.CreateCommand();
        EnlistInTransaction(cmd);
        cmd.CommandText = provider.LoadSql;
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        Logger.OnBeforeExecute(cmd);
        try
        {
            await using var reader = await cmd.ExecuteReaderAsync(token);
            RequestCount++;
            Logger.LogSuccess(cmd);

            if (await reader.ReadAsync(token))
            {
                var json = reader.GetString(1); // data column
                var doc = Serializer.FromJson<T>(json);
                SyncVersionProperties(doc, reader, provider);
                SyncTenantId(doc, reader);
                return doc;
            }

            return null;
        }
        catch (Exception ex)
        {
            Logger.LogFailure(cmd, ex);
            throw;
        }
    }

    public async Task<IReadOnlyList<T>> LoadManyAsync<T>(IEnumerable<Guid> ids, CancellationToken token = default)
        where T : class
    {
        return await LoadManyInternalAsync<T>(ids.Cast<object>().ToList(), token);
    }

    public async Task<IReadOnlyList<T>> LoadManyAsync<T>(IEnumerable<string> ids, CancellationToken token = default)
        where T : class
    {
        return await LoadManyInternalAsync<T>(ids.Cast<object>().ToList(), token);
    }

    protected virtual async Task<IReadOnlyList<T>> LoadManyInternalAsync<T>(
        List<object> ids, CancellationToken token) where T : class
    {
        if (ids.Count == 0) return [];

        var provider = _providers.GetProvider<T>();
        await _tableEnsurer.EnsureTableAsync(provider, token);

        var conn = await GetConnectionAsync(token);
        await using var cmd = conn.CreateCommand();
        EnlistInTransaction(cmd);

        var paramNames = new string[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            paramNames[i] = $"@id{i}";
            cmd.Parameters.AddWithValue(paramNames[i], ids[i]);
        }

        var softDeleteFilter = provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";
        cmd.CommandText = $"{provider.SelectSql} WHERE id IN ({string.Join(", ", paramNames)}) AND tenant_id = @tenant_id{softDeleteFilter};";
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        Logger.OnBeforeExecute(cmd);
        try
        {
            var results = new List<T>();
            await using var reader = await cmd.ExecuteReaderAsync(token);
            RequestCount++;
            Logger.LogSuccess(cmd);

            while (await reader.ReadAsync(token))
            {
                var json = reader.GetString(1); // data column
                var doc = Serializer.FromJson<T>(json);
                SyncVersionProperties(doc, reader, provider);
                SyncTenantId(doc, reader);
                results.Add(doc);
            }

            return results;
        }
        catch (Exception ex)
        {
            Logger.LogFailure(cmd, ex);
            throw;
        }
    }

    public IPolecatQueryable<T> Query<T>() where T : class
    {
        var provider = new PolecatLinqQueryProvider(this, _providers, _tableEnsurer);
        return new PolecatLinqQueryable<T>(provider);
    }

    public IBatchedQuery CreateBatchQuery()
    {
        return new BatchedQuery(this, _providers, _tableEnsurer);
    }

    public Task<string?> LoadJsonAsync<T>(Guid id, CancellationToken token = default) where T : class
        => LoadJsonInternalAsync<T>(id, token);

    public Task<string?> LoadJsonAsync<T>(string id, CancellationToken token = default) where T : class
        => LoadJsonInternalAsync<T>(id, token);

    public Task<string?> LoadJsonAsync<T>(int id, CancellationToken token = default) where T : class
        => LoadJsonInternalAsync<T>(id, token);

    public Task<string?> LoadJsonAsync<T>(long id, CancellationToken token = default) where T : class
        => LoadJsonInternalAsync<T>(id, token);

    private async Task<string?> LoadJsonInternalAsync<T>(object id, CancellationToken token) where T : class
    {
        var provider = _providers.GetProvider<T>();
        await _tableEnsurer.EnsureTableAsync(provider, token);

        var conn = await GetConnectionAsync(token);
        await using var cmd = conn.CreateCommand();
        EnlistInTransaction(cmd);

        var softDeleteFilter = provider.Mapping.DeleteStyle == Metadata.DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";

        cmd.CommandText = $"SELECT data FROM {provider.Mapping.QualifiedTableName} WHERE id = @id AND tenant_id = @tenant_id{softDeleteFilter};";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        Logger.OnBeforeExecute(cmd);
        try
        {
            var result = await cmd.ExecuteScalarAsync(token);
            RequestCount++;
            Logger.LogSuccess(cmd);
            return result is string json ? json : null;
        }
        catch (Exception ex)
        {
            Logger.LogFailure(cmd, ex);
            throw;
        }
    }

    public string ToSql<T>(IQueryable<T> queryable) where T : class
    {
        if (queryable.Provider is not PolecatLinqQueryProvider polecatProvider)
        {
            throw new InvalidOperationException(
                "ToSql can only be used with Polecat IQueryable instances.");
        }

        return polecatProvider.BuildSql(queryable.Expression, TenantId);
    }

    /// <summary>
    ///     Syncs version/revision properties from the DB columns to the document object.
    ///     SelectSql column layout: id[0], data[1], version[2], last_modified[3], dotnet_type[4], tenant_id[5], guid_version[6]?
    /// </summary>
    internal static void SyncVersionProperties<T>(T doc, SqlDataReader reader, DocumentProvider provider) where T : class
    {
        if (provider.Mapping.UseNumericRevisions && doc is IRevisioned revisioned)
        {
            revisioned.Version = reader.GetInt32(2); // version column
        }

        if (provider.Mapping.UseOptimisticConcurrency && doc is IVersioned versioned)
        {
            versioned.Version = reader.GetGuid(6); // guid_version column
        }
    }

    /// <summary>
    ///     Syncs tenant_id from the DB column to ITenanted documents.
    ///     tenant_id is at column index 5 in SelectSql.
    /// </summary>
    internal static void SyncTenantId<T>(T doc, SqlDataReader reader) where T : class
    {
        if (doc is ITenanted tenanted)
        {
            tenanted.TenantId = reader.GetString(5); // tenant_id column
        }
    }

    public virtual async ValueTask DisposeAsync()
    {
        if (_connection != null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
