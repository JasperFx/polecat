using Microsoft.Data.SqlClient;
using Polecat.Events;
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
    }

    internal StoreOptions Options { get; }
    public string TenantId { get; }
    public IPolecatSerializer Serializer { get; }

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

    public async Task<T?> LoadAsync<T>(Guid id, CancellationToken token = default) where T : class
    {
        return await LoadInternalAsync<T>(id, token);
    }

    public async Task<T?> LoadAsync<T>(string id, CancellationToken token = default) where T : class
    {
        return await LoadInternalAsync<T>(id, token);
    }

    protected virtual async Task<T?> LoadInternalAsync<T>(object id, CancellationToken token) where T : class
    {
        var provider = _providers.GetProvider<T>();
        await _tableEnsurer.EnsureTableAsync(provider, token);

        var conn = await GetConnectionAsync(token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = provider.LoadSql;
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        await using var reader = await cmd.ExecuteReaderAsync(token);
        if (await reader.ReadAsync(token))
        {
            var json = reader.GetString(1); // data column
            return Serializer.FromJson<T>(json);
        }

        return null;
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

        var paramNames = new string[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            paramNames[i] = $"@id{i}";
            cmd.Parameters.AddWithValue(paramNames[i], ids[i]);
        }

        cmd.CommandText = $"{provider.SelectSql} WHERE id IN ({string.Join(", ", paramNames)}) AND tenant_id = @tenant_id;";
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        var results = new List<T>();
        await using var reader = await cmd.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            var json = reader.GetString(1); // data column
            var doc = Serializer.FromJson<T>(json);
            results.Add(doc);
        }

        return results;
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
