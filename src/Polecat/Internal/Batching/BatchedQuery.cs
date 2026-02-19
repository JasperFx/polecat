using Polecat.Batching;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Internal.Batching;

internal class BatchedQuery : IBatchedQuery
{
    private readonly QuerySession _session;
    private readonly DocumentProviderRegistry _providers;
    private readonly DocumentTableEnsurer _tableEnsurer;
    private readonly List<IBatchQueryItem> _items = [];
    private readonly HashSet<DocumentProvider> _involvedProviders = [];

    public BatchedQuery(QuerySession session, DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer)
    {
        _session = session;
        _providers = providers;
        _tableEnsurer = tableEnsurer;
    }

    internal void AddItem(IBatchQueryItem item) => _items.Add(item);

    internal void TrackProvider(DocumentProvider provider) => _involvedProviders.Add(provider);

    public Task<T?> Load<T>(Guid id) where T : class => AddLoad<T>(id);
    public Task<T?> Load<T>(string id) where T : class => AddLoad<T>(id);
    public Task<T?> Load<T>(int id) where T : class => AddLoad<T>(id);
    public Task<T?> Load<T>(long id) where T : class => AddLoad<T>(id);

    public Task<IReadOnlyList<T>> LoadMany<T>(params Guid[] ids) where T : class
    {
        var provider = _providers.GetProvider<T>();
        _involvedProviders.Add(provider);
        var item = new LoadManyBatchQueryItem<T>(ids.Cast<object>().ToArray(), provider,
            _session.Serializer, _session.TenantId);
        _items.Add(item);
        return item.Result;
    }

    public Task<IReadOnlyList<T>> LoadMany<T>(params string[] ids) where T : class
    {
        var provider = _providers.GetProvider<T>();
        _involvedProviders.Add(provider);
        var item = new LoadManyBatchQueryItem<T>(ids.Cast<object>().ToArray(), provider,
            _session.Serializer, _session.TenantId);
        _items.Add(item);
        return item.Result;
    }

    public IBatchedQueryable<T> Query<T>() where T : class
    {
        var provider = _providers.GetProvider<T>();
        _involvedProviders.Add(provider);
        return new BatchedQueryable<T>(this, provider, _session.Options, _session.TenantId, _session.Serializer);
    }

    public async Task Execute(CancellationToken token = default)
    {
        if (_items.Count == 0) return;

        // Ensure tables exist for all involved document types
        await _tableEnsurer.EnsureTablesAsync(_involvedProviders, token);

        // Build the combined command
        var builder = new CommandBuilder();
        foreach (var item in _items)
        {
            item.WriteSql(builder);
        }

        var conn = await _session.GetConnectionAsync(token);
        await using var cmd = conn.CreateCommand();

        if (_session.ActiveTransaction != null)
        {
            cmd.Transaction = _session.ActiveTransaction;
        }

        builder.ApplyTo(cmd);

        await using var reader = await cmd.ExecuteReaderAsync(token);

        // Process each result set
        for (var i = 0; i < _items.Count; i++)
        {
            if (i > 0)
            {
                await reader.NextResultAsync(token);
            }

            await _items[i].ReadResultSetAsync(reader, token);
        }
    }

    private Task<T?> AddLoad<T>(object id) where T : class
    {
        var provider = _providers.GetProvider<T>();
        _involvedProviders.Add(provider);
        var item = new LoadBatchQueryItem<T>(id, provider, _session.Serializer, _session.TenantId);
        _items.Add(item);
        return item.Result;
    }
}
