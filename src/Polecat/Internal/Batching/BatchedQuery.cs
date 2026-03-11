using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Batching;
using Polecat.Events;
using Polecat.Events.Dcb;
using Polecat.Linq;
using Weasel.SqlServer;

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

    public IQuerySession Parent => _session;

    internal void AddItem(IBatchQueryItem item) => _items.Add(item);

    internal void TrackProvider(DocumentProvider provider) => _involvedProviders.Add(provider);

    public Task<bool> CheckExists<T>(Guid id) where T : class => AddCheckExists<T>(id);
    public Task<bool> CheckExists<T>(string id) where T : class => AddCheckExists<T>(id);
    public Task<bool> CheckExists<T>(int id) where T : class => AddCheckExists<T>(id);
    public Task<bool> CheckExists<T>(long id) where T : class => AddCheckExists<T>(id);

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

    public Task<T> QueryByPlan<T>(IBatchQueryPlan<T> plan)
    {
        return plan.Fetch(this);
    }

    /// <summary>
    ///     Register an IQueryable as a deferred list query in this batch.
    ///     Used by QueryListPlan to integrate with batched execution.
    /// </summary>
    internal Task<IReadOnlyList<T>> AddQueryableList<T>(IQueryable<T> queryable) where T : class
    {
        if (queryable.Provider is not PolecatLinqQueryProvider provider)
            throw new InvalidOperationException(
                "QueryListPlan can only be used with Polecat queryables from session.Query<T>().");

        var statement = provider.BuildStatement(queryable.Expression, _session.TenantId);
        var item = new QueryListBatchItem<T>(statement, _session.Serializer);
        AddItem(item);
        return item.Result;
    }

    public Task<bool> EventsExist(EventTagQuery query)
    {
        var eventGraph = _session is DocumentSessionBase docSession
            ? docSession.Options.EventGraph
            : throw new InvalidOperationException("EventsExist requires a document session.");

        var item = new EventsExistBatchItem(eventGraph, query);
        _items.Add(item);
        return item.Result;
    }

    public Task<IEventBoundary<T>> FetchForWritingByTags<T>(EventTagQuery query) where T : class
    {
        if (_session is not DocumentSessionBase docSession)
            throw new InvalidOperationException("FetchForWritingByTags requires a document session, not a query session.");

        var eventGraph = docSession.Options.EventGraph;
        var item = new FetchForWritingByTagsBatchItem<T>(eventGraph, query, docSession, _session.Serializer);
        _items.Add(item);
        return item.Result;
    }

    public async Task Execute(CancellationToken token = default)
    {
        if (_items.Count == 0) return;

        // Ensure tables exist for all involved document types
        await _tableEnsurer.EnsureTablesAsync(_involvedProviders, token);

        // Build the combined batch (connection-less; lifetime sets it at execution)
        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        for (var i = 0; i < _items.Count; i++)
        {
            if (i > 0) builder.StartNewCommand();
            _items[i].WriteSql(builder);
        }

        builder.Compile();
        await using var reader = await _session.ExecuteReaderAsync(batch, token);

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

    private Task<bool> AddCheckExists<T>(object id) where T : class
    {
        var provider = _providers.GetProvider<T>();
        _involvedProviders.Add(provider);
        var item = new CheckExistsBatchQueryItem<T>(id, provider, _session.TenantId);
        _items.Add(item);
        return item.Result;
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
