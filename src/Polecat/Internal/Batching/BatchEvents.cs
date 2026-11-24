using JasperFx.Core.Reflection;
using JasperFx.Events;
using Polecat.Batching;
using Polecat.Events;
using Polecat.Events.Fetching;

namespace Polecat.Internal.Batching;

/// <summary>
///     #370: <see cref="IBatchEvents" /> over <see cref="BatchedQuery" />. Each call appends one item to
///     the batch and hands back its unresolved task; the tasks complete when
///     <see cref="BatchedQuery.Execute" /> walks the result sets.
/// </summary>
internal class BatchEvents : IBatchEvents
{
    private readonly BatchedQuery _parent;
    private readonly QuerySession _session;

    public BatchEvents(BatchedQuery parent, QuerySession session)
    {
        _parent = parent;
        _session = session;
    }

    public Task<StreamState?> FetchStreamState(Guid streamId) => AddStateItem(streamId);

    public Task<StreamState?> FetchStreamState(string streamKey)
        => AddStateItem(streamKey ?? throw new ArgumentNullException(nameof(streamKey)));

    public Task<IReadOnlyList<IEvent>> FetchStream(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0)
        => AddStreamItem(streamId, version, timestamp, fromVersion);

    public Task<IReadOnlyList<IEvent>> FetchStream(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0)
        => AddStreamItem(streamKey ?? throw new ArgumentNullException(nameof(streamKey)),
            version, timestamp, fromVersion);

    private Task<StreamState?> AddStateItem(object streamId)
    {
        var item = new FetchStreamStateBatchItem(EventGraph(), streamId, _session.TenantId);
        _parent.RequireEventStore();
        _parent.AddItem(item);
        return item.Result;
    }

    private Task<IReadOnlyList<IEvent>> AddStreamItem(object streamId, long version,
        DateTimeOffset? timestamp, long fromVersion)
    {
        var item = new FetchStreamBatchItem(EventGraph(), _session.Serializer, streamId, _session.TenantId,
            version, timestamp, fromVersion);
        _parent.RequireEventStore();
        _parent.AddItem(item);
        return item.Result;
    }

    private EventGraph EventGraph() => _session.Options.EventGraph;

    // ---- #1131 / #1050 (marten#6150): aggregates, by stream id and by natural key ----

    public Task<T?> FetchLatest<T>(Guid id) where T : class => FetchLatest<T, Guid>(id);

    public Task<T?> FetchLatest<T>(string key) where T : class
        => FetchLatest<T, string>(key ?? throw new ArgumentNullException(nameof(key)));

    public Task<T?> FetchLatest<T, TId>(TId id) where T : class where TId : notnull
    {
        var store = eventStore();
        var plan = planFor<T, TId>(store, id, out var lookupValue);

        // A null key can't match a stream, so there is nothing to ask the database
        if (lookupValue == null) return Task.FromResult<T?>(null);

        // As FetchLatest standalone: an aggregate the session already holds is the answer
        if (plan.IsByStreamId ? store.TryFindStreamInAggregateIdentityMap<T>(lookupValue, out var held)
                : store.TryFindInAggregateIdentityMap(plan, lookupValue, out held))
        {
            return Task.FromResult<T?>(held);
        }

        var shape = store.NaturalKeyReadShape<T>();
        if (shape.Document != null)
        {
            _parent.TrackProvider(_session.Providers.GetProvider<T>());
        }

        var item = new FetchLatestBatchItem<T>(store, plan, lookupValue, shape, _session);
        _parent.RequireEventStore();
        _parent.AddItem(item);
        return item.Result;
    }

    public Task<IEventStream<T>> FetchForWriting<T>(Guid id) where T : class => addWriting<T, Guid>(id, false, null);

    public Task<IEventStream<T>> FetchForWriting<T>(string key) where T : class
        => addWriting<T, string>(key ?? throw new ArgumentNullException(nameof(key)), false, null);

    public Task<IEventStream<T>> FetchForWriting<T>(Guid id, long expectedVersion) where T : class
        => addWriting<T, Guid>(id, false, expectedVersion);

    public Task<IEventStream<T>> FetchForWriting<T>(string key, long expectedVersion) where T : class
        => addWriting<T, string>(key ?? throw new ArgumentNullException(nameof(key)), false, expectedVersion);

    public Task<IEventStream<T>> FetchForExclusiveWriting<T>(Guid id) where T : class
        => addWriting<T, Guid>(id, true, null);

    public Task<IEventStream<T>> FetchForExclusiveWriting<T>(string key) where T : class
        => addWriting<T, string>(key ?? throw new ArgumentNullException(nameof(key)), true, null);

    public Task<IEventStream<T>> FetchForWriting<T, TId>(TId id) where T : class where TId : notnull
        => addWriting<T, TId>(id, false, null);

    public Task<IEventStream<T>> FetchForWriting<T, TId>(TId id, long expectedVersion)
        where T : class where TId : notnull
        => addWriting<T, TId>(id, false, expectedVersion);

    public Task<IEventStream<T>> FetchForExclusiveWriting<T, TId>(TId id) where T : class where TId : notnull
        => addWriting<T, TId>(id, true, null);

    private Task<IEventStream<T>> addWriting<T, TId>(TId id, bool exclusive, long? expectedVersion)
        where T : class where TId : notnull
    {
        if (_session is not DocumentSessionBase documents)
        {
            throw new InvalidOperationException(
                $"Fetching {typeof(T).FullNameInCode()} for writing in a batched query needs a document session, not a query session.");
        }

        var operations = (EventOperations)documents.Events;
        var plan = planFor<T, TId>(operations, id, out var lookupValue);

        // A null key is refused as the standalone fetch refuses it, when the fetch is added
        var value = plan.IsByStreamId ? lookupValue! : EventOperations.KeyForWriting(plan, id);

        // An aggregate in the write cache is read from its baseline by the standalone fetch. A batch can't wait
        // for the baseline, so it reads the whole stream, and the write cache is only given the result.
        var shape = new NaturalKeyFetchShape(exclusive, true, null);
        var item = new FetchForWritingBatchItem<T>(operations, plan, plan.IsByStreamId ? value : id, value, shape,
            expectedVersion, documents.Serializer, documents);

        _parent.RequireEventStore();
        _parent.RequireEventTenant(documents.TenantId, exclusive, id);
        _parent.AddItem(item);
        return item.Result;
    }

    private QueryEventStore eventStore()
        => _session is DocumentSessionBase documents
            ? (QueryEventStore)documents.Events
            : (QueryEventStore)_session.Events;

    /// <summary>
    ///     A natural key's plan, or the plan for a stream named by its id, its strong-typed wrapper unwrapped.
    /// </summary>
    private NaturalKeyFetchPlan<T> planFor<T, TId>(QueryEventStore store, TId id, out object? lookupValue)
        where T : class where TId : notnull
    {
        if (store.NaturalKeyPlanFor<T, TId>() is { } plan)
        {
            lookupValue = plan.KeyValueFor(id);
            return plan;
        }

        lookupValue = store.UnwrapStreamIdentity(id) ?? throw store.NoNaturalKey<T, TId>();

        if (lookupValue is Guid) EventGraph().EnsureAsGuidStorage();
        else EventGraph().EnsureAsStringStorage();

        return new NaturalKeyFetchPlan<T>(EventGraph());
    }
}

/// <summary>
///     #1131: <c>FetchLatest</c> in a batched query, by natural key or by stream id. One command, as standalone.
/// </summary>
internal sealed class FetchLatestBatchItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<T?> _tcs = new();
    private readonly QueryEventStore _store;
    private readonly NaturalKeyFetchPlan<T> _plan;
    private readonly object _lookupValue;
    private readonly NaturalKeyFetchShape _shape;
    private readonly QuerySession _session;

    public FetchLatestBatchItem(QueryEventStore store, NaturalKeyFetchPlan<T> plan, object lookupValue,
        NaturalKeyFetchShape shape, QuerySession session)
    {
        _store = store;
        _plan = plan;
        _lookupValue = lookupValue;
        _shape = shape;
        _session = session;
    }

    public Task<T?> Result => _tcs.Task;

    public void Fail(Exception failure) => _tcs.FailWith(failure);

    public void WriteSql(Weasel.SqlServer.ICommandBuilder builder)
        => _plan.WriteFetch(builder, _lookupValue, _store.ReadTenantId, _shape);

    public async Task ReadResultSetAsync(System.Data.Common.DbDataReader reader, CancellationToken token)
    {
        var result = await _plan.ReadFetchAsync(reader, _session.Serializer, _session, _store.ReadTenantId, _shape,
            token).ConfigureAwait(false);
        _tcs.SetResult(await _store.CompleteFetchLatestAsync(result, _shape, token).ConfigureAwait(false));
    }
}

/// <summary>
///     #1131: <c>FetchForWriting</c> and <c>FetchForExclusiveWriting</c> in a batched query, by natural key or by
///     stream id. The stream handle is made the way the standalone fetch makes it.
/// </summary>
internal sealed class FetchForWritingBatchItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<IEventStream<T>> _tcs = new();
    private readonly EventOperations _operations;
    private readonly NaturalKeyFetchPlan<T> _plan;
    private readonly object _id;
    private readonly object _lookupValue;
    private readonly NaturalKeyFetchShape _shape;
    private readonly long? _expectedVersion;
    private readonly Polecat.Serialization.ISerializer _serializer;
    private readonly DocumentSessionBase _session;

    public FetchForWritingBatchItem(EventOperations operations, NaturalKeyFetchPlan<T> plan, object id,
        object lookupValue, NaturalKeyFetchShape shape, long? expectedVersion,
        Polecat.Serialization.ISerializer serializer, DocumentSessionBase session)
    {
        _operations = operations;
        _plan = plan;
        _id = id;
        _lookupValue = lookupValue;
        _shape = shape;
        _expectedVersion = expectedVersion;
        _serializer = serializer;
        _session = session;
    }

    public Task<IEventStream<T>> Result => _tcs.Task;

    public void Fail(Exception failure) => _tcs.FailWith(failure);

    public void WriteSql(Weasel.SqlServer.ICommandBuilder builder)
        => _plan.WriteFetch(builder, _lookupValue, _operations.TenantId, _shape);

    public async Task ReadResultSetAsync(System.Data.Common.DbDataReader reader, CancellationToken token)
    {
        var result = await _plan.ReadFetchAsync(reader, _serializer, _session, _operations.TenantId, _shape, token)
            .ConfigureAwait(false);

        // A batch can't redo itself, so a key that changed hands before its stream was locked is a conflict
        if (result.Found && !result.StillNamed)
        {
            throw EventOperations.NaturalKeyMoved<T>(_lookupValue);
        }

        _tcs.SetResult(await _operations.completeFetchForWritingAsync(_plan, _id, result, false, _expectedVersion,
            token).ConfigureAwait(false));
    }
}
