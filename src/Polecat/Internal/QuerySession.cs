using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polly;
using Polecat.Batching;
using Polecat.Events;
using Polecat.Internal.Batching;
using Polecat.Internal.Sessions;
using Polecat.Linq;
using Polecat.Logging;
using Polecat.Metadata;
using Polecat.Serialization;

namespace Polecat.Internal;

/// <summary>
///     Read-only query session. All SQL execution flows through Polly-wrapped
///     centralized methods backed by IConnectionLifetime.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level (all partials): routes load/query operations through ISerializer.FromJson and per-document DocumentProvider reflection. Document types T flow in from caller registration (Schema.For<T>() / session.Load<T>) and are preserved per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level (all partials): ISerializer.FromJson is annotated RDC; AOT consumers supply a source-generator-backed ISerializer impl.")]
internal partial class QuerySession : IQuerySession
{
    internal readonly IConnectionLifetime _lifetime;
    private readonly ResiliencePipeline _resilience;
    protected readonly DocumentProviderRegistry _providers;
    protected readonly DocumentTableEnsurer _tableEnsurer;
    protected readonly EventGraph _eventGraph;
    private QueryEventStore? _events;

    // Session-level aggregate identity map used when UseIdentityMapForAggregates is enabled.
    // Mirrors Marten's ItemMap pattern: Dictionary<Type, object> where each value is
    // a Dictionary<TId, TDoc> for the specific aggregate/id types.
    internal Dictionary<Type, object> AggregateIdentityMap { get; } = new();

    internal void StoreAggregateInIdentityMap<TDoc, TId>(TId id, TDoc document)
        where TDoc : class where TId : notnull
    {
        if (AggregateIdentityMap.TryGetValue(typeof(TDoc), out var raw))
        {
            if (raw is Dictionary<TId, TDoc> typedDict)
            {
                typedDict[id] = document;
            }
            // else: The identity map was created with a different key type (e.g., a strong-typed ID
            // like PaymentId while TId is Guid). The document is already stored by the inline
            // projection under the strong-typed key, so we skip storing it again to avoid
            // replacing the dictionary with an incompatible key type.
        }
        else
        {
            var dict = new Dictionary<TId, TDoc> { [id] = document };
            AggregateIdentityMap[typeof(TDoc)] = dict;
        }
    }

    internal bool TryGetAggregateFromIdentityMap<TDoc, TId>(TId id, out TDoc? document)
        where TDoc : class where TId : notnull
    {
        document = default;
        if (!_eventGraph.UseIdentityMapForAggregates) return false;

        if (AggregateIdentityMap.TryGetValue(typeof(TDoc), out var raw)
            && raw is Dictionary<TId, TDoc> dict
            && dict.TryGetValue(id, out var cached))
        {
            document = cached;
            return true;
        }

        return false;
    }

    public QuerySession(
        StoreOptions options,
        IConnectionLifetime lifetime,
        DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer,
        EventGraph eventGraph,
        string tenantId)
    {
        Options = options;
        Serializer = options.Serializer;
        TenantId = tenantId;
        _lifetime = lifetime;
        _resilience = options.ResiliencePipeline;
        _providers = providers;
        _tableEnsurer = tableEnsurer;
        _eventGraph = eventGraph;
        Logger = options.Logger.StartSession(this);

        // #239: seed correlation/causation from the ambient Activity so distributed-tracing context
        // flows onto events (and metadata columns) with zero app code, mirroring Marten's
        // DocumentStore session wiring. A caller can still override either after construction; the
        // ??= leaves any pre-seeded value untouched.
        CorrelationId ??= Activity.Current?.RootId;
        CausationId ??= Activity.Current?.ParentId;
    }

    internal StoreOptions Options { get; }
    internal DocumentProviderRegistry Providers => _providers;
    public string TenantId { get; }
    public ISerializer Serializer { get; }
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? LastModifiedBy { get; set; }

    // #240: session-level header bag, lazily created on first SetHeader.
    public Dictionary<string, object>? Headers { get; private set; }

    public void SetHeader(string key, object value)
    {
        Headers ??= new Dictionary<string, object>();
        Headers[key] = value;
    }

    public object? GetHeader(string key)
    {
        if (Headers != null && Headers.TryGetValue(key, out var value))
        {
            return value;
        }

        return null;
    }

    public int RequestCount { get; internal set; }
    public IPolecatSessionLogger Logger { get; set; }

    public IQueryEventStore Events => _events ??= new QueryEventStore(this, _eventGraph, Options);

    // ── Centralized Polly-wrapped execution methods ──────────────────────

    private record CommandExecution(SqlCommand Command, IConnectionLifetime Lifetime);
    private record BatchExecution(SqlBatch Batch, IConnectionLifetime Lifetime);

    internal Task<int> ExecuteAsync(SqlCommand command, CancellationToken token)
    {
        RequestCount++;
        return _resilience.ExecuteAsync(
            static (state, t) => new ValueTask<int>(state.Lifetime.ExecuteAsync(state.Command, t)),
            new CommandExecution(command, _lifetime), token).AsTask();
    }

    internal Task<object?> ExecuteScalarAsync(SqlCommand command, CancellationToken token)
    {
        RequestCount++;
        return _resilience.ExecuteAsync(
            static (state, t) => new ValueTask<object?>(state.Lifetime.ExecuteScalarAsync(state.Command, t)),
            new CommandExecution(command, _lifetime), token).AsTask();
    }

    internal Task<DbDataReader> ExecuteReaderAsync(SqlCommand command, CancellationToken token)
    {
        RequestCount++;
        return _resilience.ExecuteAsync(
            static (state, t) => new ValueTask<DbDataReader>(state.Lifetime.ExecuteReaderAsync(state.Command, t)),
            new CommandExecution(command, _lifetime), token).AsTask();
    }

    internal Task<DbDataReader> ExecuteReaderAsync(SqlBatch batch, CancellationToken token)
    {
        RequestCount++;
        return _resilience.ExecuteAsync(
            static (state, t) => new ValueTask<DbDataReader>(state.Lifetime.ExecuteReaderAsync(state.Batch, t)),
            new BatchExecution(batch, _lifetime), token).AsTask();
    }

    // ── Existence check operations ──────────────────────────────────────

    public Task<bool> CheckExistsAsync<T>(Guid id, CancellationToken token = default) where T : class
        => CheckExistsInternalAsync<T>(id, token);

    public Task<bool> CheckExistsAsync<T>(string id, CancellationToken token = default) where T : class
        => CheckExistsInternalAsync<T>(id, token);

    public Task<bool> CheckExistsAsync<T>(int id, CancellationToken token = default) where T : class
        => CheckExistsInternalAsync<T>(id, token);

    /// <summary>
    ///     #219: ensure the event store schema exists on the fly before an event read/write.
    /// </summary>
    internal Task EnsureEventStoreSchemaAsync(CancellationToken token)
        => _tableEnsurer.EnsureEventStoreSchemaAsync(token);

    public Task<bool> CheckExistsAsync<T>(long id, CancellationToken token = default) where T : class
        => CheckExistsInternalAsync<T>(id, token);

    private async Task<bool> CheckExistsInternalAsync<T>(object id, CancellationToken token) where T : class
    {
        var provider = _providers.GetProvider<T>();
        await _tableEnsurer.EnsureTableAsync(provider, token);

        await using var cmd = new SqlCommand();

        var softDeleteFilter = provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";

        // #234: tenant_id predicate is conjoined-only.
        var tenantFilter = provider.Mapping.TenancyStyle == TenancyStyle.Conjoined ? " AND tenant_id = @tenant_id" : "";
        cmd.CommandText = $"SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM {provider.Mapping.QualifiedTableName} WHERE id = @id{tenantFilter}{softDeleteFilter}) THEN 1 ELSE 0 END AS BIT);";
        cmd.Parameters.AddWithValue("@id", id);
        if (provider.Mapping.TenancyStyle == TenancyStyle.Conjoined) cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        Logger.OnBeforeExecute(cmd.CommandText);
        try
        {
            var result = await ExecuteScalarAsync(cmd, token);
            Logger.LogSuccess(cmd.CommandText);
            return result is true;
        }
        catch (Exception ex)
        {
            Logger.LogFailure(cmd.CommandText, ex);
            throw;
        }
    }

    // ── Load operations ─────────────────────────────────────────────────

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

        // #273 E2a/E2e: all document loads route through the closed-shape storage layer
        // (shared selectors + dialect commands over the descriptor); subclass types resolve
        // a SubClassPolecatStorage wrapping the hierarchy root's storage.
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectStorage<T>)
            ((Weasel.Storage.IStorageSession)this).StorageFor<T>();
        return await storage.LoadByObjectIdAsync(id, this, token);
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

        // #273 E2a/E2e: all document loads route through the closed-shape storage layer.
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectStorage<T>)
            ((Weasel.Storage.IStorageSession)this).StorageFor<T>();
        return await storage.LoadManyByObjectIdsAsync(ids, this, token);
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

    public Task<T> QueryByPlanAsync<T>(IQueryPlan<T> plan, CancellationToken token = default)
    {
        return plan.Fetch(this, token);
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

        await using var cmd = new SqlCommand();

        var softDeleteFilter = provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";

        // #234: tenant_id predicate is conjoined-only.
        var tenantFilter = provider.Mapping.TenancyStyle == TenancyStyle.Conjoined ? " AND tenant_id = @tenant_id" : "";
        cmd.CommandText = $"SELECT data FROM {provider.Mapping.QualifiedTableName} WHERE id = @id{tenantFilter}{softDeleteFilter};";
        cmd.Parameters.AddWithValue("@id", id);
        if (provider.Mapping.TenancyStyle == TenancyStyle.Conjoined) cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        Logger.OnBeforeExecute(cmd.CommandText);
        try
        {
            var result = await ExecuteScalarAsync(cmd, token);
            Logger.LogSuccess(cmd.CommandText);
            return result is string json ? json : null;
        }
        catch (Exception ex)
        {
            Logger.LogFailure(cmd.CommandText, ex);
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

    public virtual async ValueTask DisposeAsync()
    {
        await _lifetime.DisposeAsync();
    }
}
