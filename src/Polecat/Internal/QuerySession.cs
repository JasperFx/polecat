using System.Data.Common;
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

    public Task<bool> CheckExistsAsync<T>(long id, CancellationToken token = default) where T : class
        => CheckExistsInternalAsync<T>(id, token);

    private async Task<bool> CheckExistsInternalAsync<T>(object id, CancellationToken token) where T : class
    {
        var provider = _providers.GetProvider<T>();
        await _tableEnsurer.EnsureTableAsync(provider, token);

        await using var cmd = new SqlCommand();

        var softDeleteFilter = provider.Mapping.DeleteStyle == Metadata.DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";

        cmd.CommandText = $"SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM {provider.Mapping.QualifiedTableName} WHERE id = @id AND tenant_id = @tenant_id{softDeleteFilter}) THEN 1 ELSE 0 END AS BIT);";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

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

        await using var cmd = new SqlCommand();
        cmd.CommandText = provider.LoadSql;
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        Logger.OnBeforeExecute(cmd.CommandText);
        try
        {
            await using var reader = await ExecuteReaderAsync(cmd, token);
            Logger.LogSuccess(cmd.CommandText);

            if (await reader.ReadAsync(token))
            {
                var json = reader.GetString(1); // data column
                T doc;
                if (provider.DocTypeColumnIndex >= 0)
                {
                    var docType = reader.GetString(provider.DocTypeColumnIndex);
                    var resolvedType = provider.Mapping.TypeFor(docType);
                    doc = (T)Serializer.FromJson(resolvedType, json);
                }
                else
                {
                    doc = Serializer.FromJson<T>(json);
                }

                SyncVersionProperties(doc, reader, provider);
                SyncCreatedAt(doc, reader);
                SyncTenantId(doc, reader);
                return doc;
            }

            return null;
        }
        catch (Exception ex)
        {
            Logger.LogFailure(cmd.CommandText, ex);
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

        await using var cmd = new SqlCommand();

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

        Logger.OnBeforeExecute(cmd.CommandText);
        try
        {
            var results = new List<T>();
            await using var reader = await ExecuteReaderAsync(cmd, token);
            Logger.LogSuccess(cmd.CommandText);

            while (await reader.ReadAsync(token))
            {
                var json = reader.GetString(1); // data column
                T doc;
                if (provider.DocTypeColumnIndex >= 0)
                {
                    var docType = reader.GetString(provider.DocTypeColumnIndex);
                    var resolvedType = provider.Mapping.TypeFor(docType);
                    doc = (T)Serializer.FromJson(resolvedType, json);
                }
                else
                {
                    doc = Serializer.FromJson<T>(json);
                }

                SyncVersionProperties(doc, reader, provider);
                SyncCreatedAt(doc, reader);
                SyncTenantId(doc, reader);
                results.Add(doc);
            }

            return results;
        }
        catch (Exception ex)
        {
            Logger.LogFailure(cmd.CommandText, ex);
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

        var softDeleteFilter = provider.Mapping.DeleteStyle == Metadata.DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";

        cmd.CommandText = $"SELECT data FROM {provider.Mapping.QualifiedTableName} WHERE id = @id AND tenant_id = @tenant_id{softDeleteFilter};";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

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

    /// <summary>
    ///     Syncs version/revision properties from the DB columns to the document object.
    ///     SelectSql column layout: id[0], data[1], version[2], last_modified[3], created_at[4], dotnet_type[5], tenant_id[6], guid_version[7]?
    /// </summary>
    internal static void SyncVersionProperties<T>(T doc, DbDataReader reader, DocumentProvider provider) where T : class
    {
        if (provider.Mapping.UseNumericRevisions && doc is IRevisioned revisioned)
        {
            revisioned.Version = reader.GetInt32(2); // version column
        }

        if (provider.Mapping.UseOptimisticConcurrency && doc is IVersioned versioned)
        {
            versioned.Version = reader.GetGuid(7); // guid_version column
        }
    }

    /// <summary>
    ///     Syncs tenant_id from the DB column to ITenanted documents.
    ///     tenant_id is at column index 6 in SelectSql.
    /// </summary>
    internal static void SyncTenantId<T>(T doc, DbDataReader reader) where T : class
    {
        if (doc is ITenanted tenanted)
        {
            tenanted.TenantId = reader.GetString(6); // tenant_id column
        }
    }

    /// <summary>
    ///     Syncs created_at from the DB column to ICreated documents.
    ///     created_at is at column index 4 in SelectSql.
    /// </summary>
    internal static void SyncCreatedAt<T>(T doc, DbDataReader reader) where T : class
    {
        if (doc is ICreated created)
        {
            created.CreatedAt = reader.GetFieldValue<DateTimeOffset>(4); // created_at column
        }
    }

    public virtual async ValueTask DisposeAsync()
    {
        await _lifetime.DisposeAsync();
    }
}
