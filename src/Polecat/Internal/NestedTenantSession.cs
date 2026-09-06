using System.Linq.Expressions;
using Polecat.Batching;
using Polecat.Events;
using Polecat.Linq;
using Polecat.Logging;
using Polecat.Metadata;
using Polecat.Serialization;

namespace Polecat.Internal;

/// <summary>
///     Lightweight wrapper around a parent session that scopes document and event
///     operations to a different tenant. Shares the parent's connection, work tracker,
///     and serializer so all operations flush in a single transaction.
/// </summary>
internal class NestedTenantSession : ITenantOperations
{
    private readonly DocumentSessionBase _parent;
    private readonly string _tenantId;
    private EventOperations? _eventOperations;
    private TenantScopedStorageSession? _storageScope;

    public NestedTenantSession(DocumentSessionBase parent, string tenantId)
    {
        _parent = parent;
        _tenantId = tenantId;
    }

    public string TenantId => _tenantId;

    /// <summary>
    ///     polecat#548 — the single <see cref="TenantScopedStorageSession" /> this view's reads and
    ///     writes both run against. One instance per nested session (and ForTenant caches one nested
    ///     session per tenant), so identity-map and version state persist per tenant across repeated
    ///     ForTenant(t) calls the way the parent session's do for its own tenant — Marten reaches the
    ///     same result by caching one nested session per tenant.
    ///
    ///     Identity state is isolated only for CONJOINED storage, where the same id names a different
    ///     document per tenant (marten#4801). Under single-tenant storage there is one row per id for
    ///     the whole database, so the parent's map is aliased instead — isolating it there would hide
    ///     the parent's uncommitted documents from this view, which is marten#4947.
    /// </summary>
    private TenantScopedStorageSession StorageScope
        => _storageScope ??= new TenantScopedStorageSession(
            (Weasel.Storage.IStorageSession)_parent, _tenantId,
            shareIdentityState: _parent.Options.Events.TenancyStyle != TenancyStyle.Conjoined);
    public IDocumentSession Parent => _parent;

    public IEventOperations Events =>
        _eventOperations ??= new EventOperations(_parent, _parent.EventGraph, _parent.Options,
            _parent.WorkTracker, _tenantId);

    // ── IQuerySession delegation (read operations use parent's connection) ──

    IQueryEventStore IQuerySession.Events => Events;
    public ISerializer Serializer => _parent.Serializer;
    public string? CorrelationId { get => _parent.CorrelationId; set => _parent.CorrelationId = value; }
    public string? CausationId { get => _parent.CausationId; set => _parent.CausationId = value; }
    public string? LastModifiedBy { get => _parent.LastModifiedBy; set => _parent.LastModifiedBy = value; }
    public Dictionary<string, object>? Headers => _parent.Headers;
    public void SetHeader(string key, object value) => _parent.SetHeader(key, value);
    public object? GetHeader(string key) => _parent.GetHeader(key);

    // ── IQuerySession reads ────────────────────────────────────────────────
    //
    // polecat#548: these run on the parent's connection but MUST be scoped to _tenantId, not the
    // parent's tenant. Under conjoined tenancy the same id names a different document per tenant, so
    // a read that carries the parent's tenant answers with another tenant's row — the Polecat
    // analogue of marten#4801, and worse than it: Marten's bug was an identity-map cache collision
    // over a correctly-scoped query, whereas delegating the whole read sends the wrong tenant to the
    // database. The writes here were already tenant-correct (TenantScopedStorageSession), which is
    // what made the read side quiet: rows land under the right tenant and are read back as another's.
    //
    // The tenant-explicit storage overloads these route through also bypass the session's
    // identity-map flavor logic, so a cross-tenant read can neither be answered from nor poison the
    // parent's tenant-blind ItemMap — covering the #4801 cache shape as well as the query shape.

    public Task<DocumentMetadata?> MetadataForAsync<T>(T document, CancellationToken token = default) where T : notnull
    {
        var provider = _parent.Providers.GetProvider<T>();
        return _parent.MetadataForIdForTenantAsync(provider, provider.Mapping.GetId(document), _tenantId, token);
    }

    public Task<DocumentMetadata?> MetadataForAsync<T>(Guid id, CancellationToken token = default) where T : class
        => MetadataForTenantAsync<T>(id, token);
    public Task<DocumentMetadata?> MetadataForAsync<T>(string id, CancellationToken token = default) where T : class
        => MetadataForTenantAsync<T>(id, token);
    public Task<DocumentMetadata?> MetadataForAsync<T>(int id, CancellationToken token = default) where T : class
        => MetadataForTenantAsync<T>(id, token);
    public Task<DocumentMetadata?> MetadataForAsync<T>(long id, CancellationToken token = default) where T : class
        => MetadataForTenantAsync<T>(id, token);

    private Task<DocumentMetadata?> MetadataForTenantAsync<T>(object id, CancellationToken token) where T : class
        => _parent.MetadataForIdForTenantAsync(_parent.Providers.GetProvider<T>(), id, _tenantId, token);

    public int RequestCount => _parent.RequestCount;
    public IPolecatSessionLogger Logger { get => _parent.Logger; set => _parent.Logger = value; }
    public IAdvancedSql AdvancedSql => _parent.AdvancedSql;

    public Task<bool> CheckExistsAsync<T>(Guid id, CancellationToken token = default) where T : class
        => _parent.CheckExistsForTenantAsync<T>(id, _tenantId, token);

    public Task<bool> CheckExistsAsync<T>(string id, CancellationToken token = default) where T : class
        => _parent.CheckExistsForTenantAsync<T>(id, _tenantId, token);

    public Task<bool> CheckExistsAsync<T>(int id, CancellationToken token = default) where T : class
        => _parent.CheckExistsForTenantAsync<T>(id, _tenantId, token);

    public Task<bool> CheckExistsAsync<T>(long id, CancellationToken token = default) where T : class
        => _parent.CheckExistsForTenantAsync<T>(id, _tenantId, token);

    public Task<T?> LoadAsync<T>(Guid id, CancellationToken token = default) where T : notnull
        => _parent.LoadForTenantAsync<T>(id, _tenantId, StorageScope, token);

    public Task<T?> LoadAsync<T>(string id, CancellationToken token = default) where T : notnull
        => _parent.LoadForTenantAsync<T>(id, _tenantId, StorageScope, token);

    public Task<T?> LoadAsync<T>(int id, CancellationToken token = default) where T : class
        => _parent.LoadForTenantAsync<T>(id, _tenantId, StorageScope, token);

    public Task<T?> LoadAsync<T>(long id, CancellationToken token = default) where T : class
        => _parent.LoadForTenantAsync<T>(id, _tenantId, StorageScope, token);

    // polecat#472: the runtime-typed identity overload reduces to the same inner scalar the typed
    // overloads pass, so it takes the same tenant-scoped path (polecat#548).
    public Task<T?> LoadAsync<T>(object id, CancellationToken token = default) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(id);
        var mapping = _parent.Providers.GetProvider<T>().Mapping;
        return _parent.LoadForTenantAsync<T>(mapping.UnwrapIdentity(id, nameof(id)), _tenantId, StorageScope, token);
    }

    public Task<IReadOnlyList<T>> LoadManyAsync<T>(IEnumerable<Guid> ids, CancellationToken token = default) where T : class
        => _parent.LoadManyForTenantAsync<T>(ids.Cast<object>().ToList(), _tenantId, StorageScope, token);

    public Task<IReadOnlyList<T>> LoadManyAsync<T>(IEnumerable<string> ids, CancellationToken token = default) where T : class
        => _parent.LoadManyForTenantAsync<T>(ids.Cast<object>().ToList(), _tenantId, StorageScope, token);

    public IPolecatQueryable<T> Query<T>() where T : notnull
        => _parent.QueryForTenant<T>(_tenantId);

    public IBatchedQuery CreateBatchQuery()
        => _parent.CreateBatchQuery();

    public string ToSql<T>(IQueryable<T> queryable) where T : class
        => _parent.ToSql(queryable);

    public Task<T> QueryByPlanAsync<T>(IQueryPlan<T> plan, CancellationToken token = default)
        => _parent.QueryByPlanAsync(plan, token);

    public Task<string?> LoadJsonAsync<T>(Guid id, CancellationToken token = default) where T : class
        => _parent.LoadJsonForTenantAsync<T>(id, _tenantId, token);

    public Task<string?> LoadJsonAsync<T>(string id, CancellationToken token = default) where T : class
        => _parent.LoadJsonForTenantAsync<T>(id, _tenantId, token);

    public Task<string?> LoadJsonAsync<T>(int id, CancellationToken token = default) where T : class
        => _parent.LoadJsonForTenantAsync<T>(id, _tenantId, token);

    public Task<string?> LoadJsonAsync<T>(long id, CancellationToken token = default) where T : class
        => _parent.LoadJsonForTenantAsync<T>(id, _tenantId, token);

    // ── IDocumentOperations (mutations flow through the closed-shape layer with the
    //    override tenant; #273 E2e. Deletions carry _tenantId explicitly; writes bind
    //    single-tenant metadata via a TenantScopedStorageSession at flush time.) ──

    public void Store<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _parent.Providers.GetProvider<T>();
        _parent.WorkTracker.Add(_parent.BuildClosedShapeWrite(document, provider,
            DocumentSessionBase.WriteKind.Upsert, StorageScope));
    }

    public void Store<T>(params T[] documents) where T : notnull
    {
        foreach (var doc in documents) Store(doc);
    }

    public void StoreObjects(IEnumerable<object> documents)
    {
        foreach (var document in documents)
        {
            if (document is null) continue;

            SyncMetadata(document);
            var provider = _parent.Providers.GetProvider(document.GetType());
            _parent.WorkTracker.Add(_parent.BuildClosedShapeObjectWrite(document, provider, StorageScope));
        }
    }

    public void Insert<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _parent.Providers.GetProvider<T>();
        _parent.WorkTracker.Add(_parent.BuildClosedShapeWrite(document, provider,
            DocumentSessionBase.WriteKind.Insert, StorageScope));
    }

    public void Update<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _parent.Providers.GetProvider<T>();
        _parent.WorkTracker.Add(_parent.BuildClosedShapeWrite(document, provider,
            DocumentSessionBase.WriteKind.Update, StorageScope));
    }

    public void Delete<T>(T document) where T : notnull
    {
        Weasel.Storage.IStorageSession session = StorageScope;
        var storage = (Weasel.Storage.IDocumentStorage<T>)session.StorageFor<T>();
        var provider = _parent.Providers.GetProvider<T>();
        _parent.WorkTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.DeleteForDocument(document, _tenantId), session, document, provider.Mapping.GetId(document)));

        if (document is ISoftDeleted softDeleted && provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            softDeleted.Deleted = true;
            softDeleted.DeletedAt = DateTimeOffset.UtcNow;
        }
    }

    public void Delete<T>(Guid id) where T : notnull => DeleteByObjectId<T>(id);

    public void Delete<T>(string id) where T : notnull => DeleteByObjectId<T>(id);

    public void Delete<T>(int id) where T : class => DeleteByObjectId<T>(id);

    public void Delete<T>(long id) where T : class => DeleteByObjectId<T>(id);

    private void DeleteByObjectId<T>(object id) where T : notnull
    {
        Weasel.Storage.IStorageSession session = StorageScope;
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectStorage<T>)session.StorageFor<T>();
        _parent.WorkTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.DeletionForObjectId(id, _tenantId), session, id, id));
    }

    public void HardDelete<T>(T document) where T : notnull
    {
        Weasel.Storage.IStorageSession session = StorageScope;
        var storage = (Weasel.Storage.IDocumentStorage<T>)session.StorageFor<T>();
        _parent.WorkTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.HardDeleteForDocument(document, _tenantId), session, document,
            _parent.Providers.GetProvider<T>().Mapping.GetId(document)));
    }

    public void HardDelete<T>(Guid id) where T : notnull => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(string id) where T : notnull => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(int id) where T : class => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(long id) where T : class => HardDeleteByObjectId<T>(id);

    private void HardDeleteByObjectId<T>(object id) where T : notnull
    {
        Weasel.Storage.IStorageSession session = StorageScope;
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectStorage<T>)session.StorageFor<T>();
        _parent.WorkTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.HardDeletionForObjectId(id, _tenantId), session, id, id));
    }

    public void DeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : notnull
        => _parent.DeleteWhere(predicate);

    public void HardDeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : notnull
        => _parent.HardDeleteWhere(predicate);

    public void UndoDeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : notnull
        => _parent.UndoDeleteWhere(predicate);

    public ITenantOperations ForTenant(string tenantId)
        => _parent.ForTenant(tenantId);

    public void UpdateExpectedVersion<T>(T document, Guid version) where T : notnull
        => _parent.UpdateExpectedVersion(document, version);

    public void UpdateRevision<T>(T document, int revision) where T : notnull
        => _parent.UpdateRevision(document, revision);

    public void UpdateRevision<T>(T document, long revision) where T : notnull
        => _parent.UpdateRevision(document, revision);

    public void QueueSqlCommand(string sql, params object[] parameterValues)
        => _parent.QueueSqlCommand(sql, parameterValues);

    public ValueTask DisposeAsync()
    {
        // No-op: parent owns the connection
        return ValueTask.CompletedTask;
    }

    private void SyncMetadata<T>(T document)
    {
        if (document is ITenanted tenanted)
        {
            tenanted.TenantId = _tenantId;
        }
    }
}
