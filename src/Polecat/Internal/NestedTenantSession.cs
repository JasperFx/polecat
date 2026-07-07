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

    public NestedTenantSession(DocumentSessionBase parent, string tenantId)
    {
        _parent = parent;
        _tenantId = tenantId;
    }

    public string TenantId => _tenantId;
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

    public Task<DocumentMetadata?> MetadataForAsync<T>(T document, CancellationToken token = default) where T : notnull
        => _parent.MetadataForAsync(document, token);
    public Task<DocumentMetadata?> MetadataForAsync<T>(Guid id, CancellationToken token = default) where T : class
        => _parent.MetadataForAsync<T>(id, token);
    public Task<DocumentMetadata?> MetadataForAsync<T>(string id, CancellationToken token = default) where T : class
        => _parent.MetadataForAsync<T>(id, token);
    public Task<DocumentMetadata?> MetadataForAsync<T>(int id, CancellationToken token = default) where T : class
        => _parent.MetadataForAsync<T>(id, token);
    public Task<DocumentMetadata?> MetadataForAsync<T>(long id, CancellationToken token = default) where T : class
        => _parent.MetadataForAsync<T>(id, token);

    public int RequestCount => _parent.RequestCount;
    public IPolecatSessionLogger Logger { get => _parent.Logger; set => _parent.Logger = value; }
    public IAdvancedSql AdvancedSql => _parent.AdvancedSql;

    public Task<bool> CheckExistsAsync<T>(Guid id, CancellationToken token = default) where T : class
        => _parent.CheckExistsAsync<T>(id, token);

    public Task<bool> CheckExistsAsync<T>(string id, CancellationToken token = default) where T : class
        => _parent.CheckExistsAsync<T>(id, token);

    public Task<bool> CheckExistsAsync<T>(int id, CancellationToken token = default) where T : class
        => _parent.CheckExistsAsync<T>(id, token);

    public Task<bool> CheckExistsAsync<T>(long id, CancellationToken token = default) where T : class
        => _parent.CheckExistsAsync<T>(id, token);

    public Task<T?> LoadAsync<T>(Guid id, CancellationToken token = default) where T : class
        => _parent.LoadAsync<T>(id, token);

    public Task<T?> LoadAsync<T>(string id, CancellationToken token = default) where T : class
        => _parent.LoadAsync<T>(id, token);

    public Task<T?> LoadAsync<T>(int id, CancellationToken token = default) where T : class
        => _parent.LoadAsync<T>(id, token);

    public Task<T?> LoadAsync<T>(long id, CancellationToken token = default) where T : class
        => _parent.LoadAsync<T>(id, token);

    public Task<IReadOnlyList<T>> LoadManyAsync<T>(IEnumerable<Guid> ids, CancellationToken token = default) where T : class
        => _parent.LoadManyAsync<T>(ids, token);

    public Task<IReadOnlyList<T>> LoadManyAsync<T>(IEnumerable<string> ids, CancellationToken token = default) where T : class
        => _parent.LoadManyAsync<T>(ids, token);

    public IPolecatQueryable<T> Query<T>() where T : class
        => _parent.Query<T>();

    public IBatchedQuery CreateBatchQuery()
        => _parent.CreateBatchQuery();

    public string ToSql<T>(IQueryable<T> queryable) where T : class
        => _parent.ToSql(queryable);

    public Task<T> QueryByPlanAsync<T>(IQueryPlan<T> plan, CancellationToken token = default)
        => _parent.QueryByPlanAsync(plan, token);

    public Task<string?> LoadJsonAsync<T>(Guid id, CancellationToken token = default) where T : class
        => _parent.LoadJsonAsync<T>(id, token);

    public Task<string?> LoadJsonAsync<T>(string id, CancellationToken token = default) where T : class
        => _parent.LoadJsonAsync<T>(id, token);

    public Task<string?> LoadJsonAsync<T>(int id, CancellationToken token = default) where T : class
        => _parent.LoadJsonAsync<T>(id, token);

    public Task<string?> LoadJsonAsync<T>(long id, CancellationToken token = default) where T : class
        => _parent.LoadJsonAsync<T>(id, token);

    // ── IDocumentOperations (mutations flow through the closed-shape layer with the
    //    override tenant; #273 E2e. Deletions carry _tenantId explicitly; writes bind
    //    single-tenant metadata via a TenantScopedStorageSession at flush time.) ──

    public void Store<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _parent.Providers.GetProvider<T>();
        _parent.WorkTracker.Add(_parent.BuildClosedShapeWrite(document, provider,
            DocumentSessionBase.WriteKind.Upsert, _tenantId));
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
            _parent.WorkTracker.Add(_parent.BuildClosedShapeObjectWrite(document, provider, _tenantId));
        }
    }

    public void Insert<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _parent.Providers.GetProvider<T>();
        _parent.WorkTracker.Add(_parent.BuildClosedShapeWrite(document, provider,
            DocumentSessionBase.WriteKind.Insert, _tenantId));
    }

    public void Update<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _parent.Providers.GetProvider<T>();
        _parent.WorkTracker.Add(_parent.BuildClosedShapeWrite(document, provider,
            DocumentSessionBase.WriteKind.Update, _tenantId));
    }

    public void Delete<T>(T document) where T : notnull
    {
        var session = (Weasel.Storage.IStorageSession)_parent;
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

    public void Delete<T>(Guid id) where T : class => DeleteByObjectId<T>(id);

    public void Delete<T>(string id) where T : class => DeleteByObjectId<T>(id);

    public void Delete<T>(int id) where T : class => DeleteByObjectId<T>(id);

    public void Delete<T>(long id) where T : class => DeleteByObjectId<T>(id);

    private void DeleteByObjectId<T>(object id) where T : class
    {
        var session = (Weasel.Storage.IStorageSession)_parent;
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectStorage<T>)session.StorageFor<T>();
        _parent.WorkTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.DeletionForObjectId(id, _tenantId), session, id, id));
    }

    public void HardDelete<T>(T document) where T : notnull
    {
        var session = (Weasel.Storage.IStorageSession)_parent;
        var storage = (Weasel.Storage.IDocumentStorage<T>)session.StorageFor<T>();
        _parent.WorkTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.HardDeleteForDocument(document, _tenantId), session, document,
            _parent.Providers.GetProvider<T>().Mapping.GetId(document)));
    }

    public void HardDelete<T>(Guid id) where T : class => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(string id) where T : class => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(int id) where T : class => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(long id) where T : class => HardDeleteByObjectId<T>(id);

    private void HardDeleteByObjectId<T>(object id) where T : class
    {
        var session = (Weasel.Storage.IStorageSession)_parent;
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectStorage<T>)session.StorageFor<T>();
        _parent.WorkTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.HardDeletionForObjectId(id, _tenantId), session, id, id));
    }

    public void DeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : class
        => _parent.DeleteWhere(predicate);

    public void HardDeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : class
        => _parent.HardDeleteWhere(predicate);

    public void UndoDeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : class
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
