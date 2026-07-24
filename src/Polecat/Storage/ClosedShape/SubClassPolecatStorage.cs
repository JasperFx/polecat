using System.Data.Common;
using JasperFx;
using Weasel.Core;
using Weasel.Core.SqlGeneration;
using Weasel.Storage;

namespace Polecat.Storage.ClosedShape;

/// <summary>
///     Closed-shape storage for a registered subclass in a document hierarchy — the Polecat
///     analog of Marten's <c>SubClassDocumentStorage</c> (#273 phase E2e). Every operation
///     delegates to the parent (hierarchy root) storage, which owns the table, the descriptor,
///     and the hierarchical selectors; loads downcast the materialized root-typed document to
///     the subclass (a row holding a different subclass yields null / is filtered out, matching
///     Marten's semantics). One wrapper instance exists per closed-shape flavor, wrapping the
///     parent provider's corresponding flavor.
/// </summary>
internal sealed class SubClassPolecatStorage<T, TRoot, TId>
    : IDocumentStorage<T, TId>, IPolecatObjectStorage<T>, IPolecatBatchLoadStorage<T>,
        IPolecatBulkVersionCheckStorage<T>, IPolecatDeletionStorage
    where T : notnull, TRoot
    where TRoot : notnull
    where TId : notnull
{
    private readonly IDocumentStorage<TRoot, TId> _parent;
    private readonly DocumentMapping _mapping;
    private readonly string _alias;

    public SubClassPolecatStorage(IDocumentStorage<TRoot, TId> parent, DocumentMapping mapping)
    {
        _parent = parent;
        _mapping = mapping;
        _alias = mapping.AliasFor(typeof(T));
    }

    private IPolecatObjectStorage<TRoot> ParentBridge => (IPolecatObjectStorage<TRoot>)_parent;

    // ---- identity ----

    public Type SourceType => typeof(TRoot);
    public Type IdType => typeof(TId);
    public Type DocumentType => typeof(T);
    public Type SelectedType => typeof(T);

    public TId Identity(T document) => _parent.Identity(document);

    public object IdentityFor(T document) => _parent.IdentityFor(document);

    public TId AssignIdentity(T document, string tenantId, IStorageDatabase database)
        => _parent.AssignIdentity(document, tenantId, database);

    public void SetIdentity(T document, TId identity) => _parent.SetIdentity(document, identity);

    public object RawIdentityValue(object id) => _parent.RawIdentityValue(id);

    public object RawIdentityValue(TId id) => _parent.RawIdentityValue(id);

    public void SetIdentityFromString(T document, string identityString)
        => _parent.SetIdentityFromString(document, identityString);

    public void SetIdentityFromGuid(T document, Guid identityGuid)
        => _parent.SetIdentityFromGuid(document, identityGuid);

    // ---- storage facts (all the parent's — the subclass shares the root's table + modes) ----

    public bool UseOptimisticConcurrency => _parent.UseOptimisticConcurrency;
    public bool UseNumericRevisions => _parent.UseNumericRevisions;
    public TenancyStyle TenancyStyle => _parent.TenancyStyle;
    public DbObjectName TableName => _parent.TableName;
    public IOperationFragment DeleteFragment => _parent.DeleteFragment;
    public IOperationFragment HardDeleteFragment => _parent.HardDeleteFragment;
    public IOperationFragment UndeleteFragment => ((IPolecatDeletionStorage)_parent).UndeleteFragment;
    public bool IsConjoined => ((IPolecatDeletionStorage)_parent).IsConjoined;
    public IReadOnlyList<IDuplicatedField> DuplicatedFields => _parent.DuplicatedFields;

    // ---- select clause ----

    public string FromObject => _mapping.QualifiedTableName;
    public string[] SelectFields() => _parent.SelectFields();

    public void Apply(Weasel.Core.ICommandBuilder builder) => _parent.Apply(builder);

    public ISelector BuildSelector(IStorageSession session)
        => new CastingSelector<T, TRoot>((ISelector<TRoot>)_parent.BuildSelector(session));

    public ISqlFragment FilterDocuments(ISqlFragment query, IStorageSession session)
        => new AllOfFilter(new[] { _parent.FilterDocuments(query, session), DocTypeFilter() });

    public ISqlFragment? DefaultWhereFragment()
    {
        var docType = DocTypeFilter();
        return _parent.DefaultWhereFragment() is { } parentWhere
            ? new AllOfFilter(new[] { parentWhere, docType })
            : docType;
    }

    private HardCodedFilter DocTypeFilter()
        => new($"d.doc_type = '{_alias.Replace("'", "''")}'");

    public ISqlFragment ByIdFilter(TId id) => _parent.ByIdFilter(id);

    // ---- load paths (delegate to the parent's hierarchical selectors, downcast) ----

    public DbCommand BuildLoadCommand(TId id, string tenantId) => _parent.BuildLoadCommand(id, tenantId);

    public DbCommand BuildLoadManyCommand(TId[] ids, string tenantId) => _parent.BuildLoadManyCommand(ids, tenantId);

    public async Task<T?> LoadAsync(TId id, IStorageSession session, CancellationToken token)
    {
        var doc = await _parent.LoadAsync(id, session, token).ConfigureAwait(false);
        return doc is T subclass ? subclass : default;
    }

    public async Task<IReadOnlyList<T>> LoadManyAsync(TId[] ids, IStorageSession session, CancellationToken token)
        => (await _parent.LoadManyAsync(ids, session, token).ConfigureAwait(false)).OfType<T>().ToList();

    public async Task<T?> LoadProjectedAsync(TId id, IStorageDatabase database, string tenantId,
        CancellationToken token)
    {
        var doc = await _parent.LoadProjectedAsync(id, database, tenantId, token).ConfigureAwait(false);
        return doc is T subclass ? subclass : default;
    }

    public async Task<IReadOnlyList<T>> LoadManyProjectedAsync(TId[] ids, IStorageDatabase database, string tenantId,
        CancellationToken token)
        => (await _parent.LoadManyProjectedAsync(ids, database, tenantId, token).ConfigureAwait(false))
            .OfType<T>().ToList();

    // ---- session bookkeeping ----

    public void Store(IStorageSession session, T document) => _parent.Store(session, document);

    public void Store(IStorageSession session, T document, Guid? version) => _parent.Store(session, document, version);

    public void Store(IStorageSession session, T document, long revision) => _parent.Store(session, document, revision);

    public Guid? VersionFor(T document, IStorageSession session) => _parent.VersionFor(document, session);

    public void Eject(IStorageSession session, T document) => _parent.Eject(session, document);

    public void EjectById(IStorageSession session, object id) => _parent.EjectById(session, id);

    public void RemoveDirtyTracker(IStorageSession session, object id) => _parent.RemoveDirtyTracker(session, id);

    // ---- write operations ----

    public Weasel.Storage.IStorageOperation Insert(T document, IStorageSession session, string tenantId)
        => _parent.Insert(document, session, tenantId);

    public Weasel.Storage.IStorageOperation Update(T document, IStorageSession session, string tenantId)
        => _parent.Update(document, session, tenantId);

    public Weasel.Storage.IStorageOperation Upsert(T document, IStorageSession session, string tenantId)
        => _parent.Upsert(document, session, tenantId);

    public Weasel.Storage.IStorageOperation Overwrite(T document, IStorageSession session, string tenantId)
        => _parent.Overwrite(document, session, tenantId);

    public Weasel.Storage.IStorageOperation OverwriteProjected(T document, string tenantId)
        => _parent.OverwriteProjected(document, tenantId);

    public Weasel.Storage.IStorageOperation UpsertProjected(T document, string tenantId)
        => _parent.UpsertProjected(document, tenantId);

    public Weasel.Storage.IStorageOperation InsertProjected(T document, string tenantId)
        => _parent.InsertProjected(document, tenantId);

    public Weasel.Storage.IStorageOperation UpdateProjected(T document, string tenantId)
        => _parent.UpdateProjected(document, tenantId);

    // ---- deletions ----

    public IDeletion DeleteForDocument(T document, string tenantId) => _parent.DeleteForDocument(document, tenantId);

    public IDeletion HardDeleteForDocument(T document, string tenantId)
        => _parent.HardDeleteForDocument(document, tenantId);

    public IDeletion DeleteForId(TId id, string tenantId) => _parent.DeleteForId(id, tenantId);

    public IDeletion HardDeleteForId(TId id, string tenantId) => _parent.HardDeleteForId(id, tenantId);

    public Task TruncateDocumentStorageAsync(IStorageDatabase database, CancellationToken ct = default)
        => database.RunSqlAsync(
            $"DELETE FROM {_mapping.QualifiedTableName} WHERE doc_type = '{_alias.Replace("'", "''")}'", ct);

    // ---- batched-query read seam (#273 doc-side convergence) ----
    // Delegate the SELECT + id/tenant/soft-delete filters to the root storage, then constrain
    // to this subclass with a doc_type discriminator so the casting selector cannot see a row
    // of a sibling subclass.

    private IPolecatBatchLoadStorage<TRoot> BatchParent => (IPolecatBatchLoadStorage<TRoot>)_parent;

    public void WriteLoadByIdSql(Weasel.SqlServer.ICommandBuilder builder, object id, string tenantId)
    {
        BatchParent.WriteLoadByIdSql(builder, id, tenantId);
        AppendDocTypeFilter(builder);
    }

    public void WriteLoadManySql(Weasel.SqlServer.ICommandBuilder builder, IReadOnlyList<object> ids, string tenantId)
    {
        BatchParent.WriteLoadManySql(builder, ids, tenantId);
        AppendDocTypeFilter(builder);
    }

    private void AppendDocTypeFilter(Weasel.SqlServer.ICommandBuilder builder)
    {
        builder.Append(" AND doc_type = ");
        builder.AppendParameter(_alias, System.Data.SqlDbType.VarChar);
    }

    public ISelector<T> BuildLoadSelector(IStorageSession session) => (ISelector<T>)BuildSelector(session);

    // The root storage owns the descriptor + write binders (incl. the doc_type binder, whose
    // GetBulkValue resolves the alias from the document's runtime type), so bulk version-checked
    // upserts for a subclass delegate straight through with the subclass instance.
    public void ConfigureVersionCheckedUpsert(Weasel.SqlServer.ICommandBuilder builder, T document,
        long expectedVersion, string tenantId)
        => ((IPolecatBulkVersionCheckStorage<TRoot>)_parent)
            .ConfigureVersionCheckedUpsert(builder, document, expectedVersion, tenantId);

    public object BulkRawId(T document)
        => ((IPolecatBulkVersionCheckStorage<TRoot>)_parent).BulkRawId(document);

    // ---- IPolecatObjectStorage bridge ----

    public async Task<T?> LoadByObjectIdAsync(object id, IStorageSession session, CancellationToken token)
    {
        var doc = await ParentBridge.LoadByObjectIdAsync(id, session, token).ConfigureAwait(false);
        return doc is T subclass ? subclass : default;
    }

    public async Task<IReadOnlyList<T>> LoadManyByObjectIdsAsync(IReadOnlyList<object> ids, IStorageSession session,
        CancellationToken token)
        => (await ParentBridge.LoadManyByObjectIdsAsync(ids, session, token).ConfigureAwait(false))
            .OfType<T>().ToList();

    public IDeletion DeletionForObjectId(object id, string tenantId) => ParentBridge.DeletionForObjectId(id, tenantId);

    public async Task<T?> LoadByObjectIdAsync(object id, IStorageSession session, string tenantId,
        CancellationToken token)
    {
        var doc = await ParentBridge.LoadByObjectIdAsync(id, session, tenantId, token).ConfigureAwait(false);
        return doc is T subclass ? subclass : default;
    }

    public async Task<IReadOnlyList<T>> LoadManyByObjectIdsAsync(IReadOnlyList<object> ids, IStorageSession session,
        string tenantId, CancellationToken token)
        => (await ParentBridge.LoadManyByObjectIdsAsync(ids, session, tenantId, token).ConfigureAwait(false))
            .OfType<T>().ToList();

    public void StoreObject(IStorageSession session, object document) => ParentBridge.StoreObject(session, document);

    public Weasel.Storage.IStorageOperation UpsertObject(object document, IStorageSession session, string tenantId)
        => ParentBridge.UpsertObject(document, session, tenantId);

    public Weasel.Storage.IStorageOperation UpsertObjectProjected(object document, string tenantId)
        => ParentBridge.UpsertObjectProjected(document, tenantId);

    public IDeletion HardDeletionForObjectId(object id, string tenantId)
        => ParentBridge.HardDeletionForObjectId(id, tenantId);

    public IDeletion HardDeletionForDocument(object document, string tenantId)
        => ParentBridge.HardDeletionForDocument(document, tenantId);
}

/// <summary>
///     Downcasts a root-typed selector's rows to the subclass type. The LINQ subclass path
///     always constrains rows with a <c>doc_type</c> filter, so the cast cannot fail there.
/// </summary>
internal sealed class CastingSelector<T, TRoot> : ISelector<T>
    where T : TRoot
    where TRoot : notnull
{
    private readonly ISelector<TRoot> _inner;

    public CastingSelector(ISelector<TRoot> inner) => _inner = inner;

    public T Resolve(DbDataReader reader) => (T)_inner.Resolve(reader)!;

    public async Task<T> ResolveAsync(DbDataReader reader, CancellationToken token)
        => (T)(await _inner.ResolveAsync(reader, token).ConfigureAwait(false))!;
}
