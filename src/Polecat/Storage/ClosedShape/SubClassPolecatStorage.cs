using Weasel.Core.SqlGeneration;
using Weasel.Storage;
using Polecat.Internal;

namespace Polecat.Storage.ClosedShape;

/// <summary>
///     Storage for a registered sub-class of a document hierarchy (#273 E2e). Since #959 the forwarding to the
///     hierarchy root's storage is Weasel's <see cref="ClosedShapeSubClassDocumentStorage{TDoc,TRoot,TId}" />
///     (weasel#811), the wrapper Marten, Polecat and Fisher each used to write; the <c>doc_type</c>
///     discriminator is <see cref="PolecatSubClassRules{T,TRoot,TId}" />. What stays here are the four
///     Polecat-only seams, which the root implements and a sub-class narrows to its own rows.
/// </summary>
internal sealed class SubClassPolecatStorage<T, TRoot, TId>
    : ClosedShapeSubClassDocumentStorage<T, TRoot, TId>, IPolecatObjectStorage<T>, IPolecatBatchLoadStorage<T>,
        IPolecatBulkVersionCheckStorage<T>, IPolecatDeletionStorage
    where T : notnull, TRoot
    where TRoot : notnull
    where TId : notnull
{
    private readonly string _alias;

    public SubClassPolecatStorage(IDocumentStorage<TRoot, TId> parent, DocumentMapping mapping)
        : this(parent, new PolecatSubClassRules<T, TRoot, TId>(mapping))
    {
    }

    private SubClassPolecatStorage(IDocumentStorage<TRoot, TId> parent, PolecatSubClassRules<T, TRoot, TId> rules)
        : base(parent, rules)
    {
        _alias = rules.Alias;
    }

    private IPolecatObjectStorage<TRoot> ParentBridge => (IPolecatObjectStorage<TRoot>)Root;
    private IPolecatDeletionStorage ParentDeletion => (IPolecatDeletionStorage)Root;

    // ---- IPolecatDeletionStorage: the root's table, so the root's fragments ----

    public IOperationFragment? UndeleteFragment => ParentDeletion.UndeleteFragment;
    public bool IsConjoined => ParentDeletion.IsConjoined;
    public bool IsSoftDeleted => ParentDeletion.IsSoftDeleted;

    // ---- batched-query read seam (#273 doc-side convergence) ----
    // Delegate the SELECT + id/tenant/soft-delete filters to the root storage, then constrain
    // to this subclass with a doc_type discriminator so the casting selector cannot see a row
    // of a sibling subclass.

    private IPolecatBatchLoadStorage<TRoot> BatchParent => (IPolecatBatchLoadStorage<TRoot>)Root;

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
        => ((IPolecatBulkVersionCheckStorage<TRoot>)Root)
            .ConfigureVersionCheckedUpsert(builder, document, expectedVersion, tenantId);

    public object BulkRawId(T document)
        => ((IPolecatBulkVersionCheckStorage<TRoot>)Root).BulkRawId(document);

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

    public void StoreObject(IStorageSession session, object document, Guid? version)
        => ParentBridge.StoreObject(session, document, version);

    public Guid? MappedVersionForObject(object document) => ParentBridge.MappedVersionForObject(document);
    public long? MappedRevisionForObject(object document) => ParentBridge.MappedRevisionForObject(document);

    public Weasel.Storage.IStorageOperation UpsertObject(object document, IStorageSession session, string tenantId)
        => ParentBridge.UpsertObject(document, session, tenantId);

    public Weasel.Storage.IStorageOperation UpsertObjectProjected(object document, string tenantId)
        => ParentBridge.UpsertObjectProjected(document, tenantId);

    public IDeletion HardDeletionForObjectId(object id, string tenantId)
        => ParentBridge.HardDeletionForObjectId(id, tenantId);

    public IDeletion HardDeletionForDocument(object document, string tenantId)
        => ParentBridge.HardDeletionForDocument(document, tenantId);

    public void ClearTrackedVersions(IStorageSession session, object id)
        => ParentBridge.ClearTrackedVersions(session, id);
}

/// <summary>
///     #959: the <c>doc_type</c> discriminator for one sub-class of a hierarchy, as Weasel's
///     <see cref="IClosedShapeSubClassRules{TDoc,TRoot,TId}" />.
/// </summary>
/// <remarks>
///     <see cref="FilterDocuments" /> and <see cref="DefaultWhereFragment" /> are overridden rather than
///     left on Weasel's defaults so the filter keeps Polecat's <see cref="AllOfFilter" /> shape, which the
///     LINQ layer walks as an <c>ICompoundFragment</c>. <see cref="TruncateSql" /> deletes only this
///     sub-class's rows; Weasel's default would empty the whole hierarchy's table.
/// </remarks>
internal sealed class PolecatSubClassRules<T, TRoot, TId> : IClosedShapeSubClassRules<T, TRoot, TId>
    where T : notnull, TRoot
    where TRoot : notnull
    where TId : notnull
{
    private readonly string _qualifiedTableName;

    public PolecatSubClassRules(DocumentMapping rootMapping)
    {
        Alias = rootMapping.AliasFor(typeof(T));
        _qualifiedTableName = rootMapping.QualifiedTableName;
        DocumentTypeFilter = new HardCodedFilter($"d.doc_type = {SqlEscaping.Literal(Alias)}");
    }

    public string Alias { get; }

    public ISqlFragment DocumentTypeFilter { get; }

    public ISqlFragment FilterDocuments(IDocumentStorage<TRoot, TId> root, ISqlFragment query, IStorageSession session)
        => new AllOfFilter(new[] { root.FilterDocuments(query, session), DocumentTypeFilter });

    public ISqlFragment? DefaultWhereFragment(IDocumentStorage<TRoot, TId> root)
        => root.DefaultWhereFragment() is { } rootWhere
            ? new AllOfFilter(new[] { rootWhere, DocumentTypeFilter })
            : DocumentTypeFilter;

    public string? TruncateSql => $"DELETE FROM {_qualifiedTableName} WHERE doc_type = {SqlEscaping.Literal(Alias)}";
}
