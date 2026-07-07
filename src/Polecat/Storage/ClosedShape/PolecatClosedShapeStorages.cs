using Weasel.Core;
using Weasel.Storage;

namespace Polecat.Storage.ClosedShape;

// #273 phase E1: the flavor/mode composition matrix over PolecatDocumentStorage, mirroring
// Marten's ClosedShape layer. Flavors differ in identity-map behavior + selector family;
// modes differ in which shared write operations they create (and which session version
// dictionaries those bind). Polecat has no dirty tracking, so the DocumentProvider's
// DirtyTracking slot is filled with the IdentityMap storage.

// ---- flavor bases ----

internal abstract class LightweightPolecatStorage<TDoc, TId> : PolecatDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    protected LightweightPolecatStorage(DocumentMapping mapping, DocumentStorageDescriptor<TDoc, TId> descriptor)
        : base(mapping, descriptor)
    {
    }

    public override Task<TDoc?> LoadAsync(TId id, IStorageSession session, CancellationToken token)
        => QueryOneAsync(id, session, token);

    public override Task<IReadOnlyList<TDoc>> LoadManyAsync(TId[] ids, IStorageSession session, CancellationToken token)
        => QueryManyAsync(ids, session, token);
}

internal abstract class IdentityMapPolecatStorage<TDoc, TId> : PolecatDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    protected IdentityMapPolecatStorage(DocumentMapping mapping, DocumentStorageDescriptor<TDoc, TId> descriptor)
        : base(mapping, descriptor)
    {
    }

    public override async Task<TDoc?> LoadAsync(TId id, IStorageSession session, CancellationToken token)
    {
        if (TryGetFromMap(session, id, out var cached))
        {
            return cached;
        }

        return await QueryOneAsync(id, session, token).ConfigureAwait(false);
    }

    public override async Task<IReadOnlyList<TDoc>> LoadManyAsync(TId[] ids, IStorageSession session,
        CancellationToken token)
    {
        var results = new List<TDoc>(ids.Length);
        var missing = new List<TId>();
        foreach (var id in ids)
        {
            if (TryGetFromMap(session, id, out var cached))
            {
                results.Add(cached);
            }
            else
            {
                missing.Add(id);
            }
        }

        if (missing.Count > 0)
        {
            results.AddRange(await QueryManyAsync(missing.ToArray(), session, token).ConfigureAwait(false));
        }

        return results;
    }

    private static bool TryGetFromMap(IStorageSession session, TId id, out TDoc document)
    {
        if (session.ItemMap.TryGetValue(typeof(TDoc), out var raw)
            && raw is Dictionary<TId, TDoc> map
            && map.TryGetValue(id, out document!))
        {
            return true;
        }

        document = default!;
        return false;
    }

    public override void Store(IStorageSession session, TDoc document)
    {
        base.Store(session, document);
        AddToMap(session, document);
    }

    public override void Store(IStorageSession session, TDoc document, Guid? version)
    {
        base.Store(session, document, version);
        AddToMap(session, document);
    }

    public override void Store(IStorageSession session, TDoc document, long revision)
    {
        base.Store(session, document, revision);
        AddToMap(session, document);
    }

    private void AddToMap(IStorageSession session, TDoc document)
    {
        var id = Identity(document);
        if (!session.ItemMap.TryGetValue(typeof(TDoc), out var raw) || raw is not Dictionary<TId, TDoc> map)
        {
            map = new Dictionary<TId, TDoc>();
            session.ItemMap[typeof(TDoc)] = map;
        }

        map[id] = document;
    }
}

// ---- Unversioned mode ----

internal sealed class UnversionedLightweightPolecatStorage<TDoc, TId> : LightweightPolecatStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public UnversionedLightweightPolecatStorage(DocumentMapping mapping,
        DocumentStorageDescriptor<TDoc, TId> descriptor) : base(mapping, descriptor)
    {
    }

    public override Weasel.Storage.IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override Weasel.Storage.IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override Weasel.Storage.IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert);

    public override Weasel.Storage.IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override Weasel.Storage.IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => new UnversionedClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override Weasel.Storage.IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => new UnversionedClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert);

    public override Weasel.Storage.IStorageOperation InsertProjected(TDoc document, string tenantId)
        => new UnversionedClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override Weasel.Storage.IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => new UnversionedClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalUnversionedClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor)
            : new FlatUnversionedClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor);
}

internal sealed class UnversionedIdentityMapPolecatStorage<TDoc, TId> : IdentityMapPolecatStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public UnversionedIdentityMapPolecatStorage(DocumentMapping mapping,
        DocumentStorageDescriptor<TDoc, TId> descriptor) : base(mapping, descriptor)
    {
    }

    public override Weasel.Storage.IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override Weasel.Storage.IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override Weasel.Storage.IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert);

    public override Weasel.Storage.IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override Weasel.Storage.IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => new UnversionedClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override Weasel.Storage.IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => new UnversionedClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert);

    public override Weasel.Storage.IStorageOperation InsertProjected(TDoc document, string tenantId)
        => new UnversionedClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override Weasel.Storage.IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => new UnversionedClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalUnversionedClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor)
            : new FlatUnversionedClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor);
}

// ---- Optimistic mode ----

internal sealed class OptimisticLightweightPolecatStorage<TDoc, TId> : LightweightPolecatStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public OptimisticLightweightPolecatStorage(DocumentMapping mapping,
        DocumentStorageDescriptor<TDoc, TId> descriptor) : base(mapping, descriptor)
    {
    }

    private Dictionary<TId, Guid> Versions(IStorageSession session) => session.Versions.ForType<TDoc, TId>();

    public override Weasel.Storage.IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Versions(session));

    public override Weasel.Storage.IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Versions(session));

    public override Weasel.Storage.IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert, Versions(session));

    public override Weasel.Storage.IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Versions(session));

    public override Weasel.Storage.IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => new OptimisticClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override Weasel.Storage.IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => new OptimisticClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert, null);

    public override Weasel.Storage.IStorageOperation InsertProjected(TDoc document, string tenantId)
        => new OptimisticClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override Weasel.Storage.IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => new OptimisticClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalOptimisticClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor)
            : new FlatOptimisticClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor);
}

internal sealed class OptimisticIdentityMapPolecatStorage<TDoc, TId> : IdentityMapPolecatStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public OptimisticIdentityMapPolecatStorage(DocumentMapping mapping,
        DocumentStorageDescriptor<TDoc, TId> descriptor) : base(mapping, descriptor)
    {
    }

    private Dictionary<TId, Guid> Versions(IStorageSession session) => session.Versions.ForType<TDoc, TId>();

    public override Weasel.Storage.IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Versions(session));

    public override Weasel.Storage.IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Versions(session));

    public override Weasel.Storage.IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert, Versions(session));

    public override Weasel.Storage.IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Versions(session));

    public override Weasel.Storage.IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => new OptimisticClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override Weasel.Storage.IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => new OptimisticClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert, null);

    public override Weasel.Storage.IStorageOperation InsertProjected(TDoc document, string tenantId)
        => new OptimisticClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override Weasel.Storage.IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => new OptimisticClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalOptimisticClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor)
            : new FlatOptimisticClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor);
}

// ---- Numeric mode ----

internal sealed class NumericLightweightPolecatStorage<TDoc, TId> : LightweightPolecatStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public NumericLightweightPolecatStorage(DocumentMapping mapping,
        DocumentStorageDescriptor<TDoc, TId> descriptor) : base(mapping, descriptor)
    {
    }

    private Dictionary<TId, long> Revisions(IStorageSession session) => session.Versions.RevisionsFor<TDoc, TId>();

    public override Weasel.Storage.IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Revisions(session));

    public override Weasel.Storage.IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Revisions(session));

    public override Weasel.Storage.IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert, Revisions(session));

    public override Weasel.Storage.IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Revisions(session));

    public override Weasel.Storage.IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => new NumericClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override Weasel.Storage.IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => new NumericClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert, null);

    public override Weasel.Storage.IStorageOperation InsertProjected(TDoc document, string tenantId)
        => new NumericClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override Weasel.Storage.IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => new NumericClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalNumericClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor)
            : new FlatNumericClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor);
}

internal sealed class NumericIdentityMapPolecatStorage<TDoc, TId> : IdentityMapPolecatStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public NumericIdentityMapPolecatStorage(DocumentMapping mapping,
        DocumentStorageDescriptor<TDoc, TId> descriptor) : base(mapping, descriptor)
    {
    }

    private Dictionary<TId, long> Revisions(IStorageSession session) => session.Versions.RevisionsFor<TDoc, TId>();

    public override Weasel.Storage.IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Revisions(session));

    public override Weasel.Storage.IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Revisions(session));

    public override Weasel.Storage.IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert, Revisions(session));

    public override Weasel.Storage.IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, Revisions(session));

    public override Weasel.Storage.IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => new NumericClosedShapeOverwriteOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override Weasel.Storage.IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => new NumericClosedShapeUpsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, OperationRole.Upsert, null);

    public override Weasel.Storage.IStorageOperation InsertProjected(TDoc document, string tenantId)
        => new NumericClosedShapeInsertOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override Weasel.Storage.IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => new NumericClosedShapeUpdateOperation<TDoc, TId>(document, Identity(document), tenantId, _descriptor, null);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalNumericClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor)
            : new FlatNumericClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor);
}

// ---- QueryOnly flavor (single class; op factories dispatch on mode) ----

internal sealed class QueryOnlyPolecatStorage<TDoc, TId> : PolecatDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public QueryOnlyPolecatStorage(DocumentMapping mapping, DocumentStorageDescriptor<TDoc, TId> descriptor)
        : base(mapping, descriptor)
    {
    }

    protected override IDocumentMetadataBinder<TDoc>[] ReadBinders() => _descriptor.QueryOnlyReadBinders;

    public override Task<TDoc?> LoadAsync(TId id, IStorageSession session, CancellationToken token)
        => QueryOneAsync(id, session, token);

    public override Task<IReadOnlyList<TDoc>> LoadManyAsync(TId[] ids, IStorageSession session, CancellationToken token)
        => QueryManyAsync(ids, session, token);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalClosedShapeQueryOnlySelector<TDoc, TId>(session, _descriptor)
            : new FlatClosedShapeQueryOnlySelector<TDoc, TId>(session, _descriptor);

    public override Weasel.Storage.IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => CreateOp(document, session, tenantId, OpKind.Insert);

    public override Weasel.Storage.IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => CreateOp(document, session, tenantId, OpKind.Update);

    public override Weasel.Storage.IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => CreateOp(document, session, tenantId, OpKind.Upsert);

    public override Weasel.Storage.IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => CreateOp(document, session, tenantId, OpKind.Overwrite);

    public override Weasel.Storage.IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => CreateOp(document, null, tenantId, OpKind.Overwrite);

    public override Weasel.Storage.IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => CreateOp(document, null, tenantId, OpKind.Upsert);

    public override Weasel.Storage.IStorageOperation InsertProjected(TDoc document, string tenantId)
        => CreateOp(document, null, tenantId, OpKind.Insert);

    public override Weasel.Storage.IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => CreateOp(document, null, tenantId, OpKind.Update);

    private enum OpKind
    {
        Insert,
        Update,
        Upsert,
        Overwrite
    }

    private Weasel.Storage.IStorageOperation CreateOp(TDoc document, IStorageSession? session, string tenantId,
        OpKind kind)
    {
        var id = Identity(document);
        switch (_descriptor.ConcurrencyMode)
        {
            case ConcurrencyMode.Optimistic:
            {
                var versions = session?.Versions.ForType<TDoc, TId>();
                return kind switch
                {
                    OpKind.Insert => new OptimisticClosedShapeInsertOperation<TDoc, TId>(document, id, tenantId, _descriptor, versions),
                    OpKind.Update => new OptimisticClosedShapeUpdateOperation<TDoc, TId>(document, id, tenantId, _descriptor, versions),
                    OpKind.Overwrite => new OptimisticClosedShapeOverwriteOperation<TDoc, TId>(document, id, tenantId, _descriptor, versions),
                    _ => new OptimisticClosedShapeUpsertOperation<TDoc, TId>(document, id, tenantId, _descriptor, OperationRole.Upsert, versions)
                };
            }
            case ConcurrencyMode.Numeric:
            {
                var revisions = session?.Versions.RevisionsFor<TDoc, TId>();
                return kind switch
                {
                    OpKind.Insert => new NumericClosedShapeInsertOperation<TDoc, TId>(document, id, tenantId, _descriptor, revisions),
                    OpKind.Update => new NumericClosedShapeUpdateOperation<TDoc, TId>(document, id, tenantId, _descriptor, revisions),
                    OpKind.Overwrite => new NumericClosedShapeOverwriteOperation<TDoc, TId>(document, id, tenantId, _descriptor, revisions),
                    _ => new NumericClosedShapeUpsertOperation<TDoc, TId>(document, id, tenantId, _descriptor, OperationRole.Upsert, revisions)
                };
            }
            default:
                return kind switch
                {
                    OpKind.Insert => new UnversionedClosedShapeInsertOperation<TDoc, TId>(document, id, tenantId, _descriptor),
                    OpKind.Update => new UnversionedClosedShapeUpdateOperation<TDoc, TId>(document, id, tenantId, _descriptor),
                    OpKind.Overwrite => new UnversionedClosedShapeOverwriteOperation<TDoc, TId>(document, id, tenantId, _descriptor),
                    _ => new UnversionedClosedShapeUpsertOperation<TDoc, TId>(document, id, tenantId, _descriptor, OperationRole.Upsert)
                };
        }
    }
}
