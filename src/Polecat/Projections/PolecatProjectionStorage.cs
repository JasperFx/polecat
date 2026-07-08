using System.Diagnostics.CodeAnalysis;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Internal;
using Polecat.Internal.Operations;
using Polecat.Metadata;

namespace Polecat.Projections;

/// <summary>
///     Adapts Polecat's document session into the IProjectionStorage interface
///     that JasperFx.Events uses to persist projected documents.
///     All SQL execution routes through session's Polly-wrapped centralized methods.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: persists projected TDoc via ISerializer.ToJson and deserializes loaded snapshots via ISerializer.FromJson. TDoc/TId flow in from projection registration on the caller side and are preserved per the AOT publishing guide; AOT consumers supply a source-generator-backed ISerializer impl.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer is annotated RDC. AOT consumers supply a source-generator-backed impl.")]
internal class PolecatProjectionStorage<TDoc, TId> : IProjectionStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    private readonly DocumentSessionBase _session;
    private readonly DocumentProvider _provider;
    private bool _tableEnsured;

    public PolecatProjectionStorage(DocumentSessionBase session, DocumentProvider provider, string tenantId)
    {
        _session = session;
        _provider = provider;
        TenantId = tenantId;
    }

    private async Task EnsureTableExistsAsync(CancellationToken cancellation)
    {
        if (_tableEnsured) return;
        await _session.EnsureTableForProviderAsync(_provider, cancellation);
        _tableEnsured = true;
    }

    public string TenantId { get; }

    public void SetIdentity(TDoc document, TId identity)
    {
        _provider.Mapping.SetId(document, identity!);
    }

    public TId Identity(TDoc document)
    {
        var rawId = _provider.Mapping.GetId(document);

        // If TId is the inner type (e.g., Guid/string) but GetId returned the unwrapped value, cast directly
        if (rawId is TId typedId)
        {
            return typedId;
        }

        // If TId is a wrapper type (e.g., PaymentId) and GetId returned the inner value,
        // wrap it back up
        if (_provider.Mapping.ValueTypeId != null)
        {
            object wrapped;
            if (_provider.Mapping.ValueTypeId.Ctor != null)
            {
                wrapped = _provider.Mapping.ValueTypeId.Ctor.Invoke([rawId]);
            }
            else
            {
                wrapped = _provider.Mapping.ValueTypeId.Builder!.Invoke(null, [rawId])!;
            }

            return (TId)wrapped;
        }

        return (TId)rawId;
    }

    /// <summary>
    ///     Unwrap a strongly-typed ID to its inner value for use as a SQL parameter.
    ///     If the ID is not a value type wrapper, returns it unchanged.
    /// </summary>
    private object UnwrapId(TId id)
    {
        if (_provider.Mapping.ValueTypeId != null)
        {
            // Only unwrap if id is actually the wrapper type (not already the inner type)
            var wrapperType = Nullable.GetUnderlyingType(_provider.Mapping.IdType) ?? _provider.Mapping.IdType;
            if (id.GetType() == wrapperType)
            {
                return _provider.Mapping.ValueTypeId.ValueProperty.GetValue(id)!;
            }
        }

        return id;
    }

    // #273 E2e: projection writes flow through the closed-shape storage layer's session-free
    // *Projected operations (no session version-tracker reads — safe for parallel daemon
    // slice handlers). The adapter carries a tenant-scoped session when this storage serves
    // a different tenant than the driving session, so flush-time metadata binders write the
    // projection's tenant.

    // QueryOnly flavor deliberately: its ops are identical, and its selector reads no id
    // column and captures no session version/identity-map state — matching the bespoke
    // projection reads (and avoiding the writeable selectors' typed id read, which cannot
    // materialize Polecat's varchar-stored strongly-typed id columns).
    private Polecat.Storage.ClosedShape.IPolecatObjectStorage<TDoc> Storage
        => (Polecat.Storage.ClosedShape.IPolecatObjectStorage<TDoc>)
            _session.Providers.ClosedShapeGraph.StorageFor<TDoc>().QueryOnly;

    private Weasel.Storage.IStorageSession SessionFor(string tenantId)
    {
        Weasel.Storage.IStorageSession session = _session;
        return tenantId == _session.TenantId ? session : new TenantScopedStorageSession(session, tenantId);
    }

    private void StoreProjected(TDoc snapshot, string tenantId)
    {
        // Bespoke parity: assign an id when missing (aggregations normally set it from the slice).
        if (_provider.SequenceSource is not null)
        {
            _provider.Mapping.AssignIdIfMissing(snapshot, _provider.SequenceSource);
        }

        var op = ((Weasel.Storage.IDocumentStorage<TDoc>)Storage).UpsertProjected(snapshot, tenantId);
        DocumentSessionBase.CaptureExpectedRevision(op, snapshot);
        _session.WorkTracker.Add(new ClosedShapeOperationAdapter(
            op, SessionFor(tenantId), snapshot, _provider.Mapping.GetId(snapshot)));
    }

    public void Store(TDoc snapshot) => StoreProjected(snapshot, TenantId);

    public void Store(TDoc document, TId id, string tenantId)
    {
        SetIdentity(document, id);
        StoreProjected(document, tenantId);
    }

    public void Delete(TId identity) => Delete(identity, TenantId);

    public void Delete(TId identity, string tenantId)
    {
        _session.WorkTracker.Add(new ClosedShapeOperationAdapter(
            Storage.DeletionForObjectId(identity!, tenantId), SessionFor(tenantId), identity!, identity));
    }

    public void HardDelete(TDoc snapshot) => HardDelete(snapshot, TenantId);

    public void HardDelete(TDoc snapshot, string tenantId)
    {
        var id = _provider.Mapping.GetId(snapshot);
        _session.WorkTracker.Add(new ClosedShapeOperationAdapter(
            Storage.HardDeletionForObjectId(id, tenantId), SessionFor(tenantId), snapshot, id));
    }

    public void UnDelete(TDoc snapshot) => UnDelete(snapshot, TenantId);

    public void UnDelete(TDoc snapshot, string tenantId)
    {
        if (_provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            // #273 doc-side convergence: undelete SQL + tenancy from the shared closed-shape storage.
            var deletion = (Storage.ClosedShape.IPolecatDeletionStorage)Storage;
            var id = _provider.Mapping.GetId(snapshot);
            _session.WorkTracker.Add(new UnDeleteByIdOperation(
                deletion.UndeleteFragment, deletion.IsConjoined, id, tenantId, typeof(TDoc)));
        }
    }

    // #273 E2e: projection loads route through the closed-shape storage layer with this
    // storage's (possibly session-differing) tenant id; the object-id bridge normalizes
    // raw inner values vs strongly-typed wrappers.

    public async Task<TDoc> LoadAsync(TId id, CancellationToken cancellation)
    {
        await EnsureTableExistsAsync(cancellation);
        var doc = await Storage.LoadByObjectIdAsync(id, (Weasel.Storage.IStorageSession)_session, TenantId,
            cancellation);
        return doc ?? default!;
    }

    public async Task<IReadOnlyDictionary<TId, TDoc>> LoadManyAsync(TId[] identities,
        CancellationToken cancellationToken)
    {
        var dict = new Dictionary<TId, TDoc>();
        if (identities.Length == 0) return dict;

        await EnsureTableExistsAsync(cancellationToken);

        var ids = new object[identities.Length];
        for (var i = 0; i < identities.Length; i++)
        {
            ids[i] = identities[i];
        }

        var docs = await Storage.LoadManyByObjectIdsAsync(ids, (Weasel.Storage.IStorageSession)_session, TenantId,
            cancellationToken);
        foreach (var doc in docs)
        {
            dict[Identity(doc)] = doc;
        }

        return dict;
    }

    public void StoreProjection(TDoc aggregate, IEvent? lastEvent, AggregationScope scope)
    {
        Store(aggregate);
    }

    public void ArchiveStream(TId sliceId, string tenantId)
    {
        // Stream archiving not supported yet
    }
}
