using System.Data.Common;
using Microsoft.Data.SqlClient;
using Polecat.Serialization;
using Polecat.Storage;
using Weasel.Storage;

namespace Polecat.Internal;

// #273: Polecat sessions implement the dialect-neutral Weasel.Storage.IStorageSession —
// the operation/session context of the shared closed-shape storage runtime (weasel#329/#331).
// The members the shared runtime can use today map onto Polecat's existing session internals;
// StorageFor stays guarded until Polecat's document storage retargets onto the shared
// IDocumentStorage bases (phases D/E), which is also when the bespoke identity map and
// document-held versions migrate onto ItemMap / Versions as the authoritative stores.
internal partial class QuerySession : IStorageSession
{
    private IStorageSerializer? _storageSerializer;
    private PolecatVersionTracker? _versionTracker;
    private List<IChangeTracker>? _changeTrackers;
    private Dictionary<Type, object>? _itemMap;
    private int _tempTableNumber;

    IStorageSerializer IStorageSession.Serializer => _storageSerializer ??= StorageSerializerAdapter.For(Serializer);

    IStorageDatabase IStorageSession.Database =>
        Options.StorageDatabase ?? throw new InvalidOperationException(
            "StoreOptions.StorageDatabase has not been wired; sessions must be created through DocumentStore.");

    IVersionTracker IStorageSession.Versions => _versionTracker ??= new PolecatVersionTracker();

    /// <summary>
    ///     Polecat has no dirty tracking by design (Lightweight and IdentityMap sessions only),
    ///     so no change trackers are ever registered. The shared runtime iterates an empty list.
    /// </summary>
    IList<IChangeTracker> IStorageSession.ChangeTrackers => _changeTrackers ??= new List<IChangeTracker>();

    /// <summary>
    ///     The closed-shape identity map (Dictionary&lt;TId, TDoc&gt; per document type, boxed).
    ///     Owned by the shared runtime's selectors; Polecat's bespoke identity map remains the
    ///     authoritative store until phase E unifies them.
    /// </summary>
    Dictionary<Type, object> IStorageSession.ItemMap => _itemMap ??= new Dictionary<Type, object>();

    ConcurrencyChecks IStorageSession.Concurrency => ConcurrencyChecks.Enabled;

    // #273 phase E1: closed-shape storage resolution. The flavor tracks the session kind —
    // QuerySession -> QueryOnly, DocumentSessionBase (lightweight) -> Lightweight,
    // IdentityMapDocumentSession -> IdentityMap.

    IDocumentStorage IStorageSession.StorageFor(Type documentType)
        => (IDocumentStorage)typeof(QuerySession)
            .GetMethod(nameof(ClosedShapeStorageFor), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .MakeGenericMethod(documentType)
            .Invoke(this, null)!;

    IDocumentStorage<T> IStorageSession.StorageFor<T>() => ClosedShapeStorageFor<T>();

    private IDocumentStorage<T> ClosedShapeStorageFor<T>() where T : notnull
        => SelectClosedShapeStorage(Providers.ClosedShapeGraph.StorageFor<T>());

    internal virtual IDocumentStorage<T> SelectClosedShapeStorage<T>(Weasel.Storage.DocumentProvider<T> provider)
        where T : notnull
        => provider.QueryOnly;

    /// <summary>
    ///     Identity-map hook for documents newly queued for storage. No-op outside identity-map
    ///     sessions; <see cref="IdentityMapDocumentSession" /> overrides.
    /// </summary>
    public virtual void MarkAsAddedForStorage(object id, object document)
    {
    }

    /// <summary>
    ///     Identity-map hook for documents loaded from the database. No-op outside identity-map
    ///     sessions; <see cref="IdentityMapDocumentSession" /> overrides.
    /// </summary>
    public virtual void MarkAsDocumentLoaded(object id, object document)
    {
    }

    /// <summary>
    ///     Db-neutral execution seam of the shared runtime. Delegates to Polecat's existing
    ///     resilience-pipeline-wrapped reader execution; Polecat sessions only speak SqlClient.
    /// </summary>
    public Task<DbDataReader> ExecuteReaderAsync(DbCommand command, CancellationToken token = default)
    {
        if (command is not SqlCommand sqlCommand)
        {
            throw new ArgumentException(
                $"Polecat sessions execute Microsoft.Data.SqlClient commands; got {command.GetType().FullName}.",
                nameof(command));
        }

        return ExecuteReaderAsync(sqlCommand, token);
    }

    public string NextTempTableName()
    {
        return $"#pc_temp_{++_tempTableNumber}";
    }

    // ---- IMetadataContext (the base of IStorageSession) ----
    // TenantId / CorrelationId / CausationId / Headers already live on QuerySession.

    /// <summary>
    ///     The shared-runtime name for the session's user metadata; same state as
    ///     <see cref="LastModifiedBy" />.
    /// </summary>
    public string? CurrentUserName
    {
        get => LastModifiedBy;
        set => LastModifiedBy = value;
    }

    public bool CorrelationIdEnabled => Options.Events.EnableCorrelationId;
    public bool CausationIdEnabled => Options.Events.EnableCausationId;
    public bool HeadersEnabled => Options.Events.EnableHeaders;
    public bool UserNameEnabled => Options.Events.EnableUserName;
}
