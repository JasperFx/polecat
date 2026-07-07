using System.Data.Common;
using Weasel.Storage;

namespace Polecat.Internal;

/// <summary>
///     Tenant-override decorator over a session's <see cref="IStorageSession" /> surface
///     (#273 E2e). The shared closed-shape write operations bind single-tenant metadata
///     (Polecat's always-present <c>tenant_id</c> column, via <c>PolecatTenantIdBinder</c>)
///     from <see cref="IStorageSession.TenantId" /> at flush time, so nested tenant
///     operations (<c>IDocumentSession.ForTenant</c>) build their operations against this
///     wrapper to write the override tenant instead of the parent session's. Everything
///     else — version tracking, item map, execution — is the parent's shared state, because
///     nested tenant ops flush in the parent's unit of work.
/// </summary>
internal sealed class TenantScopedStorageSession : IStorageSession
{
    private readonly IStorageSession _inner;

    public TenantScopedStorageSession(IStorageSession inner, string tenantId)
    {
        _inner = inner;
        TenantId = tenantId;
    }

    public string TenantId { get; }

    // ---- IMetadataContext (session-level metadata is the parent's) ----

    public string? CausationId
    {
        get => _inner.CausationId;
        set => _inner.CausationId = value;
    }

    public string? CorrelationId
    {
        get => _inner.CorrelationId;
        set => _inner.CorrelationId = value;
    }

    public string? CurrentUserName
    {
        get => _inner.CurrentUserName;
        set => _inner.CurrentUserName = value;
    }

    public Dictionary<string, object>? Headers => _inner.Headers;

    public bool CausationIdEnabled => _inner.CausationIdEnabled;
    public bool CorrelationIdEnabled => _inner.CorrelationIdEnabled;
    public bool HeadersEnabled => _inner.HeadersEnabled;
    public bool UserNameEnabled => _inner.UserNameEnabled;

    // ---- IStorageSession (shared state and execution are the parent's) ----

    public IStorageSerializer Serializer => _inner.Serializer;
    public IStorageDatabase Database => _inner.Database;
    public IVersionTracker Versions => _inner.Versions;
    public IList<IChangeTracker> ChangeTrackers => _inner.ChangeTrackers;
    public Dictionary<Type, object> ItemMap => _inner.ItemMap;
    public ConcurrencyChecks Concurrency => _inner.Concurrency;

    public IDocumentStorage StorageFor(Type documentType) => _inner.StorageFor(documentType);

    public IDocumentStorage<T> StorageFor<T>() where T : notnull => _inner.StorageFor<T>();

    public void MarkAsAddedForStorage(object id, object document) => _inner.MarkAsAddedForStorage(id, document);

    public void MarkAsDocumentLoaded(object id, object document) => _inner.MarkAsDocumentLoaded(id, document);

    public Task<DbDataReader> ExecuteReaderAsync(DbCommand command, CancellationToken token = default)
        => _inner.ExecuteReaderAsync(command, token);

    public byte[]? TryGetCachedSerializedHeaders() => _inner.TryGetCachedSerializedHeaders();

    public string NextTempTableName() => _inner.NextTempTableName();
}
