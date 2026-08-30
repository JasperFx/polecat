using System.Data;
using Polecat.Storage;

namespace Polecat;

/// <summary>
///     Configuration options for creating a new document session.
/// </summary>
public class SessionOptions
{
    /// <summary>
    ///     Override the identity map behavior for this session.
    ///     Default is None (lightweight session).
    /// </summary>
    public DocumentTracking Tracking { get; set; } = DocumentTracking.None;

    /// <summary>
    ///     Optionally set the tenant id for this session.
    /// </summary>
    public string TenantId { get; set; } = JasperFx.StorageConstants.DefaultTenantId;

    /// <summary>
    ///     If true, this session may be opened for the default tenant even when
    ///     <see cref="StoreOptions.DefaultTenantUsageEnabled" /> is disabled on the store. Mirrors
    ///     Marten's <c>SessionOptions.AllowAnyTenant</c>. Set automatically by
    ///     <see cref="ForDatabase(PolecatDatabase)" />, which is how the async daemon opens sessions
    ///     against a tenant database it has already resolved. polecat#514.
    /// </summary>
    public bool AllowAnyTenant { get; set; }

    /// <summary>
    ///     Bind this session to an explicit database rather than routing
    ///     <see cref="TenantId" /> through the store's <see cref="ITenancy" />. Under
    ///     database-per-tenant tenancy the daemon knows the database it is working, and the events
    ///     it replays carry the default tenant id — two different coordinates, so routing by tenant
    ///     id alone cannot reach the right database. Set via <see cref="ForDatabase(PolecatDatabase)" />.
    /// </summary>
    public PolecatDatabase? Database { get; set; }

    /// <summary>
    ///     Create session options bound to <paramref name="database" /> for the default tenant.
    ///     Mirrors Marten's <c>SessionOptions.ForDatabase</c>.
    /// </summary>
    public static SessionOptions ForDatabase(PolecatDatabase database) =>
        ForDatabase(JasperFx.StorageConstants.DefaultTenantId, database);

    /// <summary>
    ///     Create session options for <paramref name="tenantId" /> bound to an explicit
    ///     <paramref name="database" />.
    /// </summary>
    public static SessionOptions ForDatabase(string tenantId, PolecatDatabase database) =>
        new()
        {
            TenantId = tenantId,
            Database = database,
            AllowAnyTenant = true,
            Tracking = DocumentTracking.None
        };

    /// <summary>
    ///     Override the transaction isolation level for this session.
    /// </summary>
    public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

    /// <summary>
    ///     Command timeout in seconds for this session. Null uses the store default.
    /// </summary>
    public int? Timeout { get; set; }

    /// <summary>
    ///     Session-specific listeners applied only to this session.
    /// </summary>
    public List<IDocumentSessionListener> Listeners { get; } = new();

    /// <summary>
    ///     Session-specific store-agnostic post-commit listeners — #485 / jasperfx#679's
    ///     <see cref="JasperFx.Events.Documents.IDocumentCommitListener" />. The per-session
    ///     counterpart to <see cref="StoreOptions.CommitListeners" />, exactly as
    ///     <see cref="Listeners" /> is to <see cref="StoreOptions.Listeners" />; both collections
    ///     run on a commit, store-level first.
    /// </summary>
    public List<JasperFx.Events.Documents.IDocumentCommitListener> CommitListeners { get; } = new();
}
