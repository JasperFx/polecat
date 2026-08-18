using System.Data;

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
