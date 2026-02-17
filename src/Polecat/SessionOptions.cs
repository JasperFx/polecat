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
    public string TenantId { get; set; } = Tenancy.DefaultTenantId;

    /// <summary>
    ///     Override the transaction isolation level for this session.
    /// </summary>
    public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

    /// <summary>
    ///     Command timeout in seconds for this session. Null uses the store default.
    /// </summary>
    public int? Timeout { get; set; }
}
