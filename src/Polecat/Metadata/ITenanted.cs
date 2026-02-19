namespace Polecat.Metadata;

/// <summary>
///     Implement this interface on a document type to automatically have
///     the TenantId set from the session on save and from the tenant_id
///     column on load.
/// </summary>
public interface ITenanted
{
    string TenantId { get; set; }
}
