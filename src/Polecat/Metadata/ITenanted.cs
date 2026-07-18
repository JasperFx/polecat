using JasperFx.MultiTenancy;

namespace Polecat.Metadata;

/// <summary>
///     Implement this interface on a document type to automatically have
///     the TenantId set from the session on save and from the tenant_id
///     column on load. Derives from the shared
///     <see cref="JasperFx.MultiTenancy.ITenanted"/> marker (jasperfx#531)
///     so the same marker drives conjoined behavior across Marten,
///     Polecat, and Wolverine — framework code accepting
///     <c>IHasTenantId</c> accepts any <c>ITenanted</c> document type.
/// </summary>
public interface ITenanted : JasperFx.MultiTenancy.ITenanted
{
}
