using JasperFx.MultiTenancy;

namespace Polecat.Metadata;

/// <summary>
///     Implement this interface on a document type to automatically have
///     the TenantId set from the session on save and from the tenant_id
///     column on load. Inherits the <see cref="IHasTenantId.TenantId"/>
///     contract from the canonical <see cref="JasperFx.MultiTenancy"/>
///     marker — Polecat-side framework code accepting <c>IHasTenantId</c>
///     accepts any <c>ITenanted</c> document type. Per the dedup audit
///     row JasperFx/jasperfx#224 (multi-tenancy slice).
/// </summary>
public interface ITenanted : IHasTenantId
{
}
