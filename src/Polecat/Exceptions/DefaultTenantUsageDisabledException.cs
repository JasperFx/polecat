namespace Polecat.Exceptions;

/// <summary>
///     Thrown when a session or projection daemon is created against the default tenant while
///     <see cref="StoreOptions.DefaultTenantUsageEnabled" /> is disabled — which is the automatic
///     state once a database-per-tenant tenancy is configured. polecat#514.
/// </summary>
/// <remarks>
///     jasperfx#751 lifted this into
///     <see cref="JasperFx.Events.DefaultTenantUsageDisabledException" /> — the Marten and Polecat
///     copies were byte-identically messaged, prefix-appending constructor included. Polecat keeps
///     the name in its own namespace so existing <c>catch</c> sites go on compiling, and derives
///     from the shared type so a store-agnostic caller catches it too.
/// </remarks>
public class DefaultTenantUsageDisabledException : JasperFx.Events.DefaultTenantUsageDisabledException
{
    public DefaultTenantUsageDisabledException()
    {
    }

    public DefaultTenantUsageDisabledException(string message) : base(message)
    {
    }
}
