namespace Polecat.Exceptions;

/// <summary>
///     Thrown when a session or projection daemon is created against the default tenant while
///     <see cref="StoreOptions.DefaultTenantUsageEnabled" /> is disabled — which is the automatic
///     state once a database-per-tenant tenancy is configured. Mirrors Marten's exception of the
///     same name. polecat#514.
/// </summary>
public class DefaultTenantUsageDisabledException : Exception
{
    public DefaultTenantUsageDisabledException()
        : base(
            $"Default tenant {JasperFx.StorageConstants.DefaultTenantId} usage is disabled. Ensure to create a session by explicitly passing a non-default tenant in the method arg or SessionOptions.")
    {
    }

    public DefaultTenantUsageDisabledException(string message)
        : base($"Default tenant {JasperFx.StorageConstants.DefaultTenantId} usage is disabled. {message}")
    {
    }
}
