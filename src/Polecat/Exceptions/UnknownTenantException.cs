namespace Polecat.Exceptions;

/// <summary>
///     Thrown when attempting to resolve a tenant that has not been registered
///     in a separate database tenancy configuration.
/// </summary>
public class UnknownTenantException : Exception
{
    public UnknownTenantException(string tenantId)
        : base($"Unknown tenant id '{tenantId}'. Register tenants via StoreOptions.MultiTenantedDatabases().")
    {
        TenantId = tenantId;
    }

    public string TenantId { get; }
}
