namespace Polecat.Exceptions;

/// <summary>
///     Thrown by <see cref="IDocumentOperations.ForTenant" /> for a tenant whose data is not in the database
///     the session is connected to. Mirrors Marten's exception of the same name.
/// </summary>
/// <remarks>
///     A session holds one connection and its save is one transaction on it, so a view of another tenant
///     can only ever reach the session's own database. Reaching the right one would take a second
///     transaction, and the two could not commit as one.
/// </remarks>
public class InvalidTenantForDatabaseException : Exception
{
    public InvalidTenantForDatabaseException(string tenantId, string databaseName)
        : base($"Tenant Id '{tenantId}' is not stored in the current database '{databaseName}'. A session's " +
               "work commits in one transaction on one database, so open a separate session for this tenant.")
    {
        TenantId = tenantId;
        DatabaseName = databaseName;
    }

    /// <summary>The tenant that was asked for.</summary>
    public string TenantId { get; }

    /// <summary>The database the session is connected to.</summary>
    public string DatabaseName { get; }
}
