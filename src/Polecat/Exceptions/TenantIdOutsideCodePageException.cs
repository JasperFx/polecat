using Polecat.Internal;

namespace Polecat.Exceptions;

/// <summary>
///     Thrown instead of running a session for a tenant id that the <c>varchar</c> <c>tenant_id</c> columns
///     cannot hold. Under the default collation <c>東京</c> and <c>大阪</c> are both <c>??</c>, so the
///     alternative to this exception is two tenants that are one: one reading the other's documents (#922).
///     The sibling of <see cref="StringIdOutsideCodePageException" />.
/// </summary>
public class TenantIdOutsideCodePageException : Exception
{
    internal TenantIdOutsideCodePageException(string tenantId, int codePage) : base(
        $"The tenant id '{tenantId}' can't be used. tenant_id is varchar, and code page {codePage} can't "
        + "represent it, so SQL Server would keep a different value with no error. Two different tenants "
        + "could then become one, and each would read and overwrite the other's documents and streams. "
        + "Use a tenant id within that code page.")
    {
        TenantId = tenantId;
        CodePage = codePage;
    }

    /// <summary>
    ///     The tenant id as the application gave it.
    /// </summary>
    public string TenantId { get; }

    /// <summary>
    ///     The code page of the column that could not hold it.
    /// </summary>
    public int CodePage { get; }
}
