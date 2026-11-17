using Microsoft.Data.SqlClient;

namespace Polecat.Exceptions;

/// <summary>
///     #935: a write refused by SQL Server because the row's key is longer than an index can hold (Msg
///     1946): 900 bytes in a clustered key, 1,700 in a nonclustered one. Under
///     <see cref="AdvancedOptions.UseUnicodeKeys" /> a key column holds two bytes a character, so some keys
///     are declared wider than that, and a row is refused only when its own key passes the limit. Keep the
///     string columns of a key, a tenant id and a string id say, under 450 characters together.
/// </summary>
public class KeyTooLongException : Exception
{
    internal const int ErrorNumber = 1946;

    internal KeyTooLongException(string? tenantId, object? id, SqlException inner) : base(
        $"SQL Server refused a write because its key is too long for an index{describe(tenantId, id)}. "
        + "With StoreOptions.Advanced.UseUnicodeKeys every character of a key column takes two bytes, and "
        + "SQL Server allows 900 bytes in a clustered key, so the string columns of one key (a tenant id and a "
        + $"string id, say) have to stay under 450 characters together. {inner.Message}", inner)
    {
        TenantId = tenantId;
        Id = id;
    }

    /// <summary>The tenant the row was written for, when Polecat could tell which write it was.</summary>
    public string? TenantId { get; }

    /// <summary>
    ///     The id, stream key or tag value the row was written under, when Polecat could tell which write it
    ///     was.
    /// </summary>
    public object? Id { get; }

    private static string describe(string? tenantId, object? id)
        => (tenantId, id) switch
        {
            (null, null) => string.Empty,
            (_, null) => $" (tenant '{tenantId}')",
            (null, _) => $" (id '{id}')",
            _ => $" (tenant '{tenantId}', id '{id}')"
        };
}
