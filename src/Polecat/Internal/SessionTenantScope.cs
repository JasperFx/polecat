using Polecat.Linq.SqlGeneration;
using Polecat.Storage;

namespace Polecat.Internal;

/// <summary>
///     Which tenants a query session reads for conjoined documents and events when the query does not say
///     itself (polecat#1048, port of marten#6011 and marten#6070). Only
///     <see cref="AdvancedOperations.QueryAcrossAllDatabasesAsync{T}(System.Func{IQuerySession,System.Threading.CancellationToken,System.Threading.Tasks.Task{System.Collections.Generic.IReadOnlyList{T}}},int,System.Threading.CancellationToken)" />
///     sets one: every tenant, or a fixed list of tenants. Without a scope a session reads its own tenant.
///     An <c>AnyTenant()</c> or <c>TenantIsOneOf()</c> in the query still wins.
/// </summary>
internal sealed class SessionTenantScope
{
    public static readonly SessionTenantScope AllTenants = new(null);

    private SessionTenantScope(string[]? tenantIds)
    {
        TenantIds = tenantIds;
    }

    public static SessionTenantScope For(string[] tenantIds) => new(tenantIds);

    /// <summary>
    ///     Null for every tenant
    /// </summary>
    public string[]? TenantIds { get; }

    /// <summary>
    ///     The filter that stands in for the session's own tenant filter, or null for every tenant.
    /// </summary>
    public ISqlFragment? BuildFilter(string column, bool unicode)
        => TenantIds == null ? null : new TenantInFilter(TenantIds, column, unicode);

    /// <summary>
    ///     For hand-written SQL that would otherwise say <c>AND tenant_id = @tenant_id</c>: the predicate to use
    ///     instead, with its parameters added to <paramref name="command" />. Empty for every tenant.
    /// </summary>
    public string SqlFilter(Microsoft.Data.SqlClient.SqlCommand command, string column, bool unicode)
    {
        if (TenantIds == null) return "";

        var names = new string[TenantIds.Length];
        for (var i = 0; i < TenantIds.Length; i++)
        {
            names[i] = $"@scope_tenant{i}";
            command.Parameters.AddKey(names[i], TenantIds[i], unicode);
        }

        return $" AND {column} IN ({string.Join(", ", names)})";
    }
}
