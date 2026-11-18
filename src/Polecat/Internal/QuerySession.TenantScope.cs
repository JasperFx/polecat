using Polecat.Linq;
using Polecat.Linq.SqlGeneration;
using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Internal;

internal partial class QuerySession
{
    /// <summary>
    ///     polecat#1048: whether a by-id read of <paramref name="mapping" /> goes across this session's tenant
    ///     scope rather than its own tenant.
    /// </summary>
    private bool readsAcrossTenantScope(DocumentMapping mapping, string tenantId)
        => TenantScope != null && mapping.TenancyStyle == TenancyStyle.Conjoined && tenantId == TenantId;

    /// <summary>
    ///     polecat#1048: a by-id load across this session's tenant scope. The closed-shape load SQL is fixed to
    ///     one tenant, so this goes through LINQ with an id predicate, which gives it the same tenant, soft
    ///     delete and subclass filters a query gets.
    /// </summary>
    private async Task<IReadOnlyList<T>> loadAcrossTenantScopeAsync<T>(DocumentMapping mapping,
        IReadOnlyList<object> ids, CancellationToken token) where T : notnull
    {
        var results = new List<T>();

        // Well under SQL Server's 2,100 parameters a command, with the tenant parameters beside them
        foreach (var chunk in ids.Chunk(1000))
        {
            var provider = new PolecatLinqQueryProvider(this, _providers, _tableEnsurer, new IdInFilter(mapping, chunk));
            results.AddRange(await new PolecatLinqQueryable<T>(provider).ToListAsync(token).ConfigureAwait(false));
        }

        return results;
    }

    /// <summary>
    ///     polecat#1048: the predicate a hand-written by-id read puts on <c>tenant_id</c>, adding its parameters:
    ///     the session's tenant, or its tenant scope. Empty for a table with no tenant column.
    /// </summary>
    private string tenantFilterFor(DocumentMapping mapping, string tenantId, Microsoft.Data.SqlClient.SqlCommand command)
    {
        if (mapping.TenancyStyle != TenancyStyle.Conjoined) return "";

        if (readsAcrossTenantScope(mapping, tenantId))
        {
            return TenantScope!.SqlFilter(command, "tenant_id", Options.Advanced.UseUnicodeKeys);
        }

        command.Parameters.AddKey("@tenant_id", tenantId, Options.Advanced.UseUnicodeKeys);
        return " AND tenant_id = @tenant_id";
    }

    /// <summary><c>id IN (...)</c> over raw (unwrapped) ids.</summary>
    private sealed class IdInFilter(DocumentMapping mapping, object[] ids) : ISqlFragment
    {
        public void Apply(ICommandBuilder builder)
        {
            builder.Append("id IN (");
            for (var i = 0; i < ids.Length; i++)
            {
                if (i > 0) builder.Append(", ");
                if (ids[i] is string key)
                {
                    builder.AppendParameter(key, KeyColumns.DbType(mapping.UnicodeKeys));
                }
                else
                {
                    builder.AppendParameter(ids[i]);
                }
            }

            builder.Append(")");
        }
    }
}
