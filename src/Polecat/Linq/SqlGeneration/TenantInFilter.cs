using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     Generates "tenant_id IN (@p0, @p1, ...)" for TenantIsOneOf queries.
/// </summary>
internal class TenantInFilter : ISqlFragment
{
    private readonly string[] _tenantIds;
    private readonly string _columnName;

    public TenantInFilter(string[] tenantIds) : this(tenantIds, "tenant_id")
    {
    }

    public TenantInFilter(string[] tenantIds, string columnName)
    {
        _tenantIds = tenantIds;
        _columnName = columnName;
    }

    public void Apply(ICommandBuilder builder)
    {
        if (_tenantIds.Length == 0)
        {
            builder.Append("1=0");
            return;
        }

        builder.Append(_columnName);
        builder.Append(" IN (");
        for (var i = 0; i < _tenantIds.Length; i++)
        {
            if (i > 0) builder.Append(", ");
            builder.AppendParameter(_tenantIds[i], System.Data.SqlDbType.VarChar);
        }

        builder.Append(")");
    }
}
