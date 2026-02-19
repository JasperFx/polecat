namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     Generates "tenant_id IN (@p0, @p1, ...)" for TenantIsOneOf queries.
/// </summary>
internal class TenantInFilter : ISqlFragment
{
    private readonly string[] _tenantIds;

    public TenantInFilter(string[] tenantIds)
    {
        _tenantIds = tenantIds;
    }

    public void Apply(CommandBuilder builder)
    {
        if (_tenantIds.Length == 0)
        {
            builder.Append("1=0");
            return;
        }

        builder.Append("tenant_id IN (");
        for (var i = 0; i < _tenantIds.Length; i++)
        {
            if (i > 0) builder.Append(", ");
            builder.AppendParameter(_tenantIds[i]);
        }

        builder.Append(")");
    }
}
