namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     A raw SQL WHERE fragment (e.g., "column IS NULL").
/// </summary>
internal class WhereFragment : ISqlFragment
{
    private readonly string _sql;

    public WhereFragment(string sql)
    {
        _sql = sql;
    }

    public void Apply(CommandBuilder builder)
    {
        builder.Append(_sql);
    }
}
