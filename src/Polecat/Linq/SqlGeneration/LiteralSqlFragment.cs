namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     A raw SQL fragment that is appended directly to the command builder without parameterization.
/// </summary>
internal class LiteralSqlFragment : ISqlFragment
{
    private readonly string _sql;

    public LiteralSqlFragment(string sql)
    {
        _sql = sql;
    }

    public void Apply(CommandBuilder builder)
    {
        builder.Append(_sql);
    }
}
