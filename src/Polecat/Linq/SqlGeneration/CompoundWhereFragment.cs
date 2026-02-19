namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     Combines multiple WHERE fragments with AND or OR.
/// </summary>
internal class CompoundWhereFragment : ISqlFragment
{
    private readonly string _separator;
    private readonly ISqlFragment _left;
    private readonly ISqlFragment _right;

    public CompoundWhereFragment(string separator, ISqlFragment left, ISqlFragment right)
    {
        _separator = separator;
        _left = left;
        _right = right;
    }

    public void Apply(CommandBuilder builder)
    {
        builder.Append("(");
        _left.Apply(builder);
        builder.Append($" {_separator} ");
        _right.Apply(builder);
        builder.Append(")");
    }
}
