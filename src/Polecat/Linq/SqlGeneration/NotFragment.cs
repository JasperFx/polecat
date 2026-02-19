namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     Negates an inner SQL fragment: NOT (inner).
/// </summary>
internal class NotFragment : ISqlFragment
{
    private readonly ISqlFragment _inner;

    public NotFragment(ISqlFragment inner)
    {
        _inner = inner;
    }

    public void Apply(CommandBuilder builder)
    {
        builder.Append("NOT (");
        _inner.Apply(builder);
        builder.Append(")");
    }
}
