namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     A comparison filter (e.g., "locator = @p0").
/// </summary>
internal class ComparisonFilter : ISqlFragment
{
    private readonly string _locator;
    private readonly string _op;
    private readonly object _value;

    public ComparisonFilter(string locator, string op, object value)
    {
        _locator = locator;
        _op = op;
        _value = value;
    }

    public void Apply(CommandBuilder builder)
    {
        builder.Append(_locator);
        builder.Append(" ");
        builder.Append(_op);
        builder.Append(" ");
        builder.AppendParameter(_value);
    }
}
