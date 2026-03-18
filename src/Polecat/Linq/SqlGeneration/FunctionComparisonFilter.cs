using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     A comparison filter that wraps the locator in a SQL function.
///     E.g., "LOWER(locator) = @p0" or "LEN(locator) > @p0".
/// </summary>
internal class FunctionComparisonFilter : ISqlFragment
{
    private readonly string _function;
    private readonly string _locator;
    private readonly string _op;
    private readonly object _value;

    public FunctionComparisonFilter(string function, string locator, string op, object value)
    {
        _function = function;
        _locator = locator;
        _op = op;
        _value = value;
    }

    public void Apply(ICommandBuilder builder)
    {
        builder.Append(_function);
        builder.Append("(");
        builder.Append(_locator);
        builder.Append(") ");
        builder.Append(_op);
        builder.Append(" ");
        builder.AppendParameter(_value);
    }
}
