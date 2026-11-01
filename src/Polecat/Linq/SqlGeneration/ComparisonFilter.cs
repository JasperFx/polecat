using System.Data;
using Weasel.SqlServer;

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

    public void Apply(ICommandBuilder builder)
    {
        builder.Append(_locator);
        builder.Append(" ");
        builder.Append(_op);
        builder.Append(" ");
        AppendValue(builder, _value);
    }

    /// <summary>
    ///     Binds a comparison value. A bare <see cref="DateTime" /> gets an explicit <c>datetime2</c>:
    ///     left to itself SqlClient infers <c>datetime</c>, which rounds to 3ms, so a comparison against
    ///     a <c>datetime2</c> locator is made against a different instant (#838).
    /// </summary>
    internal static void AppendValue(ICommandBuilder builder, object value)
    {
        if (value is DateTime)
        {
            builder.AppendParameter(value, SqlDbType.DateTime2);
        }
        else
        {
            builder.AppendParameter(value);
        }
    }
}
