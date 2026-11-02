using System.Data;
using Microsoft.Data.SqlClient;
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
    private readonly SqlDbType? _dbType;

    public ComparisonFilter(string locator, string op, object value, SqlDbType? dbType = null)
    {
        _locator = locator;
        _op = op;
        _value = value;
        _dbType = dbType;
    }

    public void Apply(ICommandBuilder builder)
    {
        builder.Append(_locator);
        builder.Append(" ");
        builder.Append(_op);
        builder.Append(" ");
        AppendValue(builder, _value, _dbType);
    }

    /// <summary>
    ///     Binds a comparison value. A bare <see cref="DateTime" /> gets an explicit <c>datetime2</c>:
    ///     left to itself SqlClient infers <c>datetime</c>, which rounds to 3ms, so a comparison against
    ///     a <c>datetime2</c> locator is made against a different instant (#838).
    /// </summary>
    internal static SqlParameter AppendValue(ICommandBuilder builder, object value, SqlDbType? dbType = null)
    {
        if (value is DateTime)
        {
            return builder.AppendParameter(value, SqlDbType.DateTime2);
        }

        return dbType == null ? builder.AppendParameter(value) : builder.AppendParameter(value, dbType.Value);
    }
}
