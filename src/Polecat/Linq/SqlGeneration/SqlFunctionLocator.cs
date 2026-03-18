using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     Represents a SQL function wrapping a locator (e.g., LOWER(locator)).
///     Used as a marker in the expression tree so the parent comparison
///     can generate the correct SQL.
/// </summary>
internal class SqlFunctionLocator : ISqlFragment
{
    public string Function { get; }
    public string InnerLocator { get; }

    public SqlFunctionLocator(string function, string innerLocator)
    {
        Function = function;
        InnerLocator = innerLocator;
    }

    /// <summary>
    ///     The full SQL expression (e.g., "LOWER(JSON_VALUE(data, '$.name'))").
    /// </summary>
    public string FullLocator => $"{Function}({InnerLocator})";

    public void Apply(ICommandBuilder builder)
    {
        builder.Append(FullLocator);
    }
}
