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

    /// <summary>What follows the call, such as a <c>COLLATE</c> clause.</summary>
    public string Suffix { get; }

    public SqlFunctionLocator(string function, string innerLocator, string suffix = "")
    {
        Function = function;
        InnerLocator = innerLocator;
        Suffix = suffix;
    }

    /// <summary>
    ///     The full SQL expression (e.g., "LOWER(JSON_VALUE(data, '$.name'))").
    /// </summary>
    public string FullLocator => $"{Function}({InnerLocator}){Suffix}";

    public void Apply(ICommandBuilder builder)
    {
        builder.Append(FullLocator);
    }
}
