using Polecat.Storage;

namespace Polecat.Attributes;

/// <summary>
///     Marks a property to be included in a computed index.
///     The index is created on the persisted computed column derived from the JSON path.
/// </summary>
[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
public class IndexAttribute : PolecatAttribute
{
    /// <summary>
    ///     Optional explicit name for the index.
    /// </summary>
    public string? IndexName { get; set; }

    /// <summary>
    ///     Sort order for the index column. Default is Ascending.
    /// </summary>
    public SortOrder SortOrder { get; set; } = SortOrder.Ascending;

    /// <summary>
    ///     Case transformation for the indexed value.
    ///     Only applies to string-typed properties; non-string properties ignore this setting.
    /// </summary>
    public IndexCasing Casing { get; set; } = IndexCasing.Default;

    /// <summary>
    ///     SQL type hint for the indexed value (e.g., "int", "varchar(500)").
    ///     Defaults to varchar(250) for string paths.
    /// </summary>
    public string? SqlType { get; set; }
}
