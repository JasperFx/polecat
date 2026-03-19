using Polecat.Storage;

namespace Polecat.Attributes;

/// <summary>
///     Marks a property to be included in a unique computed index.
///     Multiple properties with the same IndexName form a composite unique index.
/// </summary>
[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
public class UniqueIndexAttribute : PolecatAttribute
{
    /// <summary>
    ///     Optional explicit name for the index. Properties with the same IndexName
    ///     are grouped into a single composite unique index.
    /// </summary>
    public string? IndexName { get; set; }

    /// <summary>
    ///     Tenancy scope for the unique index.
    /// </summary>
    public TenancyScope TenancyScope { get; set; } = TenancyScope.Global;

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
