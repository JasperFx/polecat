namespace Polecat.Metadata;

/// <summary>
///     Controls how documents are deleted.
/// </summary>
public enum DeleteStyle
{
    /// <summary>
    ///     Hard delete: physically removes the row from the database.
    /// </summary>
    Remove,

    /// <summary>
    ///     Soft delete: marks the row as deleted without removing it.
    /// </summary>
    SoftDelete
}
