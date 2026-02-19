namespace Polecat.Metadata;

/// <summary>
///     Implement this interface on a document type to enable soft deletes
///     and have Polecat automatically set Deleted and DeletedAt in memory
///     when the document is deleted.
/// </summary>
public interface ISoftDeleted
{
    /// <summary>
    ///     Whether this document has been soft-deleted.
    /// </summary>
    bool Deleted { get; set; }

    /// <summary>
    ///     When this document was soft-deleted, if applicable.
    /// </summary>
    DateTimeOffset? DeletedAt { get; set; }
}
