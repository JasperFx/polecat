namespace Polecat.Metadata;

/// <summary>
///     Implement this interface on a document type to enable Guid-based
///     optimistic concurrency. Polecat will check the Version on save
///     and throw ConcurrencyException if it has changed since load.
/// </summary>
public interface IVersioned
{
    /// <summary>
    ///     The current version of this document. Updated automatically on save.
    /// </summary>
    Guid Version { get; set; }
}
