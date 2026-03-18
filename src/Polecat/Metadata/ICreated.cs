namespace Polecat.Metadata;

/// <summary>
///     Implement this interface on a document type to automatically have
///     the CreatedAt timestamp populated from the database's created_at column
///     when loading the document.
/// </summary>
public interface ICreated
{
    DateTimeOffset CreatedAt { get; set; }
}
