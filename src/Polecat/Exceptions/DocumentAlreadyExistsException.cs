namespace Polecat.Exceptions;

/// <summary>
///     Thrown when attempting to Insert a document with an id that already exists.
/// </summary>
public class DocumentAlreadyExistsException : Exception
{
    public DocumentAlreadyExistsException(Type documentType, object id)
        : base($"A document of type '{documentType.Name}' with id '{id}' already exists.")
    {
        DocumentType = documentType;
        Id = id;
    }

    public Type DocumentType { get; }
    public object Id { get; }
}
