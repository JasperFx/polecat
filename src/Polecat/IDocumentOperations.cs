namespace Polecat;

/// <summary>
///     Extends IQuerySession with document mutation operations.
///     Operations are queued and not executed until SaveChangesAsync is called.
/// </summary>
public interface IDocumentOperations : IQuerySession
{
    /// <summary>
    ///     Store (insert or update) a document.
    /// </summary>
    void Store<T>(T document) where T : class;

    /// <summary>
    ///     Store multiple documents.
    /// </summary>
    void Store<T>(params T[] documents) where T : class;

    /// <summary>
    ///     Insert a document. Throws if a document with the same id already exists.
    /// </summary>
    void Insert<T>(T document) where T : class;

    /// <summary>
    ///     Update an existing document. Throws if the document does not exist.
    /// </summary>
    void Update<T>(T document) where T : class;

    /// <summary>
    ///     Delete a document by entity.
    /// </summary>
    void Delete<T>(T document) where T : class;

    /// <summary>
    ///     Delete a document by its Guid id.
    /// </summary>
    void Delete<T>(Guid id) where T : class;

    /// <summary>
    ///     Delete a document by its string id.
    /// </summary>
    void Delete<T>(string id) where T : class;
}
