using System.Linq.Expressions;

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
    void Store<T>(T document) where T : notnull;

    /// <summary>
    ///     Store multiple documents.
    /// </summary>
    void Store<T>(params T[] documents) where T : notnull;

    /// <summary>
    ///     Insert a document. Throws if a document with the same id already exists.
    /// </summary>
    void Insert<T>(T document) where T : notnull;

    /// <summary>
    ///     Update an existing document. Throws if the document does not exist.
    /// </summary>
    void Update<T>(T document) where T : notnull;

    /// <summary>
    ///     Delete a document by entity. For soft-deleted types, marks as deleted.
    /// </summary>
    void Delete<T>(T document) where T : notnull;

    /// <summary>
    ///     Delete a document by its Guid id. For soft-deleted types, marks as deleted.
    /// </summary>
    void Delete<T>(Guid id) where T : class;

    /// <summary>
    ///     Delete a document by its string id. For soft-deleted types, marks as deleted.
    /// </summary>
    void Delete<T>(string id) where T : class;

    /// <summary>
    ///     Delete a document by its int id. For soft-deleted types, marks as deleted.
    /// </summary>
    void Delete<T>(int id) where T : class;

    /// <summary>
    ///     Delete a document by its long id. For soft-deleted types, marks as deleted.
    /// </summary>
    void Delete<T>(long id) where T : class;

    /// <summary>
    ///     Permanently remove a document by entity, regardless of soft-delete configuration.
    /// </summary>
    void HardDelete<T>(T document) where T : notnull;

    /// <summary>
    ///     Permanently remove a document by its Guid id, regardless of soft-delete configuration.
    /// </summary>
    void HardDelete<T>(Guid id) where T : class;

    /// <summary>
    ///     Permanently remove a document by its string id, regardless of soft-delete configuration.
    /// </summary>
    void HardDelete<T>(string id) where T : class;

    /// <summary>
    ///     Permanently remove a document by its int id, regardless of soft-delete configuration.
    /// </summary>
    void HardDelete<T>(int id) where T : class;

    /// <summary>
    ///     Permanently remove a document by its long id, regardless of soft-delete configuration.
    /// </summary>
    void HardDelete<T>(long id) where T : class;

    /// <summary>
    ///     Reverse a soft delete for documents matching the given predicate.
    ///     Sets is_deleted = 0 and deleted_at = NULL.
    /// </summary>
    void UndoDeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : class;
}
