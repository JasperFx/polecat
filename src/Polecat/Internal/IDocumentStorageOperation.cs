namespace Polecat.Internal;

/// <summary>
///     A storage operation that carries the document instance it will persist. Implemented by the
///     insert/update/upsert operations so an <see cref="Polecat.Services.IChangeSet" /> can surface
///     the actual documents that were written. Mirrors Marten's <c>IDocumentStorageOperation</c>.
/// </summary>
internal interface IDocumentStorageOperation : IStorageOperation
{
    object Document { get; }
}
