using JasperFx.Events;
using Polecat.Internal;
using Weasel.Core;
using IStorageOperation = Polecat.Internal.IStorageOperation;

namespace Polecat.Services;

/// <summary>
///     Immutable <see cref="IChangeSet" /> snapshot derived from a list of storage operations plus the
///     stream actions in a unit of work. Used both as the return value of
///     <see cref="IWorkTracker.Clone" /> (a session snapshot taken before the work tracker is reset)
///     and directly by the async daemon to describe a committed projection batch.
///     Mirrors Marten deriving Inserted/Updated/Deleted from each operation's <c>Role()</c>.
/// </summary>
internal sealed class ChangeSet : IChangeSet
{
    private readonly IReadOnlyList<IStorageOperation> _operations;
    private readonly IReadOnlyList<StreamAction> _streams;

    public ChangeSet(IReadOnlyList<IStorageOperation> operations, IReadOnlyList<StreamAction> streams)
    {
        _operations = operations;
        _streams = streams;
    }

    public IEnumerable<object> Updated => UpdatedFrom(_operations);
    public IEnumerable<object> Inserted => InsertedFrom(_operations);
    public IEnumerable<IDeletion> Deleted => DeletedFrom(_operations);

    public IEnumerable<IEvent> GetEvents() => _streams.SelectMany(x => x.Events);
    public IEnumerable<StreamAction> GetStreams() => _streams;

    // Already an immutable snapshot, so cloning is a no-op copy of the same backing lists.
    public IChangeSet Clone() => new ChangeSet(_operations, _streams);

    internal static IEnumerable<object> UpdatedFrom(IEnumerable<IStorageOperation> operations)
        => operations.OfType<IDocumentStorageOperation>()
            .Where(x => x.Role() is OperationRole.Update or OperationRole.Upsert)
            .Select(x => x.Document);

    internal static IEnumerable<object> InsertedFrom(IEnumerable<IStorageOperation> operations)
        => operations.OfType<IDocumentStorageOperation>()
            .Where(x => x.Role() == OperationRole.Insert)
            .Select(x => x.Document);

    internal static IEnumerable<IDeletion> DeletedFrom(IEnumerable<IStorageOperation> operations)
        => operations
            .Where(x => x.Role() == OperationRole.Deletion)
            .Select(x => (IDeletion)new Deletion(x.DocumentType, x.DocumentId));
}

/// <summary>
///     Default <see cref="IDeletion" /> record carrying the deleted document's type and id.
/// </summary>
internal sealed record Deletion(Type DocumentType, object? Id) : IDeletion;
