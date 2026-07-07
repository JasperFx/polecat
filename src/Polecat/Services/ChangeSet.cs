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
    private readonly IReadOnlyList<Weasel.Storage.IStorageOperation> _operations;
    private readonly IReadOnlyList<StreamAction> _streams;

    public ChangeSet(IReadOnlyList<Weasel.Storage.IStorageOperation> operations, IReadOnlyList<StreamAction> streams)
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

    internal static IEnumerable<object> UpdatedFrom(IEnumerable<Weasel.Storage.IStorageOperation> operations)
        => operations
            .Where(x => x.Role() is OperationRole.Update or OperationRole.Upsert)
            .Select(DocumentOf)
            .Where(d => d is not null)!;

    internal static IEnumerable<object> InsertedFrom(IEnumerable<Weasel.Storage.IStorageOperation> operations)
        => operations
            .Where(x => x.Role() == OperationRole.Insert)
            .Select(DocumentOf)
            .Where(d => d is not null)!;

    internal static IEnumerable<IDeletion> DeletedFrom(IEnumerable<Weasel.Storage.IStorageOperation> operations)
        => operations
            .Where(x => x.Role() == OperationRole.Deletion)
            .Select(x => (IDeletion)new Deletion(x.DocumentType, IdentityOf(x)));

    // #273 E2e: the unit of work speaks the shared currency. Bespoke Polecat operations
    // (including the closed-shape adapter) and raw shared operations both surface their
    // document / identity here.
    private static object? DocumentOf(Weasel.Storage.IStorageOperation op)
        => op switch
        {
            IDocumentStorageOperation bespoke => bespoke.Document,
            Weasel.Storage.IDocumentStorageOperation shared => shared.Document,
            _ => null
        };

    private static object? IdentityOf(Weasel.Storage.IStorageOperation op)
        => op switch
        {
            IStorageOperation bespoke => bespoke.DocumentId,
            Weasel.Storage.IDeletion deletion => deletion.Id,
            _ => null
        };
}

/// <summary>
///     Default <see cref="IDeletion" /> record carrying the deleted document's type and id.
/// </summary>
internal sealed record Deletion(Type DocumentType, object? Id) : IDeletion;
