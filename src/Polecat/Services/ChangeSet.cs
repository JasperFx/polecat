using JasperFx.Events;
using JasperFx.Events.Documents;
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

    // #485 / jasperfx#679: materialised ONCE, in the constructor, rather than left as the lazy LINQ
    // chains these used to be. Two reasons, and the first is correctness rather than speed:
    //
    //   (a) IDocumentChangeSet promises SNAPSHOTS -- IReadOnlyList and not IEnumerable -- because a
    //       listener may retain the change set past the commit boundary. A lazy chain is only as
    //       stable as the list it reads, and PolecatProjectionBatch constructs a change set over a
    //       plain List<T> it has been accumulating, not over an immutable snapshot.
    //   (b) A listener that read Inserted/Updated/Deleted re-ran the whole chain, once per property
    //       per access, on a path that has just done its SQL round trips. Marten's IChangeSet has
    //       the same shape and the same cost.
    //
    // Partitioned in a SINGLE pass rather than by three Where() chains, so this is cheaper than the
    // lazy version for any listener that reads more than one property and costs one traversal for a
    // store with no listeners at all. The public IEnumerable-typed members below hand back these
    // same lists, so nothing walks the operations twice.
    private readonly IReadOnlyList<object> _updated;
    private readonly IReadOnlyList<object> _inserted;
    private readonly IReadOnlyList<IDeletion> _deleted;

    public ChangeSet(IReadOnlyList<Weasel.Storage.IStorageOperation> operations, IReadOnlyList<StreamAction> streams)
    {
        _operations = operations;
        _streams = streams;

        List<object>? updated = null;
        List<object>? inserted = null;
        List<IDeletion>? deleted = null;

        // foreach rather than an indexed loop: WorkTracker.Operations hands over an ImmutableList,
        // whose indexer is O(log n).
        foreach (var operation in operations)
        {
            switch (operation.Role())
            {
                case OperationRole.Update or OperationRole.Upsert:
                    if (DocumentOf(operation) is { } document) (updated ??= []).Add(document);
                    break;

                case OperationRole.Insert:
                    if (DocumentOf(operation) is { } inserting) (inserted ??= []).Add(inserting);
                    break;

                case OperationRole.Deletion:
                    (deleted ??= []).Add(new Deletion(operation.DocumentType, IdentityOf(operation)));
                    break;
            }
        }

        // Empty rather than null throughout, per IDocumentChangeSet, and a shared empty array rather
        // than a List so the overwhelmingly common "this commit deleted nothing" costs no allocation.
        _updated = updated ?? (IReadOnlyList<object>)Array.Empty<object>();
        _inserted = inserted ?? (IReadOnlyList<object>)Array.Empty<object>();
        _deleted = deleted ?? (IReadOnlyList<IDeletion>)Array.Empty<IDeletion>();
    }

    public IEnumerable<object> Updated => _updated;
    public IEnumerable<object> Inserted => _inserted;
    public IEnumerable<IDeletion> Deleted => _deleted;

    // The shared contract's view (#485). Explicit because IEnumerable<T> does not satisfy an
    // IReadOnlyList<T> member -- see the commentary on IChangeSet, which carries the default
    // implementations these override. IReadOnlyList<T> is covariant, so the IDeletion list is
    // already an IReadOnlyList<IDocumentDeletion>.
    IReadOnlyList<object> IDocumentChangeSet.Updated => _updated;
    IReadOnlyList<object> IDocumentChangeSet.Inserted => _inserted;
    IReadOnlyList<IDocumentDeletion> IDocumentChangeSet.Deleted => _deleted;

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
