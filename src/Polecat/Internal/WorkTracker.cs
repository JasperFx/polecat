using JasperFx.Events;
using System.Collections.Immutable;
using JasperFx.Events.Documents;
using Polecat.Services;

namespace Polecat.Internal;

/// <summary>
///     Queues storage operations and stream actions for a document session's unit of work.
/// </summary>
internal class WorkTracker : IWorkTracker
{
    private readonly List<Weasel.Storage.IStorageOperation> _operations = [];
    private ImmutableList<Weasel.Storage.IStorageOperation>? _operationsSnapshot;

    private readonly List<StreamAction> _streams = [];
    private ImmutableList<StreamAction>? _streamsSnapshot;
    private readonly Lock _stateLock = new();

    public IReadOnlyList<Weasel.Storage.IStorageOperation> Operations
    {
        get
        {
            lock (_stateLock)
            {
                _operationsSnapshot ??= [.. _operations];
                return _operationsSnapshot;
            }
        }
    }

    public IReadOnlyList<StreamAction> Streams
    {
        get
        {
            lock (_stateLock)
            {
                _streamsSnapshot ??= [.. _streams];
                return _streamsSnapshot;
            }
        }
    }

    public bool HasOutstandingWork()
    {
        lock (_stateLock)
            return _operations.Count > 0
                || _streams.Any(x =>
                    x.Events.Count > 0 || x.AlwaysEnforceConsistency);
    }

    // IChangeSet — a live view over the current unit of work. Callers retaining it past the commit
    // boundary must Clone() first, because the tracker is Reset() after each commit.
    public IEnumerable<object> Updated => ChangeSet.UpdatedFrom(Operations);
    public IEnumerable<object> Inserted => ChangeSet.InsertedFrom(Operations);
    public IEnumerable<IDeletion> Deleted => ChangeSet.DeletedFrom(Operations);
    public IEnumerable<IEvent> GetEvents() => Streams.SelectMany(x => x.Events);
    public IEnumerable<StreamAction> GetStreams() => Streams;
    public IChangeSet Clone() => new ChangeSet(Operations, Streams);

    // #485 / jasperfx#679: the shared contract's view of the SAME live unit of work. Explicit for
    // the covariance reason spelled out on IChangeSet -- the three IEnumerable-typed members above
    // do not satisfy the IReadOnlyList-typed contract members -- and materialised here rather than
    // left on IChangeSet's default implementations, which would re-run the LINQ chains over the
    // whole operation list on every access.
    //
    // The memo is keyed off the Operations SNAPSHOT INSTANCE rather than cached outright, because
    // unlike ChangeSet this object is mutable and long-lived: it accumulates operations and is
    // Reset() and reused after every commit. Operations already invalidates its snapshot on every
    // mutation, so a snapshot the memo was not built from is exactly the signal to rebuild -- and
    // no new invalidation has to be threaded through Add / AddStream / Reset / Eject*, which is the
    // version of this that goes stale the next time someone adds a mutator.
    //
    // ⚠️ These are still a view of a LIVE tracker, so they answer the CURRENT unit of work, not a
    // frozen one. What DocumentSessionBase hands a commit listener is Clone()'s immutable ChangeSet.
    private IReadOnlyList<Weasel.Storage.IStorageOperation>? _documentViewSource;
    private IReadOnlyList<object>? _updatedView;
    private IReadOnlyList<object>? _insertedView;
    private IReadOnlyList<IDeletion>? _deletedView;

    IReadOnlyList<object> IDocumentChangeSet.Updated
    {
        get
        {
            lock (_stateLock)
            {
                refreshDocumentViews();
                return _updatedView!;
            }
        }
    }

    IReadOnlyList<object> IDocumentChangeSet.Inserted
    {
        get
        {
            lock (_stateLock)
            {
                refreshDocumentViews();
                return _insertedView!;
            }
        }
    }

    IReadOnlyList<IDocumentDeletion> IDocumentChangeSet.Deleted
    {
        get
        {
            lock (_stateLock)
            {
                refreshDocumentViews();
                return _deletedView!;
            }
        }
    }

    private void refreshDocumentViews()
    {
        // Operations takes the same lock; it is re-entrant (System.Threading.Lock), and reading it
        // inside this one is what makes "snapshot taken" and "views built from it" a single atomic
        // step.
        var operations = Operations;
        if (ReferenceEquals(_documentViewSource, operations)) return;

        _updatedView = ChangeSet.UpdatedFrom(operations).ToList();
        _insertedView = ChangeSet.InsertedFrom(operations).ToList();
        _deletedView = ChangeSet.DeletedFrom(operations).ToList();
        _documentViewSource = operations;
    }

    public void Add(Weasel.Storage.IStorageOperation operation)
    {
        lock (_stateLock)
        {
            _operations.Add(operation);
            _operationsSnapshot = null;
        }
    }

    public void AddStream(StreamAction stream)
    {
        lock (_stateLock)
        {
            _streams.Add(stream);
            _streamsSnapshot = null;
        }
    }

    public bool TryFindStream(Guid id, out StreamAction? stream)
    {
        lock (_stateLock)
            stream = _streams.FirstOrDefault(s => s.Id == id);
        return stream != null;
    }

    public bool TryFindStream(string key, out StreamAction? stream)
    {
        lock (_stateLock)
            stream = _streams.FirstOrDefault(s => s.Key == key);
        return stream != null;
    }

    public void Reset()
    {
        lock (_stateLock)
        {
            _operations.Clear();
            _operationsSnapshot = null;

            _streams.Clear();
            _streamsSnapshot = null;
        }
    }

    public void EjectDocument(Type documentType, object id)
    {
        lock (_stateLock)
        {
            var removed = _operations.RemoveAll(op =>
                op.DocumentType == documentType
                && OperationIdentity(op) is { } opId
                && opId.Equals(id));

            if (removed > 0)
                _operationsSnapshot = null;
        }
    }

    public void EjectAllOfType(Type documentType)
    {
        lock (_stateLock)
        {
            var removed = _operations.RemoveAll(op => op.DocumentType == documentType);
            
            if (removed > 0)
                _operationsSnapshot = null;
        }
    }

    /// <summary>
    ///     Per-operation document identity for eject matching over the shared currency
    ///     (#273 E2e): bespoke Polecat operations (including the closed-shape adapter)
    ///     carry DocumentId; shared deletions carry Id.
    /// </summary>
    private static object? OperationIdentity(Weasel.Storage.IStorageOperation op)
        => op switch
        {
            IStorageOperation bespoke => bespoke.DocumentId,
            Weasel.Storage.IDeletion deletion => deletion.Id,
            _ => null
        };
}
