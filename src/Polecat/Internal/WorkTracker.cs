using JasperFx.Events;
using System.Collections.Immutable;
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
