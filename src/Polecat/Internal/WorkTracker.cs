using JasperFx.Events;
using System.Collections.Immutable;

namespace Polecat.Internal;

/// <summary>
///     Queues storage operations and stream actions for a document session's unit of work.
/// </summary>
internal class WorkTracker : IWorkTracker
{
    private readonly List<IStorageOperation> _operations = [];
    private ImmutableList<IStorageOperation>? _operationsSnapshot;

    private readonly List<StreamAction> _streams = [];
    private ImmutableList<StreamAction>? _streamsSnapshot;
    private readonly Lock _stateLock = new();

    public IReadOnlyList<IStorageOperation> Operations
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

    public void Add(IStorageOperation operation)
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
                && op.DocumentId != null
                && op.DocumentId.Equals(id));

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
}
