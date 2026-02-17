using JasperFx.Events;

namespace Polecat.Internal;

/// <summary>
///     Queues storage operations and stream actions for a document session's unit of work.
/// </summary>
internal class WorkTracker : IWorkTracker
{
    private readonly List<IStorageOperation> _operations = new();
    private readonly List<StreamAction> _streams = new();

    public IReadOnlyList<IStorageOperation> Operations => _operations;
    public IReadOnlyList<StreamAction> Streams => _streams;

    public bool HasOutstandingWork() => _operations.Count > 0 || _streams.Count > 0;

    public void Add(IStorageOperation operation)
    {
        _operations.Add(operation);
    }

    public void AddStream(StreamAction stream)
    {
        _streams.Add(stream);
    }

    public bool TryFindStream(Guid id, out StreamAction? stream)
    {
        stream = _streams.FirstOrDefault(s => s.Id == id);
        return stream != null;
    }

    public bool TryFindStream(string key, out StreamAction? stream)
    {
        stream = _streams.FirstOrDefault(s => s.Key == key);
        return stream != null;
    }

    public void Reset()
    {
        _operations.Clear();
        _streams.Clear();
    }
}
