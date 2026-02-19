using Polecat.Schema.Identity.Sequences;

namespace Polecat;

public class AdvancedOperations
{
    private readonly DocumentStore _store;

    internal AdvancedOperations(DocumentStore store)
    {
        _store = store;
    }

    public HiloSettings HiloSequenceDefaults => _store.Options.HiloSequenceDefaults;

    public Task ResetHiloSequenceFloor<T>(long floor)
    {
        var sequence = _store.Sequences.SequenceFor(typeof(T));
        return sequence.SetFloor(floor);
    }
}
