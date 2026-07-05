using System.Collections.Concurrent;
using Polecat.Internal;
using Weasel.Core.Sequences;

namespace Polecat.Schema.Identity.Sequences;

// #273: implements the Weasel.Core ISequenceSource seam (SequenceFor(Type) -> ISequence, already
// present) so the shared Weasel.Core.Identity strategies can resolve Hi-Lo sequences through it.
internal class SequenceFactory : ISequenceSource
{
    private readonly ConcurrentDictionary<string, ISequence> _sequences = new();
    private readonly ConnectionFactory _connectionFactory;
    private readonly StoreOptions _options;

    public SequenceFactory(StoreOptions options, ConnectionFactory connectionFactory)
    {
        _options = options;
        _connectionFactory = connectionFactory;
    }

    public ISequence SequenceFor(Type documentType)
    {
        var provider = _options.Providers.GetProvider(documentType);
        var settings = provider.Mapping.HiloSettings ?? _options.HiloSequenceDefaults;
        return Hilo(documentType, settings);
    }

    public ISequence Hilo(Type documentType, HiloSettings settings)
    {
        var name = GetSequenceName(documentType, settings);
        return _sequences.GetOrAdd(name,
            sequenceName => new HiloSequence(_connectionFactory, _options.DatabaseSchemaName, sequenceName, settings, _options.ResiliencePipeline, _options.AutoCreateSchemaObjects));
    }

    private static string GetSequenceName(Type documentType, HiloSettings settings)
    {
        return !string.IsNullOrEmpty(settings.SequenceName)
            ? settings.SequenceName!
            : documentType.Name;
    }
}
