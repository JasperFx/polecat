using System.Collections.Concurrent;
using Polecat.Internal;

namespace Polecat.Schema.Identity.Sequences;

internal class SequenceFactory
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
            sequenceName => new HiloSequence(_connectionFactory, _options.DatabaseSchemaName, sequenceName, settings));
    }

    private static string GetSequenceName(Type documentType, HiloSettings settings)
    {
        return !string.IsNullOrEmpty(settings.SequenceName)
            ? settings.SequenceName!
            : documentType.Name;
    }
}
