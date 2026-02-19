using System.Collections.Concurrent;
using Polecat.Schema.Identity.Sequences;
using Polecat.Storage;

namespace Polecat.Internal;

/// <summary>
///     Thread-safe registry of DocumentProviders, one per document type.
///     Lazily creates mappings and providers on first access.
/// </summary>
internal class DocumentProviderRegistry
{
    private readonly ConcurrentDictionary<Type, DocumentProvider> _providers = new();
    private readonly StoreOptions _options;
    private SequenceFactory? _sequenceFactory;

    public DocumentProviderRegistry(StoreOptions options)
    {
        _options = options;
    }

    internal void SetSequenceFactory(SequenceFactory sequenceFactory)
    {
        _sequenceFactory = sequenceFactory;
    }

    public DocumentProvider GetProvider<T>() => GetProvider(typeof(T));

    public DocumentProvider GetProvider(Type documentType)
    {
        return _providers.GetOrAdd(documentType, type =>
        {
            var mapping = new DocumentMapping(type, _options);
            var provider = new DocumentProvider(mapping);

            if (mapping.IsNumericId && _sequenceFactory != null)
            {
                var settings = mapping.HiloSettings ?? _options.HiloSequenceDefaults;
                provider.Sequence = _sequenceFactory.Hilo(type, settings);
            }

            return provider;
        });
    }

    public IEnumerable<DocumentProvider> AllProviders => _providers.Values;
}
