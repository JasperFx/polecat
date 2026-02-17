using System.Collections.Concurrent;
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

    public DocumentProviderRegistry(StoreOptions options)
    {
        _options = options;
    }

    public DocumentProvider GetProvider<T>() => GetProvider(typeof(T));

    public DocumentProvider GetProvider(Type documentType)
    {
        return _providers.GetOrAdd(documentType, type =>
        {
            var mapping = new DocumentMapping(type, _options);
            return new DocumentProvider(mapping);
        });
    }

    public IEnumerable<DocumentProvider> AllProviders => _providers.Values;
}
