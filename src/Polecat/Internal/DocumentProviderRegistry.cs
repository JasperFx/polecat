using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Polecat.Schema.Identity.Sequences;
using Polecat.Storage;

namespace Polecat.Internal;

/// <summary>
///     Thread-safe registry of DocumentProviders, one per document type.
///     Lazily creates mappings and providers on first access.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2075:DynamicallyAccessedMembers",
    Justification = "Class-level: reads SubClasses/Indexes/ForeignKeys non-public fields off DocumentMappingExpression<T> via reflection. The expression type and fields are preserved by the registration boundary (Schema.For<T>()), where T flows in from caller code that trimming sees.")]
internal class DocumentProviderRegistry
{
    private readonly ConcurrentDictionary<Type, DocumentProvider> _providers = new();
    private readonly StoreOptions _options;
    private SequenceFactory? _sequenceFactory;

    public DocumentProviderRegistry(StoreOptions options)
    {
        _options = options;

        // Pre-populate subclass → parent routing from schema configuration
        foreach (var expr in options.Schema.Expressions)
        {
            var exprType = expr.GetType();
            if (!exprType.IsGenericType) continue;

            var docType = exprType.GetGenericArguments()[0];
            var subClassesField = exprType.GetField("SubClasses", BindingFlags.NonPublic | BindingFlags.Instance);
            if (subClassesField?.GetValue(expr) is not IEnumerable<(Type SubClass, string? Alias)> subClasses) continue;

            foreach (var (subClass, _) in subClasses)
            {
                _subClassToParent.TryAdd(subClass, docType);
            }
        }
    }

    internal void SetSequenceFactory(SequenceFactory sequenceFactory)
    {
        _sequenceFactory = sequenceFactory;
    }

    public DocumentProvider GetProvider<T>() => GetProvider(typeof(T));

    public DocumentProvider GetProvider(Type documentType)
    {
        // Check if this type is a registered subclass — route to parent's provider
        if (_subClassToParent.TryGetValue(documentType, out var parentType))
        {
            return GetProvider(parentType);
        }

        return _providers.GetOrAdd(documentType, type =>
        {
            var mapping = new DocumentMapping(type, _options);

            // Apply schema configuration (sub-class hierarchy registrations)
            ApplySchemaConfiguration(mapping);

            var provider = new DocumentProvider(mapping);

            if (mapping.IsNumericId && _sequenceFactory != null)
            {
                var settings = mapping.HiloSettings ?? _options.HiloSequenceDefaults;
                provider.Sequence = _sequenceFactory.Hilo(type, settings);
            }

            return provider;
        });
    }

    private readonly ConcurrentDictionary<Type, Type> _subClassToParent = new();

    private void ApplySchemaConfiguration(DocumentMapping mapping)
    {
        foreach (var expr in _options.Schema.Expressions)
        {
            var exprType = expr.GetType();
            if (!exprType.IsGenericType) continue;

            var docType = exprType.GetGenericArguments()[0];
            if (docType != mapping.DocumentType) continue;

            // Found the expression for this mapping — apply sub-classes
            var subClassesField = exprType.GetField("SubClasses", BindingFlags.NonPublic | BindingFlags.Instance);
            if (subClassesField?.GetValue(expr) is IEnumerable<(Type SubClass, string? Alias)> subClasses)
            {
                foreach (var (subClass, alias) in subClasses)
                {
                    mapping.AddSubClass(subClass, alias);
                    _subClassToParent.TryAdd(subClass, mapping.DocumentType);
                }
            }

            // Apply indexes
            var indexesField = exprType.GetField("Indexes", BindingFlags.NonPublic | BindingFlags.Instance);
            if (indexesField?.GetValue(expr) is IEnumerable<Storage.DocumentIndex> indexes)
            {
                foreach (var index in indexes)
                {
                    mapping.Indexes.Add(index);
                }
            }

            // Apply foreign keys
            var fkField = exprType.GetField("ForeignKeys", BindingFlags.NonPublic | BindingFlags.Instance);
            if (fkField?.GetValue(expr) is IEnumerable<Storage.DocumentForeignKey> foreignKeys)
            {
                foreach (var fk in foreignKeys)
                {
                    mapping.ForeignKeys.Add(fk);
                }
            }
        }
    }

    public IEnumerable<DocumentProvider> AllProviders => _providers.Values;
}
