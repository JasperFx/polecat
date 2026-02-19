using JasperFx.Events.Projections;
using Polecat.Events;

namespace Polecat.Internal;

/// <summary>
///     Session with identity map. Loading the same document by id returns the same
///     object reference. Documents queued via Store/Insert/Update are also tracked.
/// </summary>
internal class IdentityMapDocumentSession : DocumentSessionBase
{
    private readonly Dictionary<Type, Dictionary<object, object>> _identityMap = new();

    public IdentityMapDocumentSession(
        StoreOptions options,
        ConnectionFactory connectionFactory,
        DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer,
        EventGraph eventGraph,
        IInlineProjection<IDocumentSession>[] inlineProjections,
        string tenantId,
        IReadOnlyList<IDocumentSessionListener>? sessionListeners = null)
        : base(options, connectionFactory, providers, tableEnsurer, eventGraph, inlineProjections, tenantId, sessionListeners)
    {
    }

    protected override void OnDocumentStored(Type documentType, object id, object document)
    {
        var typeMap = GetOrCreateTypeMap(documentType);
        typeMap[id] = document;
    }

    protected override async Task<T?> LoadInternalAsync<T>(object id, CancellationToken token) where T : class
    {
        // Check identity map first
        if (_identityMap.TryGetValue(typeof(T), out var typeMap) && typeMap.TryGetValue(id, out var cached))
        {
            return (T)cached;
        }

        // Load from database
        var result = await base.LoadInternalAsync<T>(id, token);

        // Cache in identity map
        if (result != null)
        {
            var map = GetOrCreateTypeMap(typeof(T));
            map[id] = result;
        }

        return result;
    }

    protected override async Task<IReadOnlyList<T>> LoadManyInternalAsync<T>(
        List<object> ids, CancellationToken token) where T : class
    {
        var results = new List<T>();
        var missingIds = new List<object>();

        // Check identity map for each id
        _identityMap.TryGetValue(typeof(T), out var typeMap);

        foreach (var id in ids)
        {
            if (typeMap != null && typeMap.TryGetValue(id, out var cached))
            {
                results.Add((T)cached);
            }
            else
            {
                missingIds.Add(id);
            }
        }

        // Load missing from database
        if (missingIds.Count > 0)
        {
            var loaded = await base.LoadManyInternalAsync<T>(missingIds, token);
            var map = GetOrCreateTypeMap(typeof(T));

            foreach (var doc in loaded)
            {
                var provider = _providers.GetProvider<T>();
                var id = provider.Mapping.GetId(doc);
                map[id] = doc;
                results.Add(doc);
            }
        }

        return results;
    }

    protected override void OnDocumentEjected(Type documentType, object id)
    {
        if (_identityMap.TryGetValue(documentType, out var typeMap))
        {
            typeMap.Remove(id);
        }
    }

    protected override void OnAllOfTypeEjected(Type documentType)
    {
        _identityMap.Remove(documentType);
    }

    private Dictionary<object, object> GetOrCreateTypeMap(Type type)
    {
        if (!_identityMap.TryGetValue(type, out var map))
        {
            map = new Dictionary<object, object>();
            _identityMap[type] = map;
        }

        return map;
    }
}
