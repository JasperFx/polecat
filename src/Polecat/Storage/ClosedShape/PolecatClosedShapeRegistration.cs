using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JasperFx.Core.Reflection;
using Weasel.Core.Identity;
using Weasel.Storage;

namespace Polecat.Storage.ClosedShape;

/// <summary>
///     Builds the closed-shape <see cref="DocumentProvider{T}" /> (the four storage flavors) for
///     a document type from its Polecat <see cref="DocumentMapping" /> — the Polecat analog of
///     Marten's <c>ClosedShapeRegistration</c> (#273 phase E1). Identity strategies come from the
///     shared <c>Weasel.Core.Identity</c> family (#276): sequential GUIDs, externally-assigned
///     strings, Hi-Lo int/long, and strongly-typed value ids.
/// </summary>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Closes the shared identity strategies + storage generics over runtime document/id types via MakeGenericMethod once per document type at registration — the same pattern as DocumentMapping's id accessors and Marten's ClosedShapeRegistration. AOT consumers register document types explicitly per the AOT publishing guide.")]
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Same as IL3050 — registration-time generic closing over registered document types.")]
[UnconditionalSuppressMessage("Trimming", "IL2060:MakeGenericMethod",
    Justification = "Same as IL3050.")]
internal static class PolecatClosedShapeRegistration
{
    internal static object BuildProviderFor(DocumentMapping mapping)
    {
        var buildTyped = typeof(PolecatClosedShapeRegistration)
            .GetMethod(nameof(BuildTypedProvider), BindingFlags.NonPublic | BindingFlags.Static)!;

        if (mapping.ValueTypeId is { } vt)
        {
            var identification = typeof(ValueTypeIdentification<,,>)
                .MakeGenericType(mapping.DocumentType, vt.OuterType, vt.SimpleType)
                .GetConstructors()[0]
                .Invoke(new object[] { mapping.IdMember, vt, mapping.DocumentType });
            return buildTyped.MakeGenericMethod(mapping.DocumentType, vt.OuterType)
                .Invoke(null, new[] { mapping, identification })!;
        }

        object strategy = mapping.IdType switch
        {
            var t when t == typeof(Guid) =>
                new object[] { typeof(SequentialGuidIdentification<>), mapping.IdMember },
            var t when t == typeof(string) =>
                new object[] { typeof(StringIdentification<>), mapping.IdMember },
            var t when t == typeof(int) =>
                new object[] { typeof(HiloIntIdentification<>), mapping.IdMember, mapping.DocumentType },
            var t when t == typeof(long) =>
                new object[] { typeof(HiloLongIdentification<>), mapping.IdMember, mapping.DocumentType },
            _ => throw new NotSupportedException(
                $"Unsupported id type {mapping.IdType.FullName} for closed-shape storage.")
        };

        var parts = (object[])strategy;
        var strategyType = ((Type)parts[0]).MakeGenericType(mapping.DocumentType);
        var identificationInstance = Activator.CreateInstance(strategyType, parts.Skip(1).ToArray())!;
        return buildTyped.MakeGenericMethod(mapping.DocumentType, mapping.IdType)
            .Invoke(null, new[] { mapping, identificationInstance })!;
    }

    private static DocumentProvider<TDoc> BuildTypedProvider<TDoc, TId>(
        DocumentMapping mapping,
        IIdentification<TDoc, TId> identification)
        where TDoc : notnull
        where TId : notnull
    {
        var descriptor = SqlServerDocumentStorageDescriptorBuilder.Build(
            mapping, identification, mapping.StoreOptions);

        var queryOnly = new QueryOnlyPolecatStorage<TDoc, TId>(mapping, descriptor);
        var lightweight = BuildLightweight(mapping, descriptor);
        var identityMap = BuildIdentityMap(mapping, descriptor);

        // Polecat has no dirty tracking by design — the DirtyTracking slot gets the
        // IdentityMap storage (the closest tracking mode Polecat offers).
        return new DocumentProvider<TDoc>(queryOnly, lightweight, identityMap, identityMap);
    }

    private static LightweightPolecatStorage<TDoc, TId> BuildLightweight<TDoc, TId>(
        DocumentMapping mapping, DocumentStorageDescriptor<TDoc, TId> descriptor)
        where TDoc : notnull
        where TId : notnull
        => descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Optimistic => new OptimisticLightweightPolecatStorage<TDoc, TId>(mapping, descriptor),
            ConcurrencyMode.Numeric => new NumericLightweightPolecatStorage<TDoc, TId>(mapping, descriptor),
            _ => new UnversionedLightweightPolecatStorage<TDoc, TId>(mapping, descriptor)
        };

    private static IdentityMapPolecatStorage<TDoc, TId> BuildIdentityMap<TDoc, TId>(
        DocumentMapping mapping, DocumentStorageDescriptor<TDoc, TId> descriptor)
        where TDoc : notnull
        where TId : notnull
        => descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Optimistic => new OptimisticIdentityMapPolecatStorage<TDoc, TId>(mapping, descriptor),
            ConcurrencyMode.Numeric => new NumericIdentityMapPolecatStorage<TDoc, TId>(mapping, descriptor),
            _ => new UnversionedIdentityMapPolecatStorage<TDoc, TId>(mapping, descriptor)
        };
}

/// <summary>
///     Polecat's <see cref="IProviderGraph" /> — lazily builds and caches the closed-shape
///     <see cref="DocumentProvider{T}" /> per document type over the bespoke registry's mappings.
/// </summary>
internal sealed class PolecatProviderGraph : IProviderGraph
{
    private readonly Internal.DocumentProviderRegistry _registry;
    private readonly Dictionary<Type, object> _providers = new();
    private readonly object _lock = new();

    public PolecatProviderGraph(Internal.DocumentProviderRegistry registry)
    {
        _registry = registry;
    }

    public DocumentProvider<T> StorageFor<T>() where T : notnull
    {
        lock (_lock)
        {
            if (_providers.TryGetValue(typeof(T), out var cached))
            {
                return (DocumentProvider<T>)cached;
            }

            var mapping = _registry.GetProvider(typeof(T)).Mapping;
            var provider = (DocumentProvider<T>)PolecatClosedShapeRegistration.BuildProviderFor(mapping);
            _providers[typeof(T)] = provider;
            return provider;
        }
    }

    public void Append<T>(DocumentProvider<T> provider) where T : notnull
    {
        lock (_lock)
        {
            _providers[typeof(T)] = provider;
        }
    }

    /// <summary>Non-generic lookup for <c>IStorageSession.StorageFor(Type)</c>.</summary>
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Closes StorageFor<T> over a registered document type; cached per type.")]
    [UnconditionalSuppressMessage("Trimming", "IL2060:MakeGenericMethod", Justification = "Same as IL3050.")]
    internal object ProviderFor(Type documentType)
    {
        lock (_lock)
        {
            if (_providers.TryGetValue(documentType, out var cached))
            {
                return cached;
            }
        }

        return typeof(PolecatProviderGraph)
            .GetMethod(nameof(StorageFor))!
            .MakeGenericMethod(documentType)
            .Invoke(this, null)!;
    }
}
