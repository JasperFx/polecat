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
    /// <summary>
    ///     The closed-shape provider for <typeparamref name="TDoc" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>The four canonical id types are closed STATICALLY, and that is what makes this
    ///         work under Native AOT</b> (#733). <typeparamref name="TDoc" /> is already the document
    ///         type here — <c>PolecatProviderGraph.StorageFor&lt;T&gt;</c> is generic — so
    ///         <c>BuildTypedProvider&lt;TDoc, Guid&gt;</c> and its three siblings are ordinary generic
    ///         calls the compiler emits and ILC compiles. The id type is still DECIDED at runtime;
    ///         what changed is that the generic is no longer CLOSED at runtime.
    ///     </para>
    ///     <para>
    ///         This method used to do <c>MakeGenericMethod(mapping.DocumentType, mapping.IdType)</c>,
    ///         which works under CoreCLR and throws in a native image on the first document
    ///         operation:
    ///     </para>
    ///     <code>
    ///     NotSupportedException: 'PolecatClosedShapeRegistration.BuildTypedProvider[Quest,System.Guid]'
    ///     is missing native code. MethodInfo.MakeGenericMethod() is not compatible with AOT.
    ///     </code>
    ///     <para>
    ///         <c>MakeGenericType</c> / <c>MakeGenericMethod</c> can close an instantiation whose
    ///         arguments are all REFERENCE types, because those share one canonical body — which is
    ///         why reaching this method itself through <c>MakeGenericMethod(documentType)</c> is fine,
    ///         and why only the id type needed moving. An id type is routinely <see cref="Guid" />,
    ///         <see cref="int" /> or <see cref="long" />.
    ///     </para>
    ///     <para>
    ///         <b>Both sibling stores already did it this way</b> and Polecat was the outlier:
    ///         Marten's <c>ClosedShapeRegistration.BuildSupportedProvider&lt;TDoc&gt;</c> and Fisher's
    ///         <c>DocumentProviderRegistry.BuildProviderFor&lt;T&gt;</c>, the latter from fisher#384 —
    ///         the same bug, the same symptom, the same fix.
    ///     </para>
    ///     <para>
    ///         A strong-typed id wrapper stays reflective, because its type is a runtime value nothing
    ///         here can name. That is the one case Weasel's own
    ///         <c>Identifications.ForValueType</c> carries a reflective fallback for (weasel#690).
    ///     </para>
    /// </remarks>
    internal static object BuildProviderFor<TDoc>(DocumentMapping mapping) where TDoc : notnull
    {
        if (mapping.ValueTypeId is { } vt)
        {
            return BuildValueTypeProvider<TDoc>(mapping, vt);
        }

        if (mapping.IdType == typeof(Guid))
        {
            return BuildTypedProvider<TDoc, Guid>(mapping,
                new SequentialGuidIdentification<TDoc>(mapping.IdMember));
        }

        if (mapping.IdType == typeof(string))
        {
            return BuildTypedProvider<TDoc, string>(mapping,
                new StringIdentification<TDoc>(mapping.IdMember));
        }

        if (mapping.IdType == typeof(int))
        {
            return BuildTypedProvider<TDoc, int>(mapping,
                new HiloIntIdentification<TDoc>(mapping.IdMember, mapping.DocumentType));
        }

        if (mapping.IdType == typeof(long))
        {
            return BuildTypedProvider<TDoc, long>(mapping,
                new HiloLongIdentification<TDoc>(mapping.IdMember, mapping.DocumentType));
        }

        throw new NotSupportedException(
            $"Unsupported id type {mapping.IdType.FullName} for closed-shape storage.");
    }

    /// <summary>
    ///     The provider for a strong-typed id, whose wrapper type is a runtime value — so this one
    ///     closes reflectively and is the only part of #733's fix that does.
    /// </summary>
    /// <remarks>
    ///     ⚠️ Still a value-type argument in <c>ValueTypeIdentification&lt;TDoc, TOuter, TInner&gt;</c>
    ///     and in <c>BuildTypedProvider&lt;TDoc, TOuter&gt;</c> when the wrapper is a
    ///     <c>readonly record struct</c>, so a natively-published store with a strong-typed document
    ///     id still fails here. Tracked on #733; the smoke harness drives a plain Guid id, so it is
    ///     not currently covered either way.
    /// </remarks>
    private static object BuildValueTypeProvider<TDoc>(DocumentMapping mapping, ValueTypeInfo vt)
        where TDoc : notnull
    {
        var identification = typeof(ValueTypeIdentification<,,>)
            .MakeGenericType(typeof(TDoc), vt.OuterType, vt.SimpleType)
            .GetConstructors()[0]
            .Invoke(new object[] { mapping.IdMember, vt, mapping.DocumentType });

        return typeof(PolecatClosedShapeRegistration)
            .GetMethod(nameof(BuildTypedProvider), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(TDoc), vt.OuterType)
            .Invoke(null, new[] { mapping, identification })!;
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

    /// <summary>
    ///     Builds the closed-shape provider for a registered SUBCLASS (#273 E2e): each flavor
    ///     is a <see cref="SubClassPolecatStorage{T,TRoot,TId}" /> wrapping the hierarchy
    ///     root's corresponding flavor from <paramref name="rootProvider" />.
    /// </summary>
    internal static object BuildSubClassProviderFor(DocumentMapping rootMapping, Type subclassType,
        object rootProvider)
    {
        var idType = rootMapping.ValueTypeId?.OuterType ?? rootMapping.IdType;
        return typeof(PolecatClosedShapeRegistration)
            .GetMethod(nameof(BuildTypedSubClassProvider), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(subclassType, rootMapping.DocumentType, idType)
            .Invoke(null, new[] { rootProvider, rootMapping })!;
    }

    private static DocumentProvider<T> BuildTypedSubClassProvider<T, TRoot, TId>(
        DocumentProvider<TRoot> rootProvider, DocumentMapping rootMapping)
        where T : notnull, TRoot
        where TRoot : notnull
        where TId : notnull
    {
        SubClassPolecatStorage<T, TRoot, TId> Wrap(IDocumentStorage<TRoot> storage)
            => new((IDocumentStorage<TRoot, TId>)storage, rootMapping);

        return new DocumentProvider<T>(
            Wrap(rootProvider.QueryOnly),
            Wrap(rootProvider.Lightweight),
            Wrap(rootProvider.IdentityMap),
            Wrap(rootProvider.DirtyTracking));
    }
}

/// <summary>
///     Polecat's <see cref="IProviderGraph" /> — lazily builds and caches the closed-shape
///     <see cref="DocumentProvider{T}" /> per document type over the bespoke registry's mappings.
/// </summary>
internal sealed class PolecatProviderGraph : IProviderGraph
{
    private readonly Internal.DocumentProviderRegistry _registry;
    private readonly Dictionary<Type, object> _providers = new();

    /// <summary>
    ///     Each registered type's QueryOnly storage, captured where <c>T</c> is still a type
    ///     parameter. #741 — see <see cref="QueryOnlySelectClauseFor" />; this is not a speed cache.
    /// </summary>
    private readonly Dictionary<Type, object> _selectClauses = new();
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

            // The registry routes subclass types to the hierarchy root's provider, so a
            // mapping whose DocumentType differs from T marks T as a registered subclass:
            // wrap the root's flavors in SubClassPolecatStorage (#273 E2e).
            var mapping = _registry.GetProvider(typeof(T)).Mapping;
            var provider = mapping.DocumentType == typeof(T)
                ? (DocumentProvider<T>)PolecatClosedShapeRegistration.BuildProviderFor<T>(mapping)
                : (DocumentProvider<T>)PolecatClosedShapeRegistration.BuildSubClassProviderFor(
                    mapping, typeof(T), ProviderFor(mapping.DocumentType));
            _providers[typeof(T)] = provider;

            // #741: capture the select clause HERE, while T is still a type parameter. See
            // QueryOnlySelectClauseFor for why this is not a cache for speed.
            _selectClauses[typeof(T)] = provider.QueryOnly;
            return provider;
        }
    }

    public void Append<T>(DocumentProvider<T> provider) where T : notnull
    {
        lock (_lock)
        {
            _providers[typeof(T)] = provider;
            _selectClauses[typeof(T)] = provider.QueryOnly;
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

    /// <summary>
    ///     Non-generic access to a root document type's QueryOnly storage as the shared
    ///     select-clause contract — the LINQ provider's materialization seam (#273 E2d).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>#741: this used to reflect, and under Native AOT that was every LINQ query's
    ///         undoing.</b> It read
    ///         <c>provider.GetType().GetProperty(nameof(DocumentProvider&lt;object&gt;.QueryOnly))!</c>
    ///         — and <see cref="Type.GetProperty(string)" /> returns <b>null</b> for a member the
    ///         trimmer removed rather than throwing, so the null-forgiving operator one line later
    ///         produced a bare <see cref="NullReferenceException" /> naming nothing:
    ///     </para>
    ///     <code>
    ///     NullReferenceException
    ///       at PolecatProviderGraph.QueryOnlySelectClauseFor(Type)
    ///       at PolecatLinqQueryProvider.ExecuteAsync
    ///       at PolecatQueryableExtensions.ToListAsync
    ///     </code>
    ///     <para>
    ///         Three separate LINQ shapes in the AOT smoke run failed on this one line — a document
    ///         read, an enum comparison and a child-collection filter — which looked like three bugs
    ///         until the harness started printing frames.
    ///     </para>
    ///     <para>
    ///         <b>The fix is to never ask at all.</b> <c>DocumentProvider&lt;T&gt;.QueryOnly</c> is
    ///         read in <see cref="StorageFor{T}" /> and <see cref="Append{T}" />, where <c>T</c> is
    ///         still a type parameter and the property access is an ordinary one the compiler emits.
    ///         So this is a <i>projection of state captured generically</i>, not a cache for speed:
    ///         removing it would not slow this down, it would break it under AOT again. Same shape as
    ///         weasel#689's lesson — a statically-referenced member survives trimming; a reflected one
    ///         does not.
    ///     </para>
    ///     <para>
    ///         <c>ProviderFor</c> is still called first, for its side effect: it builds and registers
    ///         the provider for a type nothing has touched yet, which is what populates the entry
    ///         below. Its own <c>MakeGenericMethod</c> closes a single REFERENCE-type argument, which
    ///         shares a canonical body and works natively.
    ///     </para>
    /// </remarks>
    internal Weasel.Storage.ISelectClause QueryOnlySelectClauseFor(Type documentType)
    {
        ProviderFor(documentType);

        lock (_lock)
        {
            if (_selectClauses.TryGetValue(documentType, out var clause))
            {
                return (Weasel.Storage.ISelectClause)clause;
            }
        }

        throw new InvalidOperationException(
            $"No closed-shape QueryOnly storage was registered for '{documentType.FullName}'. "
            + "ProviderFor should have built one; this means the provider was registered by a route "
            + "that does not record its select clause (see polecat#741).");
    }
}
