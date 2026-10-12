using System.Collections.Concurrent;
using JasperFx.Core.Reflection;
using Weasel.Core.Identity;
using Weasel.Storage;

namespace Polecat.Storage.ClosedShape;

/// <summary>
///     Statically-closed factories for the generics Polecat would otherwise close over a strong-typed
///     document id at runtime (#751). Registry first, reflective fallback.
/// </summary>
/// <remarks>
///     <para>
///         <see cref="PolecatClosedShapeRegistration" /> closes <c>ValueTypeIdentification&lt;TDoc,
///         TWrapper, TInner&gt;</c> and the four storage flavors over the WRAPPER, which is routinely a
///         <c>readonly record struct</c>. A value-type argument has no canonical body to share, and ILC
///         cannot have precompiled an instantiation naming types from the consumer's assembly — so
///         under Native AOT one strong-typed id anywhere in the store failed the first session to touch
///         that document. #733 ruled out both escapes by trying them: reflecting over the closed type
///         (the trimmer removes the metadata) and reimplementing Weasel's identity rules locally.
///     </para>
///     <para>
///         <see cref="Register{TDoc,TWrapper,TInner}" /> is the third way, and Marten's (#5589): a call
///         in the CONSUMER's code naming all three types makes the compiler emit every instantiation the
///         body names, and the delegate stored here hands the runtime those exact closed types. Reached
///         through <see cref="StoreOptions.RegisterValueTypeId{TDoc,TWrapper,TInner}" />.
///     </para>
///     <para>
///         Static rather than per-store, because the instantiations are a fact about the compiled image
///         rather than about one <see cref="StoreOptions" /> — which also lets a future source generator
///         emit the registrations from a <c>[ModuleInitializer]</c> with no store to hang them off. Under a
///         JIT a miss falls through to the reflective path, which is correct wherever dynamic code is
///         available, so nothing changes there.
///     </para>
/// </remarks>
internal static class ValueTypeIdRegistry
{
    private static readonly ConcurrentDictionary<(Type Document, Type Wrapper), object> _providers = new();

    public static void Register<TDoc, TWrapper, TInner>()
        where TDoc : notnull
        where TWrapper : struct
        where TInner : notnull
    {
        _providers[(typeof(TDoc), typeof(TWrapper))] =
            new Func<DocumentMapping, ValueTypeInfo, DocumentProvider<TDoc>>((mapping, vt) =>
                PolecatClosedShapeRegistration.BuildTypedProvider<TDoc, TWrapper>(mapping,
                    new ValueTypeIdentification<TDoc, TWrapper, TInner>(mapping.IdMember, vt,
                        mapping.DocumentType)));
    }

    /// <summary>Whether a registration rooted <paramref name="document" /> over <paramref name="wrapper" />.</summary>
    internal static bool IsRegistered(Type document, Type wrapper) => _providers.ContainsKey((document, wrapper));

    public static DocumentProvider<TDoc>? TryProvider<TDoc>(Type wrapper, DocumentMapping mapping,
        ValueTypeInfo vt)
        where TDoc : notnull
    {
        return _providers.TryGetValue((typeof(TDoc), wrapper), out var factory)
            ? ((Func<DocumentMapping, ValueTypeInfo, DocumentProvider<TDoc>>)factory)(mapping, vt)
            : null;
    }
}
