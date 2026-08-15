using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JasperFx.Core.Reflection;

namespace Polecat.Storage;

/// <summary>
///     Recognizes "strong typed id" style value wrappers — a type whose single public getter wraps one
///     of the scalar types Polecat can persist, paired with either a matching constructor or a static
///     factory method. Both Vogen (<c>[ValueObject&lt;Guid&gt;]</c>, private ctor + static <c>From</c>)
///     and StronglyTypedId (<c>[StronglyTypedId(Template.Guid)]</c>, public ctor) emit that shape, as
///     does a hand-written <c>record struct OrderId(Guid Value)</c>.
///     <para>
///     <see cref="DocumentMapping" /> uses this for the Id member; the LINQ layer uses it for every
///     other member, so a wrapper-typed property is queried against its inner scalar rather than
///     handed to SqlClient as an unknown CLR type.
///     </para>
/// </summary>
internal static class ValueTypes
{
    /// <summary>
    ///     The scalar types Polecat can store in a column or compare in a JSON path predicate, and
    ///     therefore the inner types a recognized wrapper may wrap.
    /// </summary>
    internal static readonly HashSet<Type> SupportedInnerTypes =
        [typeof(Guid), typeof(string), typeof(int), typeof(long)];

    // Negative results are cached too, and that is the point: ValueTypeInfo.ForType signals "not a
    // wrapper" by throwing, and only caches successes. Every LINQ member resolution over an ordinary
    // DateTimeOffset or decimal member would otherwise raise and swallow an exception.
    private static readonly ConcurrentDictionary<(Type, bool), ValueTypeInfo?> Cache = new();

    /// <summary>
    ///     Resolves <paramref name="type" /> as a value wrapper, or returns null when it is not one.
    ///     Nullable wrappers are unwrapped first, so <c>OrderId?</c> resolves the same as <c>OrderId</c>.
    /// </summary>
    /// <param name="type">The declared type of the id or member.</param>
    /// <param name="allowReferenceTypes">
    ///     True for a non-Id member, where a reference-type value object built through a static factory
    ///     also qualifies. Ids stay struct-only.
    /// </param>
    internal static ValueTypeInfo? TryResolve(Type type, bool allowReferenceTypes = false)
        => Cache.GetOrAdd((type, allowReferenceTypes), key => Resolve(key.Item1, key.Item2));

    /// <summary>
    ///     Resolves <paramref name="type" /> up front and throws if it is not a usable value wrapper.
    ///     Polecat never needs this — every path here resolves on demand — but Marten's
    ///     <c>StoreOptions.RegisterValueType</c> exists, so configuration source shared between the two
    ///     stores needs the call to compile and to mean the same thing. Registration is therefore
    ///     eager and loud rather than a no-op: a type that cannot be a value wrapper is a configuration
    ///     error worth hearing about at startup instead of at the first query.
    /// </summary>
    /// <exception cref="InvalidValueTypeException">
    ///     <paramref name="type" /> has no single readable value property over a supported inner type,
    ///     or no constructor or static factory that takes one.
    /// </exception>
    internal static ValueTypeInfo Register(Type type)
    {
        // Prime both cache entries so the id path and the member path agree from here on, and so a
        // later resolution never re-runs the reflective probe for this type.
        TryResolve(type);
        var info = TryResolve(type, allowReferenceTypes: true);

        if (info == null)
        {
            throw new InvalidValueTypeException(type,
                "Polecat requires a single public, readable property wrapping one of "
                + $"{string.Join(", ", SupportedInnerTypes.Select(x => x.Name))}, paired with either a "
                + "constructor or a public static factory method taking that value.");
        }

        return info;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "ValueTypeInfo.ForType reflects the candidate's public properties/constructors/static methods. Candidates reach here only as the declared type of a document member, which is preserved at the Schema.For<T>() registration boundary.")]
    [UnconditionalSuppressMessage("Trimming", "IL2067:DynamicallyAccessedMembers",
        Justification = "Same as above: the candidate Type originates from a document member on a registered document type.")]
    private static ValueTypeInfo? Resolve(Type type, bool allowReferenceTypes)
    {
        var candidate = Nullable.GetUnderlyingType(type) ?? type;

        // Cheap structural gate before the reflective probe. An id must be a struct — that is the
        // shape every generator emits and what the identity assigners assume — but a *member* may
        // also be a reference-type wrapper, which is what Vogen emits for
        // `[ValueObject<int>] public partial record Age`. Primitives and enums are never wrappers,
        // and the supported inner types would otherwise be probed on every member resolution.
        if (candidate.IsPrimitive || candidate.IsEnum) return null;
        if (!candidate.IsValueType && !(allowReferenceTypes && candidate.IsClass)) return null;
        if (SupportedInnerTypes.Contains(candidate)) return null;

        try
        {
            var info = CorrectBuilder(ValueTypeInfo.ForType(candidate), candidate);
            if (!SupportedInnerTypes.Contains(info.SimpleType)) return null;

            // A reference-type candidate only qualifies when it is built through a static factory
            // rather than a public constructor. Vogen's record value objects keep their constructor
            // private and expose `From`, which is exactly the shape that says "this is a value
            // object, not a nested entity"; an ordinary single-property DTO such as
            // `record Address(string City)` resolves through its public constructor and must keep
            // being treated as a nested object, not silently queried as a scalar.
            if (!candidate.IsValueType && info.Ctor != null) return null;

            return info;
        }
        catch
        {
            // ValueTypeInfo throws InvalidValueTypeException for anything that is not a single-value
            // wrapper. That is the common case for an ordinary struct member, so it is not an error.
            return null;
        }
    }

    /// <summary>
    ///     ValueTypeInfo.ForType takes the *first* public static method accepting one inner-typed
    ///     argument as the wrapper's builder, without checking what it returns. A type that pairs
    ///     <c>static X From(string)</c> with a nullable sibling such as <c>static X? FromNullable(string?)</c>
    ///     can therefore bind to the sibling, and every wrapper built from it fails with
    ///     "Expression of type 'Nullable&lt;X&gt;' cannot be used for return type 'X'". Prefer a builder
    ///     that actually returns the wrapper, and re-register the corrected info so the shared cache —
    ///     which the event store's stream-id resolution also reads — agrees. Marten fixes the same
    ///     defect in its own RegisterValueType (marten#4288).
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2070:DynamicallyAccessedMembers",
        Justification = "Enumerates public static methods on a candidate that reached here as the declared type of a member on a registered document type.")]
    private static ValueTypeInfo CorrectBuilder(ValueTypeInfo info, Type candidate)
    {
        if (info.Builder == null || info.Builder.ReturnType == candidate) return info;

        var better = candidate.GetMethods(BindingFlags.Static | BindingFlags.Public)
            .FirstOrDefault(x => x.ReturnType == candidate
                                 && x.GetParameters().Length == 1
                                 && x.GetParameters()[0].ParameterType == info.SimpleType);

        if (better == null) return info;

        var corrected = new ValueTypeInfo(info.OuterType, info.SimpleType, info.ValueProperty, better);
        ValueTypeInfo.Register(corrected);
        return corrected;
    }
}
