using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Polecat.Serialization;

namespace Polecat.Linq.Members;

/// <summary>
///     Whether a member is known to be stored as a JSON object or an array, which has no value SQL can
///     sort or group by.
/// </summary>
/// <remarks>
///     <para>
///         #916: every scalar read is <c>JSON_VALUE</c>, which is NULL for an object or an array, so
///         <c>OrderBy(x =&gt; x.Address)</c> sorted nothing and <c>GroupBy(x =&gt; x.Address)</c> put every
///         row in one group, with no error.
///     </para>
///     <para>
///         ⚠️ <b>The declared type cannot decide this.</b> A class with a converter of its own can be
///         stored as a number or a string, and ordering by one works, so
///         <see cref="IQueryableMember.MayBeObject" /> is only a "may". The serializer's contract for the
///         type can: System.Text.Json reports a converter it did not build for an object or a collection
///         as <see cref="JsonTypeInfoKind.None" />, wherever the converter was declared, so what is left
///         is what it would write as an object itself. A member typed <c>object</c> is
///         <see cref="JsonTypeInfoKind.None" /> too, which is right: it can hold a scalar.
///     </para>
///     <para>
///         A converter on the member itself is not part of the type's contract, so one of those is taken
///         at its word, and a custom <see cref="ISerializer" /> says nothing about what it writes. Both are
///         left as they were, because refusing something that works is worse than this was.
///     </para>
/// </remarks>
internal static class StoredShape
{
    /// <summary>
    ///     <paramref name="member" />, unless it is known to have no value to sort or group by.
    /// </summary>
    /// <param name="member">The resolved member.</param>
    /// <param name="declared">The property or field it was resolved from.</param>
    /// <param name="serializer">The store's serializer.</param>
    /// <param name="position">Where the query uses it, for the message.</param>
    public static IQueryableMember AssertHasValue(IQueryableMember member, MemberInfo declared, ISerializer serializer,
        string position)
    {
        if (member.MayBeObject && IsObjectOrArray(declared, member.MemberType, serializer))
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate '{declared.Name}' {position}: it is stored as a JSON object or array, "
                + "which has no value SQL can sort or group by, so every row would read as NULL. Use one of "
                + "its members, or materialize the query and finish the work in memory.");
        }

        return member;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Asks for the contract the default serializer already built reflectively to store this member. A consumer with a source-generated resolver has it answered by that resolver instead.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Asks for the contract the default serializer already built reflectively to store this member. A consumer with a source-generated resolver has it answered by that resolver instead.")]
    private static bool IsObjectOrArray(MemberInfo declared, Type memberType, ISerializer serializer)
    {
        if (serializer is not Serializer { Options: var options }) return false;
        if (declared.IsDefined(typeof(JsonConverterAttribute), inherit: true)) return false;

        var type = Nullable.GetUnderlyingType(memberType) ?? memberType;

        // A resolver is only filled in when the options are first used, and this can be what comes first.
        var resolver = options.TypeInfoResolver ?? new DefaultJsonTypeInfoResolver();

        return resolver.GetTypeInfo(type, options)?.Kind
            is JsonTypeInfoKind.Object or JsonTypeInfoKind.Enumerable or JsonTypeInfoKind.Dictionary;
    }
}
