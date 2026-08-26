using System.Reflection;
using System.Text.Json;
using Polecat.Patching;
using Polecat.Serialization;

namespace Polecat.Storage;

/// <summary>
///     #507 / #510: the one place that answers "what key does the serializer write for this member".
/// </summary>
/// <remarks>
///     <para>
///         Three consumers need the same answer and each used to work it out for itself: the LINQ
///         translator (<c>MemberFactory</c>), the patch path builder (<see cref="JsonPathHelper" />),
///         and the index DDL builder (<see cref="DocumentIndex" />). They drifted, twice, and both
///         times the symptom was silent — a computed column over a path the serializer never writes
///         is <c>NULL</c> for every row, so the index can never match, and queries keep returning
///         correct results by scanning <c>data</c>. #507 was the <c>[JsonPropertyName]</c> half and
///         #510 the naming-policy half.
///     </para>
///     <para>
///         The rule itself lives in <see cref="JsonPathHelper.FormatMember" />, matching
///         System.Text.Json: an explicit <c>[JsonPropertyName]</c> wins verbatim, and the policy is
///         NOT applied on top of it. This type only resolves which policy is in force.
///     </para>
/// </remarks>
internal static class SerializedNames
{
    /// <summary>
    ///     The naming policy the store's serializer actually applies. Mirrors
    ///     <c>MemberFactory</c>'s constructor: a custom <c>ISerializer</c> is opaque, so fall back to
    ///     the CamelCase default rather than guessing.
    /// </summary>
    public static JsonNamingPolicy? PolicyFor(StoreOptions options)
    {
        return options.Serializer is Serializer s
            ? s.Options.PropertyNamingPolicy
            : JsonNamingPolicy.CamelCase;
    }

    /// <summary>
    ///     The serialized key for a single member under the store's policy.
    /// </summary>
    public static string For(MemberInfo member, StoreOptions options)
        => JsonPathHelper.FormatMember(member, PolicyFor(options));

    /// <summary>
    ///     Renders a member chain (outermost first) as a JSON path, e.g. <c>$.address.city</c>.
    /// </summary>
    public static string PathFor(IReadOnlyList<MemberInfo> chain, StoreOptions options)
    {
        var policy = PolicyFor(options);
        return "$." + string.Join(".", chain.Select(m => JsonPathHelper.FormatMember(m, policy)));
    }
}
