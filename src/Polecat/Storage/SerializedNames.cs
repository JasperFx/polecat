using System.Reflection;
using System.Text;
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
        return PathFor(chain.Select(m => JsonPathHelper.FormatMember(m, policy)), options.Serializer);
    }

    /// <summary>
    ///     Stored keys as a JSON path to put inside a SQL string literal.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         #916: a key that isn't a plain identifier has to be quoted, or <c>$.a-b</c> fails as a
    ///         badly formed path. That is any <c>[JsonPropertyName("a-b")]</c>, and every member of a store
    ///         on a kebab-case policy. The rule is <see cref="JsonPathHelper.Segment" />, which the patch
    ///         paths already use, because a path has to match how the serializer wrote the key.
    ///     </para>
    ///     <para>
    ///         ⚠️ A serializer on a relaxed encoder writes an apostrophe as it is, where the default one
    ///         writes <c>\u0027</c>, so the literal has to be allowed for here. Every site writes the path
    ///         as <c>'{path}'</c>. That is also why text outside the code page can't work on such a
    ///         serializer: it would need an <c>N'...'</c> at each of those sites.
    ///     </para>
    /// </remarks>
    public static string PathFor(IEnumerable<string> keys, ISerializer? serializer)
        => "$." + string.Join(".", keys.Select(x => Segment(x, serializer)));

    /// <summary>
    ///     One member's segment of a path, as <see cref="PathFor(IEnumerable{string}, ISerializer?)" /> wrote it.
    /// </summary>
    public static string SegmentFor(MemberInfo member, StoreOptions options)
        => Segment(For(member, options), options.Serializer);

    private static string Segment(string key, ISerializer? serializer)
        => JsonPathHelper.Segment(key, serializer).Replace("'", "''");

    /// <summary>
    ///     A path as the part of a column or index name that comes from it: <c>$.address.city</c> is
    ///     <c>address_city</c>.
    /// </summary>
    /// <remarks>
    ///     A character no name can hold is spelled out rather than dropped, because Weasel refuses it, and
    ///     because <c>a-b</c>, <c>a.b</c> as one key and <c>a_b</c> are three keys. It can only be reached
    ///     by a key that used to fail, so no column that exists has a name that changes.
    /// </remarks>
    public static string IdentifierFor(string jsonPath)
    {
        var name = new StringBuilder();
        var quoted = false;

        foreach (var character in jsonPath.Replace("$.", string.Empty))
        {
            switch (character)
            {
                case '"':
                    quoted = !quoted;
                    break;

                case '.' when !quoted:
                    name.Append('_');
                    break;

                default:
                    if (char.IsAsciiLetterOrDigit(character) || character == '_')
                    {
                        name.Append(char.ToLowerInvariant(character));
                    }
                    else
                    {
                        name.Append($"_x{(int)character:x}_");
                    }

                    break;
            }
        }

        return name.ToString();
    }
}
