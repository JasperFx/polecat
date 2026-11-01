using Polecat.Internal;
using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     One key of a server-side JSON object.
/// </summary>
/// <param name="Key">The JSON key, already through the serializer's naming rules.</param>
/// <param name="Value">The SQL expression producing the value.</param>
/// <param name="ClrType">The CLR type the value lands in, which decides what a NULL means.</param>
/// <param name="SqlType">The SQL type <paramref name="Value" /> yields, or null for nvarchar or a number.</param>
internal sealed record JsonObjectMember(string Key, ISqlFragment Value, Type ClrType, string? SqlType);

/// <summary>
///     A native SQL Server JSON object for a projection. SQL Server does the encoding, so a value's
///     SQL type decides its JSON form: numbers stay numbers, strings stay quoted and escaped, and a
///     <c>uniqueidentifier</c> or a date comes out as a string.
/// </summary>
/// <remarks>
///     <para>
///         Shared by the JSON-streaming <c>Select()</c> paths and by GroupBy, which used to assemble
///         its JSON by hand with <c>CONCAT</c> and got everything but a number or a quote-free string
///         wrong (#837).
///     </para>
///     <para>
///         ⚠️ <b>Whether a SQL NULL becomes <c>"key": null</c> or no key at all is decided PER
///         MEMBER, from the CLR type (#722), and neither answer is right for both.</b>
///     </para>
///     <para>
///         This used to be one <c>JSON_OBJECT(...)</c> on the default <c>NULL ON NULL</c>, which
///         emitted <c>"key": null</c> for a key that is simply ABSENT from the stored document —
///         the ordinary case of a document type that gained a property after rows were written.
///         A consumer deserializing that into a non-nullable value type throws, while the same
///         stored document loads fine as a whole document and the same member projects fine on
///         its own, which is what made it confusing to diagnose (marten#5461).
///     </para>
///     <para>
///         ⚠️ <b>And <c>ABSENT ON NULL</c> is not the fix — that is marten#5499.</b> Stripping
///         every null turned a <c>DateOnly?</c> that was LEGITIMATELY stored as null into an
///         absent key, trading a throw for a silently different value. <c>JSON_VALUE</c> cannot
///         tell absent from present-and-null — both are SQL NULL — so no single object-level
///         setting can be correct for both shapes.
///     </para>
///     <para>
///         So the members are split by what their CLR type can represent:
///     </para>
///     <list type="bullet">
///         <item>
///             a <b>nullable or reference</b> member keeps <c>"key": null</c>, because null is a
///             value it can hold and a consumer may distinguish it from absence;
///         </item>
///         <item>
///             a <b>non-nullable value type</b> has the key omitted, because null is a value it
///             CANNOT hold — absence deserializes to the CLR default, which is the only
///             representable answer and the one the whole-document read already gives.
///         </item>
///     </list>
///     <para>
///         <b><c>JSON_MODIFY</c> in lax mode is what makes that per-member.</b> Setting a key to
///         a NULL value in lax mode DELETES it, and sets it when the value is non-null — exactly
///         the conditional-omit this needs, with no per-type default literal to get wrong and no
///         string surgery over two objects. The nullable members form the base
///         <c>JSON_OBJECT</c>, which keeps its nulls; each non-nullable member is layered on.
///     </para>
///     <para>
///         ⚠️ <b>JSON_MODIFY is pickier than JSON_OBJECT about its value.</b> It refuses
///         <c>uniqueidentifier</c> and every date and time type outright (Msg 8116), so a
///         non-nullable <see cref="Guid" /> or <see cref="DateTime" /> member goes through
///         <c>CONVERT(nvarchar, x, 126)</c>, which yields the same text JSON_OBJECT would have
///         written. That is what makes <c>Select(x =&gt; new { x.Id, x.Name })</c> streamable at all.
///     </para>
///     <para>
///         ⚠️ Two consequences worth knowing. The emitted <b>key order changes</b> — nullable
///         members first, then the rest — which is not semantically meaningful in JSON and which
///         System.Text.Json does not care about, but which a test asserting exact JSON text would
///         see. And a projection of N non-nullable members nests N <c>JSON_MODIFY</c> calls rather
///         than one <c>JSON_OBJECT</c>; the alternative was a per-CLR-type default literal
///         (<c>CAST(0 AS bit)</c>, <c>'0001-01-01'</c>, an enum with no zero member …), which is a
///         long tail where every entry is a silent wrong answer if it is wrong — the exact class
///         of bug this is fixing.
///     </para>
///     <para>
///         The last row of #722's truth table — a key present as null projected into a
///         non-nullable value type — comes out as the CLR default rather than a throw. That is
///         deliberate and is what the issue calls "arguably correct to throw on anyway": nothing
///         distinguishes it from absence at the SQL layer, so it gets absence's answer.
///     </para>
/// </remarks>
internal sealed class JsonObjectFragment : ISqlFragment
{
    private readonly List<JsonObjectMember> _preserveNull = [];
    private readonly List<JsonObjectMember> _omitWhenNull = [];

    public JsonObjectFragment(IEnumerable<JsonObjectMember> members)
    {
        foreach (var member in members)
        {
            (CanRepresentNull(member.ClrType) ? _preserveNull : _omitWhenNull).Add(member);
        }
    }

    public void Apply(ICommandBuilder builder)
    {
        foreach (var _ in _omitWhenNull)
        {
            builder.Append("JSON_MODIFY(");
        }

        builder.Append("JSON_OBJECT(");
        for (var i = 0; i < _preserveNull.Count; i++)
        {
            if (i > 0) builder.Append(", ");
            builder.Append($"'{SqlEscaping.LiteralBody(_preserveNull[i].Key)}': ");
            _preserveNull[i].Value.Apply(builder);
        }

        builder.Append(")");

        foreach (var member in _omitWhenNull)
        {
            // Lax mode (the default, and NOT spelled out: 'lax' is implied and SQL Server accepts the
            // bare path) deletes the key when the value is NULL. The key is quoted so a naming policy
            // or [JsonPropertyName] that produced a space or a dot cannot change which path this is.
            builder.Append($", '$.\"{JsonPathKey(member.Key)}\"', ");

            if (TravelsAsText(member.SqlType))
            {
                builder.Append("CONVERT(nvarchar(50), ");
                member.Value.Apply(builder);
                builder.Append(", 126)");
            }
            else
            {
                member.Value.Apply(builder);
            }

            builder.Append(")");
        }
    }

    /// <summary>
    ///     Whether <c>null</c> is a value this member's CLR type can hold — a reference type or a
    ///     <see cref="Nullable{T}" />. See the remarks on the class.
    /// </summary>
    private static bool CanRepresentNull(Type clrType)
        => !clrType.IsValueType || Nullable.GetUnderlyingType(clrType) != null;

    /// <summary>A type JSON_MODIFY refuses as a value. See the remarks on the class.</summary>
    private static bool TravelsAsText(string? sqlType)
    {
        if (sqlType is null) return false;

        var name = sqlType.Split('(')[0];
        return name.Equals("uniqueidentifier", StringComparison.OrdinalIgnoreCase)
               || SqlTypeMap.IsDateTimeFamily(name);
    }

    /// <summary>
    ///     A JSON key escaped for use inside a double-quoted <c>JSON_MODIFY</c> path, which is then
    ///     itself inside a SQL string literal.
    /// </summary>
    private static string JsonPathKey(string key)
        => SqlEscaping.LiteralBody(key).Replace("\"", "\\\"");
}
