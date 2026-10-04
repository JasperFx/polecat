using System.Buffers;
using System.Collections;
using System.Text;
using System.Text.Json;
using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     #710 — a large <c>IN</c> list bound as ONE JSON array parameter and unpacked server-side with
///     <c>OPENJSON</c>, instead of one parameter per value.
/// </summary>
/// <remarks>
///     <para>
///         <b>SQL Server rejects a command carrying more than 2100 parameters outright</b>, so a
///         <c>Contains()</c> / <c>IsOneOf()</c> over a few thousand values threw rather than running.
///         The by-id paths never had this: <c>PolecatDocumentStorage</c> and
///         <c>DocumentSessionBase</c> have bound the whole list as one JSON array since #363, which is
///         why <c>LoadManyAsync</c> worked over the very list a <c>Where</c> clause could not. This is
///         the same shape, reached from the LINQ side. Marten has no equivalent problem because
///         PostgreSQL binds an array parameter (<c>= ANY(:p)</c>); SQL Server has no array parameter.
///     </para>
///     <para>
///         ⚠️ <b>Above a threshold rather than always, and the threshold is deliberately LOW.</b> A
///         short <c>IN (@p0, @p1, …)</c> hands the optimizer real literals and a real row count, where
///         <c>OPENJSON</c> is costed at a fixed guess regardless of the list's actual length — so
///         switching unconditionally would change the plan of every small <c>IsOneOf</c> for no
///         correctness gain. But a HIGH threshold would not close the hole either, because <b>the 2100
///         budget belongs to the command, not to this fragment</b>: two filters of 1500 values each are
///         both under any per-fragment ceiling and together over the server's. Until a fragment can ask
///         the builder how much budget is left (JasperFx/weasel#675), a threshold low enough that
///         summing across fragments cannot realistically reach 2100 is what actually holds — and it
///         costs nothing, because this is roughly where OPENJSON stops losing anyway.
///     </para>
/// </remarks>
internal static class JsonValueList
{
    /// <summary>
    ///     Above this many values, bind one JSON array instead of one parameter per value. See the
    ///     remarks for why this is ~100 rather than ~2000.
    /// </summary>
    internal const int Threshold = 100;

    internal static bool ShouldBindAsJsonArray(int count) => count > Threshold;

    /// <summary>
    ///     OPENJSON's own <c>value</c> column type. The fallback when the locator is left uncast — a
    ///     bare <c>JSON_VALUE</c>, which SQL Server also types as nvarchar, so both sides agree and
    ///     neither is converted.
    /// </summary>
    private const string UntypedColumn = "nvarchar(4000)";

    /// <summary>
    ///     Appends <c>IN (SELECT [value] FROM OPENJSON(@p) WITH ([value] &lt;type&gt; '$'))</c>, binding
    ///     the values as a single JSON array parameter.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b><paramref name="locatorSqlType" /> must be the type the LOCATOR produces, and
    ///         getting it wrong is the #363 trap in one direction or a wrong answer in the other.</b>
    ///         OPENJSON's untyped <c>value</c> is <c>nvarchar(4000)</c>; compared against a
    ///         <c>varchar(250)</c> computed column that puts <c>CONVERT_IMPLICIT</c> on the COLUMN and
    ///         scans, which is exactly what #363 removed from the by-id path and what makes the
    ///         computed-column indexes of #223/#684 dead weight. Guessing a narrow type instead
    ///         silently TRUNCATES values over an unindexed string member, which is worse — a wrong
    ///         result rather than a slow one. So the type is read from the member
    ///         (<c>IQueryableMember.LocatorSqlType</c>) rather than inferred here.
    ///     </para>
    ///     <para>
    ///         <c>WITH</c> rather than <c>CAST(value AS …)</c>: it types the column instead of wrapping
    ///         it in an expression, which is one less thing between the join and an index seek.
    ///     </para>
    /// </remarks>
    internal static void AppendInClause(ICommandBuilder builder, string locator, IList values,
        string? locatorSqlType, Func<object?, object?> convert)
    {
        var json = WriteJsonArray(values, convert);

        builder.Append(locator);
        builder.Append(" IN (SELECT [value] FROM OPENJSON(");
        builder.AppendParameter(json, System.Data.SqlDbType.NVarChar);
        builder.Append($") WITH ([value] {locatorSqlType ?? UntypedColumn} '$'))");
    }

    /// <summary>
    ///     The values as a JSON array, written manually so no serializer configuration can change the
    ///     text SQL Server has to read back.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The input is whatever <c>IQueryableMember.ConvertValue</c> yields, which is already the
    ///         SQL-ready scalar — an unwrapped strong-typed id, an enum as its int or its
    ///         policy-cased name, a bool as the <c>"true"</c>/<c>"false"</c> text the JSON holds. So
    ///         this handles scalars, not documents.
    ///     </para>
    ///     <para>
    ///         ⚠️ <b>A date is the sharp edge.</b> <c>Utf8JsonWriter</c> writes ISO 8601 round-trip,
    ///         which is what the <c>WITH</c> clause's date/time types parse — and critically it keeps
    ///         the OFFSET on a <c>DateTimeOffset</c>. A format that dropped it would shift the
    ///         comparison silently rather than failing.
    ///     </para>
    ///     <para>
    ///         ⚠️ <b>A null in the list stays a JSON null</b>, and that is the faithful choice rather
    ///         than a convenient one: <c>IN</c> never matches <c>NULL</c>, which is exactly what the
    ///         one-parameter-per-value form did too. Dropping nulls here would be a behaviour change
    ///         dressed as a detail.
    ///     </para>
    /// </remarks>
    internal static string WriteJsonArray(IList values, Func<object?, object?> convert)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();

            for (var i = 0; i < values.Count; i++)
            {
                Write(writer, convert(values[i]));
            }

            writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static void Write(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case string text:
                writer.WriteStringValue(text);
                break;
            case Guid guid:
                writer.WriteStringValue(guid);
                break;
            case bool flag:
                // Defensive: a boolean member's ConvertValue already yields "true"/"false" text,
                // because that is what the JSON document holds and what JSON_VALUE returns.
                writer.WriteStringValue(flag ? "true" : "false");
                break;
            case int i: writer.WriteNumberValue(i); break;
            case long l: writer.WriteNumberValue(l); break;
            case short sh: writer.WriteNumberValue(sh); break;
            case byte b: writer.WriteNumberValue(b); break;
            case decimal d: writer.WriteNumberValue(d); break;
            case double db: writer.WriteNumberValue(db); break;
            case float f: writer.WriteNumberValue(f); break;
            case DateTimeOffset dto: writer.WriteStringValue(dto); break;
            case DateTime dt: writer.WriteStringValue(dt); break;
            case DateOnly date: writer.WriteStringValue(date.ToString("yyyy-MM-dd")); break;
            case TimeOnly time: writer.WriteStringValue(time.ToString("HH:mm:ss.fffffff")); break;
            case Enum e: writer.WriteNumberValue(Convert.ToInt64(e)); break;
            default:
                // Deliberately a refusal rather than ToString(). A type nobody has typed the OPENJSON
                // column for would otherwise compare as whatever its ToString happens to produce,
                // which is a wrong answer arrived at silently -- and this fragment is reached only
                // above the threshold, so it would appear in the field and never in a small test.
                throw new BadLinqExpressionException(
                    $"Polecat cannot bind a '{value.GetType().FullName}' into a large IN list. Values "
                    + "over the threshold travel as one JSON array, which supports strings, Guids, "
                    + "numbers, enums, booleans and date/time types. Use a smaller list, or compare on "
                    + "a member of one of those types.");
        }
    }
}
