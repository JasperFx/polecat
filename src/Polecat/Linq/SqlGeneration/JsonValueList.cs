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
///         correctness gain.
///     </para>
///     <para>
///         <b>#721: that low threshold is the FLOOR, and the command's own parameter count is now the
///         ceiling.</b> The 2100 budget belongs to the command, not to this fragment, and a fragment
///         could not see past itself until Weasel 9.40.0 exposed
///         <c>ICommandBuilder.ParameterCount</c> (weasel#675, filed off #710). So the decision is made
///         twice: over <see cref="Threshold" /> values, bind an array because the list is big enough
///         that OPENJSON stops losing on its own merits; under it, bind an array anyway when what is
///         ALREADY bound on this command plus this list would cross
///         <c>SqlServerMigrator.MaxParametersPerCommand</c>.
///     </para>
///     <para>
///         ⚠️ <b>Note which case the ceiling actually fixes, because it is not the one weasel#675 was
///         filed for.</b> Two filters of 1500 values each — the motivating example — were never broken
///         once #710 shipped: 1500 is over the floor, so each fragment already became an array on its
///         own. The residual hole is the opposite shape, <i>many small</i> lists each under the floor
///         summing past the budget, which takes roughly twenty-plus filters of a hundred values. That
///         is the case no per-fragment threshold can reach at any value, because lowering it far
///         enough to cover twenty fragments would penalize every single-digit <c>IsOneOf</c> in the
///         store.
///     </para>
///     <para>
///         A builder compiled against an older Weasel returns
///         <see cref="Weasel.Core.ICommandBuilder.UnknownParameterCount" /> (-1), which means "no
///         information" rather than "none bound" — so the ceiling is skipped and the floor alone
///         decides, exactly as before #721. Treating -1 as zero would be the silently-wrong version
///         of this.
///     </para>
/// </remarks>
internal static class JsonValueList
{
    /// <summary>
    ///     Above this many values, bind one JSON array instead of one parameter per value. See the
    ///     remarks for why this is ~100 rather than ~2000, and why it is a floor rather than the whole
    ///     decision.
    /// </summary>
    internal const int Threshold = 100;

    /// <summary>
    ///     The per-command parameter budget this fragment will not help exceed —
    ///     <c>SqlServerMigrator.MaxParametersPerCommand</c>, read rather than restated so a Weasel
    ///     change moves both together.
    /// </summary>
    /// <remarks>
    ///     Weasel's 2000 is already below SQL Server's hard 2100, and that slack does real work here:
    ///     parameters bound AFTER this fragment — the rest of the where clause, a TOP, a tenant id —
    ///     are not visible to it, so a budget equal to the limit would leave no room for them.
    /// </remarks>
    internal static int Budget { get; } = new SqlServerMigrator().MaxParametersPerCommand;

    /// <summary>
    ///     Whether <paramref name="count" /> values should travel as one JSON array on this command.
    ///     See the remarks on the class for the floor-and-ceiling split.
    /// </summary>
    internal static bool ShouldBindAsJsonArray(Weasel.Core.ICommandBuilder builder, int count)
    {
        // The floor: big enough that OPENJSON is the better plan on its own merits.
        if (count > Threshold) return true;

        // The ceiling, which needs the command's running total (#721, weasel#675).
        var alreadyBound = builder.ParameterCount;
        if (alreadyBound == Weasel.Core.ICommandBuilder.UnknownParameterCount) return false;

        return alreadyBound + count > Budget;
    }

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
