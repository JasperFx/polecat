using System.Collections;

namespace Polecat.Linq.Members;

/// <summary>
///     Represents a queryable member (property) of a document, mapping to a SQL expression.
/// </summary>
internal interface IQueryableMember
{
    /// <summary>
    ///     The CLR type of this member.
    /// </summary>
    Type MemberType { get; }

    /// <summary>
    ///     The SQL locator with appropriate CAST for typed comparisons.
    ///     E.g., "CAST(JSON_VALUE(data, '$.age') AS int)" or "id".
    /// </summary>
    string TypedLocator { get; }

    /// <summary>
    ///     The raw SQL locator without CAST, used for IS NULL checks.
    ///     E.g., "JSON_VALUE(data, '$.age')" or "id".
    /// </summary>
    /// <remarks>
    ///     #835: for a string member this reads the value in full, because it is what every text
    ///     predicate is built on, and a bare <c>JSON_VALUE</c> answers NULL past 4000 characters. See
    ///     <see cref="Polecat.Internal.SqlText.UnlimitedJsonString" />. <see cref="TypedLocator" /> stays
    ///     as it was, because a <c>GROUP BY</c> cannot take the <c>nvarchar(max)</c> storage form.
    /// </remarks>
    string RawLocator { get; }

    /// <summary>
    ///     Whether this member is a boolean (requires "true"/"false" string comparison).
    /// </summary>
    bool IsBoolean { get; }

    /// <summary>
    ///     The SQL type <see cref="TypedLocator" /> produces, or null when the locator is left uncast —
    ///     a bare <c>JSON_VALUE</c>, which SQL Server types as <c>nvarchar</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         #710: this exists so a fragment binding a value LIST can type the other side of the
    ///         comparison to exactly what the locator yields. <see cref="MemberFactory" /> already
    ///         works this out — including the #223 rewrite onto an index's computed column, whose type
    ///         the caller may have overridden — and used to discard it.
    ///     </para>
    ///     <para>
    ///         ⚠️ <b>Guessing it instead is the #363 trap in both directions.</b> Type the list side too
    ///         WIDE and the comparison puts <c>CONVERT_IMPLICIT</c> on the COLUMN, which scans and makes
    ///         the computed-column indexes of #223/#684 dead weight. Type it too NARROW — a
    ///         <c>varchar(250)</c> guess over an unindexed string member — and values are silently
    ///         truncated, which is a wrong answer rather than a slow one. Reading the locator's own type
    ///         is the only thing that is right in every case.
    ///     </para>
    /// </remarks>
    string? LocatorSqlType { get; }

    /// <summary>
    ///     How many characters <see cref="TypedLocator" /> can carry for a string member: 4000 for a bare
    ///     <c>JSON_VALUE</c>, or the width of the computed column it was rewritten onto. Null for
    ///     anything that is not text. See <see cref="QueryableMemberExtensions.ComparisonLocator" />.
    /// </summary>
    int? TextCapacity => null;

    /// <summary>
    ///     Convert a CLR value to the appropriate SQL parameter value for this member.
    /// </summary>
    object? ConvertValue(object? value);
}

internal static class QueryableMemberExtensions
{
    /// <summary>
    ///     #835: <see cref="IQueryableMember.TypedLocator" /> when it is safe to compare
    ///     <paramref name="value" /> through it, otherwise the full read in
    ///     <see cref="IQueryableMember.RawLocator" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>Over an index, the index wins</b> unless it cannot hold the value. A computed column is
    ///         the reason the index exists, and a value wider than the column can never be found in it,
    ///         so that one comparison goes to the document. The column's declared width is otherwise the
    ///         contract: a stored value wider than it reads NULL (native) or truncated (nvarchar), and a
    ///         <c>!=</c> over the column sees that. <c>Index(..., SqlType = ...)</c> is what widens it.
    ///     </para>
    ///     <para>
    ///         <b>Over a bare <c>JSON_VALUE</c>, only a positive equality qualifies</b>, and only for a
    ///         value that fits. A stored value over the limit reads NULL, so the comparison is UNKNOWN and
    ///         the row is dropped, which is right: something longer than the sought value cannot equal it.
    ///         Under a <c>NOT</c> the same UNKNOWN still drops the row, and now that is wrong. The same goes
    ///         for <c>!=</c>. On native storage this saves 40% of the CPU of a scan (217ms against 368ms over
    ///         200,000 documents), and the optimizer does not seek a JSON index for this comparison in
    ///         either form, so nothing index-backed turns on it.
    ///     </para>
    /// </remarks>
    /// <param name="member">The member being compared.</param>
    /// <param name="op">The SQL comparison operator.</param>
    /// <param name="positive">False when the predicate sits under a <c>NOT</c>.</param>
    /// <param name="value">The value as written in the query, before <c>ConvertValue</c>.</param>
    internal static string ComparisonLocator(this IQueryableMember member, string op, bool positive, object? value)
        => member.Choose(positive && op == "=", new[] { value });

    /// <summary>
    ///     <see cref="ComparisonLocator" /> for an <c>IN</c> over <paramref name="values" />, which is a
    ///     chain of equalities and is safe or not on the same terms.
    /// </summary>
    /// <param name="member">The member being compared.</param>
    /// <param name="positive">False when the predicate sits under a <c>NOT</c>.</param>
    /// <param name="values">The values as written in the query, before <c>ConvertValue</c>.</param>
    internal static string ListLocator(this IQueryableMember member, bool positive, IEnumerable values)
        => member.Choose(positive, values);

    private static string Choose(this IQueryableMember member, bool unknownDropsTheRowCorrectly, IEnumerable values)
    {
        if (member.TextCapacity is not { } capacity) return member.TypedLocator;

        var indexed = member.LocatorSqlType != null;
        if (!indexed && !unknownDropsTheRowCorrectly) return member.RawLocator;

        foreach (var value in values)
        {
            if (member.ConvertValue(value) is string text && text.Length > capacity)
            {
                return member.RawLocator;
            }
        }

        return member.TypedLocator;
    }
}
