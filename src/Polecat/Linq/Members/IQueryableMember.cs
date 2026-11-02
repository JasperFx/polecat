using System.Collections;
using System.Data;
using System.Text;
using Polecat.Internal;
using Polecat.Linq.SqlGeneration;

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
    ///     What <c>IS NULL</c> has to be asked of. <see cref="RawLocator" /> for anything JSON holds as a
    ///     scalar, which is what <c>JSON_VALUE</c> reads.
    /// </summary>
    /// <remarks>
    ///     #875: <c>JSON_VALUE</c> is NULL for an object and for an array, present or not, so asking it
    ///     about one called every row null. See <see cref="SqlText.StoredValue" />.
    /// </remarks>
    string NullLocator => RawLocator;

    /// <summary>
    ///     Whether the declared type can be stored as a JSON object or array, which the scalar locators
    ///     read as NULL. A class with a converter of its own can still turn out to hold a scalar, so
    ///     this is a "may".
    /// </summary>
    bool MayBeObject => false;

    /// <summary>
    ///     A string member read through a bare <c>JSON_VALUE</c>, for when <see cref="TypedLocator" /> has
    ///     been rewritten onto a computed column and cannot be trusted to hold the whole stored value. It
    ///     needs no subquery on either storage shape, and it is NULL past 4000 characters. See
    ///     <see cref="QueryableMemberExtensions.ExactTextLocator" />.
    /// </summary>
    string? BareTextLocator => null;

    /// <summary>
    ///     The value as a JSON projection should write it, when that differs from
    ///     <see cref="TypedLocator" />.
    /// </summary>
    string? JsonLocator => null;

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

    /// <summary>
    ///     The predicate that this member <paramref name="op" /> <paramref name="value" />.
    /// </summary>
    /// <param name="member">The member being compared.</param>
    /// <param name="op">The SQL comparison operator.</param>
    /// <param name="positive">False when the predicate sits under a <c>NOT</c>.</param>
    /// <param name="value">The value as written in the query, before <c>ConvertValue</c>.</param>
    internal static ISqlFragment Comparison(this IQueryableMember member, string op, bool positive, object value)
    {
        var locator = member.ComparisonLocator(op, positive, value);
        var converted = member.ConvertValue(value)!;
        var dbType = member.BindingFor(locator, converted);

        return op is "=" or "!=" && member.TextCapacity != null && converted is string
            ? new TextEqualityFilter(locator, member.ExactTextLocator(locator), op == "=", converted, dbType)
            : new ComparisonFilter(locator, op, converted, dbType);
    }

    /// <summary>
    ///     <see cref="SqlDbType.VarChar" /> when <paramref name="locator" /> is a <c>varchar</c> column and
    ///     <paramref name="value" /> can be bound as one without changing, otherwise null for the default.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         #877, the #363 rule on the LINQ path. A string binds as <c>nvarchar</c>, which outranks
    ///         <c>varchar</c>, so under a SQL collation the conversion lands on the COLUMN and an index on
    ///         it cannot be sought. That is every default <c>Index(x =&gt; x.Name)</c>: an Index Scan at 451
    ///         reads over 100,000 rows, against an Index Seek at 3 with the parameter typed to match.
    ///     </para>
    ///     <para>
    ///         ASCII only, because that is the part of a <c>varchar</c> parameter that means the same thing
    ///         under every code page, and this cannot see which one the database has. A value outside it
    ///         stays <c>nvarchar</c>: bound as <c>varchar</c>, <c>東京</c> would become <c>??</c> and match
    ///         other values that did the same. See <see cref="Choose" /> for where a text member's goes.
    ///     </para>
    ///     <para>
    ///         ⚠️ Not for a short <c>IN</c>, which stays <c>nvarchar</c> and scans. Twenty-five of them over
    ///         a <c>varchar</c> primary key turned 2,500 seek ranges into Msg 8623, the optimizer running out
    ///         of resources, on a shape that scanned without complaint.
    ///     </para>
    /// </remarks>
    internal static SqlDbType? BindingFor(this IQueryableMember member, string locator, object? value)
        => value is string text && locator == member.TypedLocator && IsVarChar(member.LocatorSqlType)
                                && Ascii.IsValid(text)
            ? SqlDbType.VarChar
            : null;

    private static bool IsVarChar(string? sqlType)
        => sqlType != null && sqlType.StartsWith("varchar", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    ///     What to measure to find out that <paramref name="locator" />, a text read of this member,
    ///     really holds what it appears to.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Usually the locator itself. A computed column is the exception: it can be narrower than
    ///         the document, and <c>nvarchar(max)</c> storage truncates to fit it rather than answering
    ///         NULL (#877), so <c>"aaa…tail"</c> equals its own first 250 characters there. That has to be
    ///         checked against the document, which is what <see cref="IQueryableMember.BareTextLocator" />
    ///         reads.
    ///     </para>
    ///     <para>
    ///         Bare rather than <see cref="IQueryableMember.RawLocator" />, because it is enough and it is
    ///         not a subquery. The value going through the column fits it, so a stored value past 4000
    ///         characters cannot equal it, and the NULL that reads as is the right answer. And the
    ///         <c>OPENJSON</c> form of the full read would get an indexed comparison refused inside
    ///         <c>g.Count(...)</c>, where it has always worked.
    ///     </para>
    /// </remarks>
    internal static string ExactTextLocator(this IQueryableMember member, string locator)
        => locator == member.RawLocator ? locator : member.BareTextLocator ?? locator;

    private static string Choose(this IQueryableMember member, bool unknownDropsTheRowCorrectly, IEnumerable values)
    {
        if (member.TextCapacity is not { } capacity) return member.TypedLocator;

        var indexed = member.LocatorSqlType != null;
        if (!indexed && !unknownDropsTheRowCorrectly) return member.RawLocator;

        // #877: a varchar column can no more hold a value outside its code page than one wider than
        // itself. It stored 東京 as ??, so comparing through it found nothing, with no error.
        var narrow = IsVarChar(member.LocatorSqlType);

        foreach (var value in values)
        {
            if (member.ConvertValue(value) is string text && (text.Length > capacity || (narrow && !Ascii.IsValid(text))))
            {
                return member.RawLocator;
            }
        }

        return member.TypedLocator;
    }
}
