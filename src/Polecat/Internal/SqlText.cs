namespace Polecat.Internal;

/// <summary>
///     What text means in the SQL Polecat renders: how a string member is read in full, and what counts
///     as whitespace. The LINQ translator, the full-text trigger and the full-text backfill all have to
///     answer those the same way, and each failure here is a wrong answer rather than an error, so the
///     answers live in one place (#835, #839).
/// </summary>
internal static class SqlText
{
    /// <summary>
    ///     The most characters a bare <c>JSON_VALUE</c> can return. One past this and it answers NULL in
    ///     lax mode. It does not truncate, and that difference is what made a long member look absent.
    /// </summary>
    internal const int JsonValueCapacity = 4000;

    /// <summary>
    ///     A collation that compares the characters themselves. See <see cref="BinaryCollated" />.
    /// </summary>
    internal const string BinaryCollation = "Latin1_General_100_BIN2";

    /// <summary>
    ///     Every character <see cref="char.IsWhiteSpace(char)" /> answers true for, in code point order.
    ///     <c>sql_text_tests</c> holds this to the runtime over the whole BMP.
    /// </summary>
    internal const string Whitespace =
        "\t\n\v\f\r \u0085\u00A0\u1680"
        + "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A"
        + "\u2028\u2029\u202F\u205F\u3000";

    /// <summary>
    ///     <see cref="Whitespace" /> as a T-SQL expression, for the second argument of <c>TRIM</c> and
    ///     friends and the source set of <c>TRANSLATE</c>.
    /// </summary>
    /// <remarks>
    ///     Spelled as <c>NCHAR</c> calls rather than as a literal because the same text is stored in the
    ///     full-text trigger, where a literal of raw control characters and invisible separators would
    ///     be unreadable in <c>sys.sql_modules</c> and could not survive being copied out of a script.
    /// </remarks>
    internal static readonly string WhitespaceExpression =
        string.Join("+", Whitespace.Select(c => $"NCHAR({(int)c})"));

    /// <summary>
    ///     A string member read in full: <c>nvarchar(max)</c>, however long the value.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>Two spellings, because neither works for both storage shapes.</b> Measured over
    ///         200,000 documents on SQL Server 2025, MAXDOP 1, minimum of five runs:
    ///     </para>
    ///     <list type="table">
    ///         <item><term><c>json</c>, bare <c>JSON_VALUE</c></term><description>217ms</description></item>
    ///         <item><term><c>json</c>, <c>RETURNING nvarchar(max)</c></term><description>368ms</description></item>
    ///         <item><term><c>json</c>, <c>OPENJSON ... WITH</c></term><description>5417ms</description></item>
    ///         <item><term><c>nvarchar(max)</c>, bare <c>JSON_VALUE</c></term><description>719ms</description></item>
    ///         <item><term><c>nvarchar(max)</c>, <c>OPENJSON ... WITH</c></term><description>714ms</description></item>
    ///         <item><term><c>nvarchar(max)</c>, <c>CAST AS json</c> + <c>RETURNING</c></term><description>1803ms</description></item>
    ///     </list>
    ///     <para>
    ///         <c>RETURNING</c> is the only clause that is cheap on a native column and it is a syntax
    ///         error on an <c>nvarchar(max)</c> one, where the column cannot be cast to <c>json</c> at all
    ///         on SQL Server 2022. <c>OPENJSON</c> runs on both, costs nothing extra on
    ///         <c>nvarchar(max)</c> and 25x on <c>json</c>.
    ///     </para>
    ///     <para>
    ///         The <c>OPENJSON</c> form is a subquery, and SQL Server refuses a subquery in a
    ///         <c>GROUP BY</c>. That is why this is the predicate read rather than
    ///         <c>TypedLocator</c>, which still has to work there.
    ///     </para>
    /// </remarks>
    internal static string UnlimitedJsonString(string dataColumn, string jsonPath, bool nativeJson) =>
        nativeJson
            ? $"JSON_VALUE({dataColumn}, '{jsonPath}' RETURNING nvarchar(max))"
            : $"(SELECT pc_value FROM OPENJSON({dataColumn}) WITH (pc_value nvarchar(max) '{jsonPath}'))";

    /// <summary>
    ///     Whether a locator is the <c>OPENJSON</c> form of <see cref="UnlimitedJsonString" />, which SQL
    ///     Server refuses as a <c>GROUP BY</c> key and inside an aggregate.
    /// </summary>
    internal static bool IsSubquery(string locator) => locator.StartsWith("(SELECT ", StringComparison.Ordinal);

    /// <summary>
    ///     An expression matched by the characters it holds rather than by what the database's collation
    ///     says they are equivalent to.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b><c>TRIM</c> and <c>TRANSLATE</c> match through the collation</b>, so the list in
    ///         <see cref="Whitespace" /> is not enough on its own. Under the default
    ///         <c>SQL_Latin1_General_CP1_CI_AS</c> U+1680, U+202F and U+205F survive a <c>TRIM</c> that
    ///         names them, because that collation gives them no weight and so cannot find them; and
    ///         being width-insensitive it has <c>TRANSLATE</c> replace U+FF01 as though it were
    ///         <c>!</c>, which the ordinal C# tokenizer does not.
    ///     </para>
    ///     <para>
    ///         An expression keeps the collation it was given, so anything compared after this has to
    ///         be put back with <see cref="DatabaseCollated" />. Otherwise a predicate quietly turns
    ///         case-sensitive on a case-insensitive database.
    ///     </para>
    /// </remarks>
    internal static string BinaryCollated(string expression) => $"{expression} COLLATE {BinaryCollation}";

    /// <summary>Undoes <see cref="BinaryCollated" />, for a result that goes on to be compared.</summary>
    internal static string DatabaseCollated(string expression) => $"{expression} COLLATE DATABASE_DEFAULT";

    /// <summary>Both ends trimmed of <see cref="Whitespace" />, still binary collated.</summary>
    internal static string Trim(string expression) =>
        $"TRIM({WhitespaceExpression} FROM {BinaryCollated(expression)})";
}
