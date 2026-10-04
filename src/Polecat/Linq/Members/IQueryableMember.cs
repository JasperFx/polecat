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
    ///     Convert a CLR value to the appropriate SQL parameter value for this member.
    /// </summary>
    object? ConvertValue(object? value);
}
