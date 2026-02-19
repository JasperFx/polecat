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
    ///     Convert a CLR value to the appropriate SQL parameter value for this member.
    /// </summary>
    object? ConvertValue(object? value);
}
