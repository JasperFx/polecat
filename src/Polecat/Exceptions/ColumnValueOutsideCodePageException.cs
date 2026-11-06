namespace Polecat.Exceptions;

/// <summary>
///     Thrown instead of writing a value that its <c>varchar</c> column cannot hold: an event's
///     <c>correlation_id</c> or <c>dotnet_type</c>, a string DCB tag value, or a document subclass alias in
///     <c>doc_type</c>. SQL Server keeps something else with no error, and which of them matters differs:
///     <c>correlation_id</c> just loses its text, a tag value that becomes <c>??</c> is the same value as
///     every other such tag (so a tag query finds events it should not, and a DCB boundary conflicts with
///     writes it has nothing to do with), and a type name that is no longer the type's name can no longer
///     be read back at all (#922). The sibling of <see cref="StringIdOutsideCodePageException" />.
/// </summary>
public class ColumnValueOutsideCodePageException : Exception
{
    internal ColumnValueOutsideCodePageException(string column, string value, int codePage) : base(
        $"The value '{value}' can't be stored in {column}. {column} is varchar, and code page {codePage} "
        + "can't represent it, so SQL Server would keep a different value with no error. "
        + "Use a value within that code page.")
    {
        Column = column;
        Value = value;
        CodePage = codePage;
    }

    /// <summary>
    ///     The column, as <c>correlation_id</c>, <c>dotnet_type</c>, <c>doc_type</c> or
    ///     <c>tag value (city)</c> for the tag type that names it.
    /// </summary>
    public string Column { get; }

    /// <summary>
    ///     The value as the application gave it.
    /// </summary>
    public string Value { get; }

    /// <summary>
    ///     The code page of the column that could not hold it.
    /// </summary>
    public int CodePage { get; }
}
