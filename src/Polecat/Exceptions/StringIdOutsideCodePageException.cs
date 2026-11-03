using JasperFx.Core.Reflection;

namespace Polecat.Exceptions;

/// <summary>
///     Thrown instead of writing a string document id or stream key that its <c>varchar</c> column cannot
///     hold. SQL Server keeps something else with no error: <c>東京</c> and <c>大阪</c> are both <c>??</c>
///     under the default collation, so the alternative to this exception is two ids that are one row, with
///     the first document silently overwritten (#919).
/// </summary>
public class StringIdOutsideCodePageException : Exception
{
    private StringIdOutsideCodePageException(string id, Type? type, int codePage, string message) : base(message)
    {
        Id = id;
        Type = type;
        CodePage = codePage;
    }

    /// <summary>
    ///     The id or stream key as the application gave it.
    /// </summary>
    public string Id { get; }

    /// <summary>
    ///     The document type, or the aggregate type of a stream. Null for a stream started without one.
    /// </summary>
    public Type? Type { get; }

    /// <summary>
    ///     The code page of the column that could not hold it.
    /// </summary>
    public int CodePage { get; }

    internal static StringIdOutsideCodePageException ForDocument(string id, Type documentType, string table,
        int codePage)
        => new(id, documentType, codePage,
            $"The id '{id}' of document type '{documentType.FullNameInCode()}' can't be stored. "
            + Reason($"{table}.id", codePage)
            + "Use an id within that code page, or a Guid id.");

    internal static StringIdOutsideCodePageException ForStream(string key, Type? aggregateType, string table,
        int codePage)
        => new(key, aggregateType, codePage,
            $"The stream key '{key}'{(aggregateType == null ? "" : $" of aggregate type '{aggregateType.FullNameInCode()}'")} can't be stored. "
            + Reason($"{table}.id", codePage)
            + "Use a key within that code page, or Guid stream ids with StreamIdentity.AsGuid.");

    private static string Reason(string column, int codePage)
        => $"{column} is varchar, and code page {codePage} can't represent it, so SQL Server would keep a "
           + "different value with no error. Two different ids could then become one row, and the second write "
           + "would overwrite the first. ";
}
