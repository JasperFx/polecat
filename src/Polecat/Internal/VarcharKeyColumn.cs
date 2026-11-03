using System.Collections.Concurrent;
using System.Text;

namespace Polecat.Internal;

/// <summary>
///     What a <c>varchar</c> key column keeps of a string (#919).
/// </summary>
/// <remarks>
///     <para>
///         <b>Two code pages, and either can lose the value.</b> A <c>varchar</c> parameter is encoded with
///         the database's default collation before it leaves the client, and the column then keeps what its
///         own collation holds. They're usually the same, because a column takes the default when it's
///         created, but <c>ALTER DATABASE ... COLLATE</c> moves the default and leaves every existing column
///         where it was. That's the obvious thing to try after this refusal, so going by the database alone
///         would let the id back into a column that can't hold it.
///     </para>
///     <para>
///         <b>Whether the value survives, not whether it's ASCII.</b> <c>café</c> and <c>ñ</c> are in code
///         page 1252 and have always worked, while under <c>Japanese_CI_AS</c> <c>東京</c> does and
///         <c>café</c> becomes <c>cafe</c>. A <c>?</c> in the result isn't the test either: outside what it
///         holds SQL Server takes the nearest thing it has, so <c>ā</c> is stored as <c>a</c>, which is
///         worse than <c>??</c> because that's somebody else's id. The encoding is asked for with a
///         replacement fallback because that turns the best fit off, and the value has to come back as it
///         went in. Under <c>_UTF8</c> only half a surrogate pair doesn't. <c>varchar_key_column_tests</c>
///         puts every character of the basic plane through the server on every code page a database can
///         have, and the two agree on all of them.
///     </para>
///     <para>
///         From <see cref="CodePagesEncodingProvider" /> directly rather than through
///         <see cref="Encoding.RegisterProvider" />, so that nothing process-wide changes. The provider is
///         part of the shared framework and doesn't reflect, so it's fine under Native AOT.
///     </para>
///     <para>
///         A code page that can't be read (no collation, or one the provider doesn't have) keeps what it's
///         given: an answer that can't be trusted mustn't refuse an id that may well work.
///     </para>
/// </remarks>
internal sealed class VarcharKeyColumn
{
    private const int Utf8 = 65001;

    private static readonly ConcurrentDictionary<int, Encoding?> _encodings = new();

    private readonly int _databaseCodePage;
    private readonly Encoding? _database;
    private readonly int _columnCodePage;
    private readonly Encoding? _column;

    public VarcharKeyColumn(int databaseCodePage, int columnCodePage)
    {
        _databaseCodePage = databaseCodePage;
        _database = encodingFor(databaseCodePage);
        _columnCodePage = columnCodePage;
        _column = encodingFor(columnCodePage);
    }

    /// <summary>
    ///     The code page that would change <paramref name="value" />, or null if it would come back as it is.
    /// </summary>
    public int? CodePageLosing(string value)
    {
        if (Ascii.IsValid(value)) return null;
        if (_database != null && !survives(_database, value)) return _databaseCodePage;
        if (_column != null && !survives(_column, value)) return _columnCodePage;

        return null;
    }

    private static bool survives(Encoding encoding, string value)
        => encoding.GetString(encoding.GetBytes(value)) == value;

    private static Encoding? encodingFor(int codePage) => _encodings.GetOrAdd(codePage, static x =>
        x == Utf8
            ? new UTF8Encoding(encoderShouldEmitUTF8Identifier: false)
            : CodePagesEncodingProvider.Instance.GetEncoding(x, new EncoderReplacementFallback("?"),
                new DecoderReplacementFallback("?")));
}
