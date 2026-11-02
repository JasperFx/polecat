using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     String equality that means what .NET means by it: <c>locator = @p</c>, and the same number of bytes.
/// </summary>
/// <remarks>
///     <para>
///         #877: <c>=</c> pads the shorter side with spaces, whatever the collation, so
///         <c>x.Text == "abc"</c> matched <c>"abc  "</c> and <c>x.Text == ""</c> matched a value of
///         nothing but spaces. It is the same trap as the <c>IsNullOrEmpty</c> half of #839, and
///         <c>DATALENGTH</c> is the same way out of it: the byte count of an <c>nvarchar</c> is the one
///         thing nothing pads. It also catches a character the collation gives no weight at all, which
///         <c>=</c> calls equal to nothing.
///     </para>
///     <para>
///         ⚠️ <b>A residual beside the <c>=</c>, never a replacement for it.</b> The <c>=</c> is what an
///         index seeks on, and <c>DATALENGTH</c> only runs against the rows it found, so the seek
///         survives. Measured over 100,000 rows against an <c>nvarchar(250)</c> computed column: Index
///         Seek at 3 reads, then the residual in a Filter above the lookup. A padding-free expression on
///         its own would be right and would scan.
///     </para>
///     <para>
///         Both sides have to be measured as <c>nvarchar</c>, because the count means nothing otherwise.
///         The value is, and so is a member read out of the JSON. An <c>id</c> column is <c>varchar</c>
///         at a byte per character and a <c>LOWER</c> over one stays that way, so a locator that may be
///         one goes through <see cref="Measured" /> first. The value's bytes are read from the parameter
///         rather than written into the SQL, so one statement serves every length.
///     </para>
/// </remarks>
internal class TextEqualityFilter : ISqlFragment
{
    private readonly string _locator;
    private readonly string _exact;
    private readonly bool _equal;
    private readonly object _value;

    /// <param name="locator">What is compared with <c>=</c>, and sought through any index.</param>
    /// <param name="exact">
    ///     What is measured: the same value, read so that it cannot have been narrowed. See
    ///     <see cref="Members.QueryableMemberExtensions.ExactTextLocator" />.
    /// </param>
    /// <param name="equal">True for equality, false for inequality.</param>
    /// <param name="value">The value.</param>
    public TextEqualityFilter(string locator, string exact, bool equal, object value)
    {
        _locator = locator;
        _exact = exact;
        _equal = equal;
        _value = value;
    }

    public void Apply(ICommandBuilder builder)
    {
        var (op, join) = _equal ? ("=", "AND") : ("!=", "OR");

        builder.Append($"({_locator} {op} ");
        var parameter = builder.AppendParameter(_value);
        builder.Append($" {join} {SameBytes(_exact, op, parameter.ParameterName)})");
    }

    /// <summary>
    ///     That <paramref name="exact" /> holds the same number of bytes as the parameter, or a different
    ///     number for <c>!=</c>.
    /// </summary>
    internal static string SameBytes(string exact, string op, string parameterName)
        => $"DATALENGTH({exact}) {op} DATALENGTH(@{parameterName.TrimStart('@')})";

    /// <summary>A locator made safe to measure, for one that may read a <c>varchar</c> column.</summary>
    internal static string Measured(string locator) => $"CAST({locator} AS nvarchar(max))";
}
