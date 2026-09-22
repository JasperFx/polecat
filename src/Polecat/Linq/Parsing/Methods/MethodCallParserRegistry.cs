using System.Linq.Expressions;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Registry of method call parsers for LINQ WHERE clause support.
/// </summary>
internal static class MethodCallParserRegistry
{
    private static readonly IMethodCallParser[] Parsers =
    [
        new StringContains(),
        new StringStartsWith(),
        new StringEndsWith(),
        new StringEquals(),
        new StringIsNullOrEmpty(),
        new StringToLower(),
        new StringToUpper(),
        new StringTrim(),
        new IsOneOf(),
        new FullTextSearchMethods(),
        new EnumerableContains(),
        new IsEmpty(),
        new ObjectEquals()
    ];

    /// <summary>
    ///     Human-readable inventory of what <see cref="FindParser" /> can translate, for refusal
    ///     messages. #656: Fisher's habit — a refusal that names what IS supported is actionable;
    ///     one that names only what was rejected sends the caller to the source.
    /// </summary>
    /// <remarks>
    ///     ⚠️ Keep this in step with <c>Parsers</c> above. It is a message, not a lookup, so nothing
    ///     breaks if it drifts — which is exactly why it is worth saying so here.
    /// </remarks>
    public const string SupportedCalls =
        "string Contains/StartsWith/EndsWith/Equals/IsNullOrEmpty/ToLower/ToUpper/Trim, "
        + "IsOneOf(...)/In(...), collection Contains(...), IsEmpty(), Equals(...), and the full text "
        + "search operators (PlainTextSearch, PhraseSearch, PrefixSearch, WebStyleSearch)";

    public static IMethodCallParser? FindParser(MethodCallExpression expression)
    {
        foreach (var parser in Parsers)
        {
            if (parser.Matches(expression))
                return parser;
        }

        return null;
    }
}
