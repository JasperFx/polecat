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
        new IsOneOf(),
        new EnumerableContains(),
        new IsEmpty()
    ];

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
