using System.Linq.Expressions;
using Polecat.Internal;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses string.ToLower()/ToLowerInvariant() calls within comparisons.
///     Transforms x.Name.ToLower() == "abc" → LOWER(JSON_VALUE(data, '$.name')) = @p0
/// </summary>
internal class StringToLower : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(string)
            && (expression.Method.Name == "ToLower" || expression.Method.Name == "ToLowerInvariant")
            && expression.Arguments.Count == 0;
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        // This is called when ToLower() is the object of a comparison.
        // The comparison will be handled by the parent expression visitor.
        // We return a marker that applies LOWER() to the locator.
        var member = memberFactory.ResolveMember((MemberExpression)expression.Object!);
        return new SqlFunctionLocator("LOWER", member.RawLocator);
    }
}

/// <summary>
///     Parses string.ToUpper()/ToUpperInvariant() calls within comparisons.
/// </summary>
internal class StringToUpper : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(string)
            && (expression.Method.Name == "ToUpper" || expression.Method.Name == "ToUpperInvariant")
            && expression.Arguments.Count == 0;
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        var member = memberFactory.ResolveMember((MemberExpression)expression.Object!);
        return new SqlFunctionLocator("UPPER", member.RawLocator);
    }
}

/// <summary>
///     Parses string.Trim() calls within comparisons.
/// </summary>
internal class StringTrim : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.DeclaringType == typeof(string)
            && expression.Method.Name is "Trim" or "TrimStart" or "TrimEnd"
            && expression.Arguments.Count == 0;
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        var member = memberFactory.ResolveMember((MemberExpression)expression.Object!);
        var binary = SqlText.BinaryCollated(member.RawLocator);

        // #839: the argument-less LTRIM and RTRIM strip U+0020 and nothing else, where .NET strips every
        // whitespace character. A binary collation because TRIM matches through the collation, and the
        // collation goes back afterwards because this result is what the query compares with, and a
        // Trim() should not turn a case-insensitive database's comparison case-sensitive.
        return expression.Method.Name switch
        {
            "TrimStart" => new SqlFunctionLocator("LTRIM", $"{binary}, {SqlText.WhitespaceExpression}", DatabaseDefault),
            "TrimEnd" => new SqlFunctionLocator("RTRIM", $"{binary}, {SqlText.WhitespaceExpression}", DatabaseDefault),
            _ => new SqlFunctionLocator("TRIM", $"{SqlText.WhitespaceExpression} FROM {binary}", DatabaseDefault)
        };
    }

    private const string DatabaseDefault = " COLLATE DATABASE_DEFAULT";
}
