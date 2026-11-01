using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing.Methods;

/// <summary>
///     Parses a specific method call expression into an ISqlFragment for WHERE clauses.
/// </summary>
internal interface IMethodCallParser
{
    bool Matches(MethodCallExpression expression);
    ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression);

    /// <summary>
    ///     <see cref="Parse(IMemberResolver, MethodCallExpression)" /> for a parser that needs to know
    ///     whether a <c>NOT</c> encloses the call. See <c>QueryableMemberExtensions.ComparisonLocator</c>.
    /// </summary>
    /// <param name="memberFactory">Resolves the document members the call refers to.</param>
    /// <param name="expression">The method call.</param>
    /// <param name="positive">False when the call sits under a <c>NOT</c>.</param>
    ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression, bool positive)
        => Parse(memberFactory, expression);
}
