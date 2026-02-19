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
    ISqlFragment Parse(MemberFactory memberFactory, MethodCallExpression expression);
}
