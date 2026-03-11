using System.Linq.Expressions;

namespace Polecat.Linq.Members;

/// <summary>
///     Resolves a member expression to an IQueryableMember for SQL generation.
/// </summary>
internal interface IMemberResolver
{
    IQueryableMember ResolveMember(MemberExpression expression);
}
