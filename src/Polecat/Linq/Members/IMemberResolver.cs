using System.Linq.Expressions;

namespace Polecat.Linq.Members;

/// <summary>
///     Resolves a member expression to an IQueryableMember for SQL generation.
/// </summary>
internal interface IMemberResolver
{
    IQueryableMember ResolveMember(MemberExpression expression);

    /// <summary>
    ///     <see cref="ResolveMember" /> for a member SQL has to sort or group by, which refuses one known
    ///     to have no value for it to do that with. See <see cref="StoredShape" />.
    /// </summary>
    /// <param name="expression">The member.</param>
    /// <param name="position">Where the query uses it, for the message.</param>
    IQueryableMember ResolveKey(MemberExpression expression, string position) => ResolveMember(expression);
}
