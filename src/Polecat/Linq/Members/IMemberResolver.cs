using JasperFx;
using System.Linq.Expressions;
using Polecat.Linq.Parsing;
using Polecat.Linq.SqlGeneration;

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

    /// <summary>
    ///     #808: <c>x.Payment.Value is Card</c>, a type test over a union-typed member.
    /// </summary>
    /// <param name="unionValue">The union's <c>Value</c>.</param>
    /// <param name="tested">The type tested for.</param>
    /// <param name="expression">The whole test, for a message.</param>
    ISqlFragment ResolveUnionTypeTest(MemberExpression unionValue, Type tested, Expression expression)
        => throw new BadLinqExpressionException(
            $"Polecat cannot translate the union type test '{expression}' here: union-typed members are "
            + "only translated in document queries. Materialize the query and test the case in memory.");

    /// <summary>
    ///     #808: <c>(int)x.Code.Value</c>, a union's primitive case read as itself.
    /// </summary>
    IQueryableMember ResolveUnionCase(MemberExpression unionValue, Type caseType, Expression expression)
        => throw new BadLinqExpressionException(
            $"Polecat cannot translate the union case '{expression}' here: union-typed members are only "
            + "translated in document queries. Materialize the query and read the case in memory.");
}
