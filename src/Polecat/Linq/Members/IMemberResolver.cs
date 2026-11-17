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
    ///     #1030: <c>x.Member.ToLower()</c> or <c>x.Member.ToUpper()</c> as a member of its own, read through
    ///     the computed column of an <c>Index(...)</c> with the same <paramref name="casing" />, so a
    ///     comparison over it can seek that index. Null when no such index covers the member, and the
    ///     caller translates the call as it always has.
    /// </summary>
    /// <param name="expression">The member the case is folded on.</param>
    /// <param name="casing"><see cref="Storage.IndexCasing.Lower" /> or <see cref="Storage.IndexCasing.Upper" />.</param>
    IQueryableMember? ResolveCasedMember(MemberExpression expression, Storage.IndexCasing casing) => null;

    /// <summary>
    ///     <see cref="ResolveMember" /> for a member SQL has to sort or group by, which refuses one known
    ///     to have no value for it to do that with. See <see cref="StoredShape" />.
    /// </summary>
    /// <param name="expression">The member.</param>
    /// <param name="position">Where the query uses it, for the message.</param>
    IQueryableMember ResolveKey(MemberExpression expression, string position) => ResolveMember(expression);

    /// <summary>
    ///     gh-1016: the resolver for the elements of a collection a <c>SelectMany()</c> flattens, or null where
    ///     this resolver has no JSON body to flatten, which refuses the <c>SelectMany()</c>.
    /// </summary>
    IMemberResolver? ForElements() => null;

    /// <summary>
    ///     gh-1014: a member, or a chain of members and indexes into the document's collections such as
    ///     <c>x.Grid[1][0]</c> or <c>x.Items[0].Name</c>. A resolver that has no JSON body to index into
    ///     resolves members only.
    /// </summary>
    IQueryableMember ResolvePath(Expression expression)
        => LinqQueryParser.StripConvert(expression) is MemberExpression member
            ? ResolveMember(member)
            : throw new BadLinqExpressionException(
                $"Polecat cannot translate '{expression}' to SQL here: an index into a collection is only "
                + "translated in document queries. Materialize the query and finish the work in memory.");

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
