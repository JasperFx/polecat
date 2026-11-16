using System.Linq.Expressions;

namespace Polecat.Linq.Joins;

/// <summary>
///     Data holder parsed from GroupJoin + SelectMany expression arguments.
/// </summary>
internal class GroupJoinData
{
    public Expression InnerSourceExpression { get; set; } = null!;
    public LambdaExpression OuterKeySelector { get; set; } = null!;
    public LambdaExpression InnerKeySelector { get; set; } = null!;
    public LambdaExpression GroupJoinResultSelector { get; set; } = null!;
    public Type OuterElementType { get; set; } = null!;
    public Type InnerElementType { get; set; } = null!;
    public bool IsLeftJoin { get; set; }
    public LambdaExpression? SelectManyResultSelector { get; set; }
    public LambdaExpression? SelectManyCollectionSelector { get; set; }

    /// <summary>
    ///     OrderBy expressions captured after SelectMany, stored as (lambda, descending).
    ///     These reference the projected anonymous type and must be resolved
    ///     by mapping through the result selector.
    /// </summary>
    public List<(LambdaExpression Expression, bool Descending)> OrderByExpressions { get; } = [];

    /// <summary>
    ///     gh-1001: the predicates of a Where() after the join, and the inline predicate of a First(),
    ///     Count(), Any() and the like, each already read through the row selector
    ///     (see <see cref="JoinedRows.Inline" />).
    /// </summary>
    public List<LambdaExpression> PostJoinPredicates { get; } = [];

    /// <summary>
    ///     <paramref name="rowLambda" />, written against the joined rows, as a lambda over the GroupJoin row
    ///     and the inner element, the parameters of <see cref="SelectManyResultSelector" />.
    /// </summary>
    public LambdaExpression ReadThroughRow(LambdaExpression rowLambda)
        => JoinedRows.Inline(rowLambda, SelectManyResultSelector ?? throw MissingResultSelector());

    /// <summary>gh-1001: a Select() after the join projects the joined rows, so it composes with the row selector.</summary>
    public void ComposeSelect(LambdaExpression select) => SelectManyResultSelector = ReadThroughRow(select);

    public static JasperFx.BadLinqExpressionException MissingResultSelector()
        => new("Polecat cannot translate a GroupJoin without a following SelectMany(source, collectionSelector, "
               + "resultSelector). Write GroupJoin(...).SelectMany(x => x.matches, (x, m) => new { ... }), which is "
               + "the shape a JOIN is expressed in, or SelectMany(x => x.matches.DefaultIfEmpty(), ...) for a LEFT JOIN.");
}
