using System.Linq.Expressions;
using JasperFx;

namespace Polecat.Linq.Joins;

/// <summary>
///     gh-1001: reads an operator written against the rows of a <c>GroupJoin(...).SelectMany(...)</c> through
///     the projection that made them, so its members resolve to the outer or the inner document.
/// </summary>
/// <remarks>
///     <para>
///         A <c>Where(r =&gt; r.Index &gt; 0)</c> after the join is a lambda over the anonymous row type, not
///         over a document. It used to be handed to the outer document's member resolution as if it were
///         one, so <c>r.Index</c> became <c>JSON_VALUE(outer_t.data, '$.index')</c> -- a path the customer
///         does not have -- and the query returned no rows at all.
///     </para>
///     <para>
///         Every lambda after the join is instead inlined into the row selector, which is a lambda over the
///         GroupJoin's row and the inner element, <c>(x, o) =&gt; new { x.c.Number, o.Index }</c>:
///         <c>r.Index</c> becomes <c>o.Index</c> and <c>r.Number</c> becomes <c>x.c.Number</c>.
///         <see cref="JoinResultSelectorRewriter" /> then turns <c>x.c</c> into the outer document. A
///         <c>Select()</c> after the join composes the same way, so whatever follows it reads through both.
///     </para>
/// </remarks>
internal static class JoinedRows
{
    /// <summary>
    ///     <paramref name="rowLambda" />, a lambda over the joined row, as a lambda over the parameters of
    ///     <paramref name="rowSelector" />.
    /// </summary>
    public static LambdaExpression Inline(LambdaExpression rowLambda, LambdaExpression rowSelector)
    {
        var body = new ParameterReplacer(rowLambda.Parameters[0], rowSelector.Body).Visit(rowLambda.Body);
        return Expression.Lambda(new ProjectedMemberSimplifier().Visit(body), rowSelector.Parameters);
    }

    /// <summary>
    ///     A lambda over the GroupJoin row and the inner element, as a lambda over the outer and the inner
    ///     document.
    /// </summary>
    public static LambdaExpression OnTheDocuments(GroupJoinData join, LambdaExpression rowLambda)
        => JoinResultSelectorRewriter.Rewrite(join.GroupJoinResultSelector, rowLambda,
            join.OuterElementType, join.InnerElementType);

    /// <summary>The parameters of <paramref name="lambda" /> its body reads.</summary>
    public static HashSet<ParameterExpression> ParametersReadBy(LambdaExpression lambda)
    {
        var finder = new ParameterFinder();
        finder.Visit(lambda.Body);
        return finder.Found;
    }

    /// <summary>The predicates <paramref name="body" /> ANDs together.</summary>
    public static IEnumerable<Expression> Conjuncts(Expression body)
    {
        if (body is BinaryExpression { NodeType: ExpressionType.AndAlso } and)
        {
            foreach (var left in Conjuncts(and.Left)) yield return left;
            foreach (var right in Conjuncts(and.Right)) yield return right;
        }
        else
        {
            yield return body;
        }
    }

    public static BadLinqExpressionException Refusal(string reason)
        => new($"Polecat cannot translate this GroupJoin(...).SelectMany(...): {reason}.");

    private sealed class ParameterReplacer(ParameterExpression from, Expression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) => node == from ? to : node;
    }

    /// <summary><c>new { A = x.c.Name }.A</c> is <c>x.c.Name</c>.</summary>
    private sealed class ProjectedMemberSimplifier : ExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            var source = node.Expression == null ? null : Visit(node.Expression);

            switch (source)
            {
                case NewExpression { Members: not null } created:
                    for (var i = 0; i < created.Members.Count; i++)
                    {
                        if (created.Members[i].Name == node.Member.Name) return created.Arguments[i];
                    }

                    break;

                case MemberInitExpression initialized:
                    foreach (var binding in initialized.Bindings)
                    {
                        if (binding is MemberAssignment assignment && assignment.Member.Name == node.Member.Name)
                        {
                            return assignment.Expression;
                        }
                    }

                    break;
            }

            return node.Update(source);
        }
    }

    private sealed class ParameterFinder : ExpressionVisitor
    {
        public HashSet<ParameterExpression> Found { get; } = [];

        protected override Expression VisitParameter(ParameterExpression node)
        {
            Found.Add(node);
            return node;
        }

        // A nested lambda's own parameters are not the ones the predicate reads
        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            var finder = new ParameterFinder();
            finder.Visit(node.Body);
            Found.UnionWith(finder.Found.Except(node.Parameters));
            return node;
        }
    }
}
