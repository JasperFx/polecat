using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Polecat.Linq.Parsing;

/// <summary>
///     Rewrites a lambda over a <c>Select</c>'s output into one over the document that projection was made
///     from, so that an operator written after the <c>Select</c> means what it says.
/// </summary>
/// <remarks>
///     <para>
///         #875: the translator resolves a member by walking back to whatever parameter it starts at, and
///         assumes that parameter is the document. After a <c>Select</c> it is the projection, so
///         <c>Select(x =&gt; x.Address).Where(a =&gt; a.City == "x")</c> read <c>$.city</c> and
///         <c>Select(x =&gt; new { N = x.Name }).OrderBy(y =&gt; y.N)</c> read <c>$.n</c>. Neither key
///         exists, so the filter matched nothing and the ordering did nothing, and neither said so.
///     </para>
///     <para>
///         This substitutes the projection's body for the parameter and folds a member read off a
///         constructed shape down to the expression assigned to it, which is all it takes: what comes out
///         is the lambda the caller could have written against the document, and every rule that applies
///         to one of those applies, its refusals included.
///     </para>
///     <para>
///         A member the tree does not give a value for is refused. <c>new Dto { A = x.A }</c> leaves
///         <c>B</c> at its default, which no row holds. <c>new Dto(x.A)</c> names no member at all, and a
///         constructor's parameter names are not evidence of what it does with them.
///     </para>
/// </remarks>
internal sealed class ProjectedMembers: ExpressionVisitor
{
    private readonly ParameterExpression _parameter;
    private readonly Expression _projection;

    private ProjectedMembers(ParameterExpression parameter, Expression projection)
    {
        _parameter = parameter;
        _projection = projection;
    }

    /// <summary><paramref name="lambda" /> as a lambda over <paramref name="select" />'s parameter.</summary>
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Expression.Lambda over a body whose types came from the caller's own tree. The expression trees originate at the LINQ registration boundary where document types are preserved per the AOT publishing guide.")]
    public static LambdaExpression Rewrite(LambdaExpression select, LambdaExpression lambda)
    {
        var body = new ProjectedMembers(lambda.Parameters[0], select.Body).Visit(lambda.Body);
        return Expression.Lambda(body, select.Parameters);
    }

    protected override Expression VisitParameter(ParameterExpression node)
        => node == _parameter ? _projection : node;

    protected override Expression VisitMember(MemberExpression node)
    {
        var owner = Visit(node.Expression);

        return (owner == null ? null : LinqQueryParser.StripConvert(owner)) switch
        {
            NewExpression created => ReadOff(created, node),
            MemberInitExpression initialized => ReadOff(initialized, node),
            _ => node.Update(owner)
        };
    }

    private static Expression ReadOff(NewExpression created, MemberExpression node)
    {
        var position = created.Members?.ToList().FindIndex(x => x.Name == node.Member.Name) ?? -1;

        return position >= 0 ? created.Arguments[position] : throw Refuse(node);
    }

    private static Expression ReadOff(MemberInitExpression initialized, MemberExpression node)
    {
        var assignment = initialized.Bindings.OfType<MemberAssignment>()
            .FirstOrDefault(x => x.Member.Name == node.Member.Name);

        return assignment?.Expression ?? throw Refuse(node);
    }

    private static BadLinqExpressionException Refuse(MemberExpression node)
        => new($"Polecat cannot translate '{node}' to SQL: the preceding Select(...) does not say what "
               + $"{node.Member.Name} holds. An operator after a Select can read a member it assigned, in "
               + "an anonymous type or an object initializer. Move the operator in front of the Select, or "
               + "materialize the query and finish the work in memory.");
}
