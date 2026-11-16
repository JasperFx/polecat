using System.Linq.Expressions;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Members;

/// <summary>
///     gh-1016: resolves the members of a row a <c>SelectMany()</c> made, which reads both the parent document
///     and one element of its collection. A member chain is resolved by whichever of the two it starts at:
///     <c>(x, c) =&gt; new { x.Id, c.Sku }</c> reads <c>x.Id</c> off the document and <c>c.Sku</c> off the element.
/// </summary>
internal sealed class FlattenedRowMembers : IMemberResolver
{
    private readonly IMemberResolver _parents;
    private readonly IMemberResolver _elements;
    private readonly ParameterExpression _element;

    public FlattenedRowMembers(IMemberResolver parents, IMemberResolver elements, ParameterExpression element)
    {
        _parents = parents;
        _elements = elements;
        _element = element;
    }

    private IMemberResolver For(Expression expression) => RootOf(expression) == _element ? _elements : _parents;

    internal static ParameterExpression? RootOf(Expression? expression)
    {
        while (expression != null)
        {
            switch (expression)
            {
                case ParameterExpression parameter:
                    return parameter;
                case MemberExpression member:
                    expression = member.Expression;
                    continue;
                case UnaryExpression unary:
                    expression = unary.Operand;
                    continue;
            }

            if (DocumentPaths.TryGetIndex(expression, out var collection, out _))
            {
                expression = collection;
                continue;
            }

            return null;
        }

        return null;
    }

    public IQueryableMember ResolveMember(MemberExpression expression) => For(expression).ResolveMember(expression);

    public IQueryableMember ResolveKey(MemberExpression expression, string position)
        => For(expression).ResolveKey(expression, position);

    public IQueryableMember ResolvePath(Expression expression) => For(expression).ResolvePath(expression);

    public ISqlFragment ResolveUnionTypeTest(MemberExpression unionValue, Type tested, Expression expression)
        => For(unionValue).ResolveUnionTypeTest(unionValue, tested, expression);

    public IQueryableMember ResolveUnionCase(MemberExpression unionValue, Type caseType, Expression expression)
        => For(unionValue).ResolveUnionCase(unionValue, caseType, expression);
}
