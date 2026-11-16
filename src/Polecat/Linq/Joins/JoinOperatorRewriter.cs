using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using JasperFx;

namespace Polecat.Linq.Joins;

/// <summary>
///     gh-1015 (marten#5910): rewrites <c>Queryable.Join</c>, and the <c>LeftJoin</c> / <c>RightJoin</c> pair .NET 10
///     added, into the <c>GroupJoin()</c> + <c>SelectMany()</c> (+ <c>DefaultIfEmpty()</c>) form Polecat already
///     translates, before the query is parsed.
/// </summary>
/// <remarks>
///     <para>
///         The rewrite produces the tree a user would write by hand:
///         <c>outer.Join(inner, ok, ik, (o, i) =&gt; r)</c> becomes
///         <c>outer.GroupJoin(inner, ok, ik, (o, g) =&gt; new JoinGroup(o, g, "Join()")).SelectMany(x =&gt; x.Matches, (x, i) =&gt; r[o := x.Outer])</c>,
///         and <c>LeftJoin</c> the same over <c>x.Matches.DefaultIfEmpty()</c>. <c>RightJoin</c> is the left join
///         with the two sources (and key selectors) swapped: the side whose rows must all be kept becomes the
///         GroupJoin's outer source, and the result selector is re-bound so that its outer parameter is the
///         <c>SelectMany</c> element (null when it has no match) and its inner one <c>x.Outer</c>. So every
///         operator after the join, and the SQL, is whatever the hand-written form gets.
///     </para>
///     <para>
///         The .NET 11 overloads without a result selector, which return <c>(TOuter, TInner)</c> tuples, get
///         <c>(o, i) =&gt; new ValueTuple&lt;TOuter, TInner&gt;(o, i)</c> as theirs.
///     </para>
///     <para>
///         Only the shapes the GroupJoin path answers correctly are rewritten, and everything else is refused
///         here, naming the operator the caller wrote: an operator other than <c>Where()</c> (or one of Polecat's
///         own row filters) before the join on either source, an inner source that is not a Polecat
///         <c>Query&lt;T&gt;()</c>, a composite or computed key, an <c>IEqualityComparer</c>, and a join of a join.
///         <c>FullJoin</c> (.NET 11) is not touched, so the operator guard still refuses it.
///     </para>
///     <para>
///         Only the operator chain itself (each call's source argument) is walked, so a query without a join
///         costs a handful of type checks and comes back as the same instance.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "MakeGenericMethod / MakeGenericType / Expression.Lambda close Queryable.GroupJoin, Queryable.SelectMany, Enumerable.DefaultIfEmpty and JoinGroup<,> over the join's own document types. They are only named in an expression tree that Polecat translates to SQL; nothing here is ever invoked.")]
[UnconditionalSuppressMessage("Trimming", "IL2060",
    Justification = "MakeGenericMethod over Queryable/Enumerable operators that the expression tree only names.")]
[UnconditionalSuppressMessage("Trimming", "IL2070",
    Justification = "The constructor of the ValueTuple<,> that the query's own element type already is.")]
[UnconditionalSuppressMessage("Trimming", "IL2075",
    Justification = "JoinGroup<,> is Polecat's own record; its constructor and properties are referenced statically by nameof() and by the record itself.")]
internal static class JoinOperatorRewriter
{
    private static readonly MethodInfo GroupJoinDefinition =
        new Func<IQueryable<object>, IEnumerable<object>, Expression<Func<object, object>>,
                Expression<Func<object, object>>, Expression<Func<object, IEnumerable<object>, object>>,
                IQueryable<object>>(Queryable.GroupJoin)
            .Method.GetGenericMethodDefinition();

    private static readonly MethodInfo SelectManyDefinition =
        new Func<IQueryable<object>, Expression<Func<object, IEnumerable<object>>>,
                Expression<Func<object, object, object>>, IQueryable<object>>(Queryable.SelectMany)
            .Method.GetGenericMethodDefinition();

    private static readonly MethodInfo DefaultIfEmptyDefinition =
        new Func<IEnumerable<object>, IEnumerable<object?>>(Enumerable.DefaultIfEmpty)
            .Method.GetGenericMethodDefinition();

    private enum JoinKind
    {
        Inner,
        Left,
        Right
    }

    /// <summary>
    ///     <paramref name="expression" /> with every <c>Join</c>, <c>LeftJoin</c> and <c>RightJoin</c> on its
    ///     operator chain rewritten, or the same instance when there is none.
    /// </summary>
    public static Expression Rewrite(Expression expression)
    {
        if (expression is not MethodCallExpression call || call.Arguments.Count == 0) return expression;

        var source = call.Arguments[0];
        var rewrittenSource = Rewrite(source);

        if (call.Method.DeclaringType == typeof(Queryable))
        {
            switch (call.Method.Name)
            {
                case nameof(Queryable.Join):
                    return RewriteJoin(call, rewrittenSource, JoinKind.Inner);
                case "LeftJoin":
                    return RewriteJoin(call, rewrittenSource, JoinKind.Left);
                case "RightJoin":
                    return RewriteJoin(call, rewrittenSource, JoinKind.Right);
            }
        }

        if (ReferenceEquals(rewrittenSource, source)) return call;

        var arguments = call.Arguments.ToArray();
        arguments[0] = rewrittenSource;
        return call.Update(call.Object, arguments);
    }

    /// <summary>
    ///     The operator a GroupJoin result selector this rewriter built names, or null for a hand-written one.
    /// </summary>
    public static string? OperatorOf(LambdaExpression groupJoinResultSelector)
    {
        return groupJoinResultSelector.Body is NewExpression { Type.IsGenericType: true } created
               && created.Type.GetGenericTypeDefinition() == typeof(JoinGroup<,>)
               && created.Arguments[2] is ConstantExpression { Value: string name }
            ? name
            : null;
    }

    private static Expression RewriteJoin(MethodCallExpression call, Expression outerSource, JoinKind kind)
    {
        var name = $"{call.Method.Name}()";

        // (outer, inner, outerKey, innerKey[, resultSelector][, comparer]). Without a result selector this is one
        // of the .NET 11 overloads that return (outer, inner) tuples, which take the comparer as an optional
        // parameter -- so a call without one still carries the argument, as a null constant.
        var hasResultSelector = call.Arguments.Count >= 5
                                && GroupJoinSources.QuotedLambda(call.Arguments[4]) != null;
        var comparerIndex = hasResultSelector ? 5 : 4;
        GroupJoinSources.AssertNoComparer(
            call.Arguments.Count > comparerIndex ? call.Arguments[comparerIndex] : null, name);

        var genericArguments = call.Method.GetGenericArguments();
        var outerType = genericArguments[0];
        var innerType = genericArguments[1];
        var keyType = genericArguments[2];

        // IQueryable<TResult>, or IQueryable<(TOuter Outer, TInner Inner)> for a tuple overload
        var resultType = call.Type.GetGenericArguments()[0];

        var resultSelector = hasResultSelector
            ? GroupJoinSources.QuotedLambda(call.Arguments[4])!
            : TupleSelector(outerType, innerType, resultType);

        var innerSource = call.Arguments[1];
        var outerKey = GroupJoinSources.QuotedLambda(call.Arguments[2])!;
        var innerKey = GroupJoinSources.QuotedLambda(call.Arguments[3])!;

        AssertKeyIsMember(name, outerKey, "outer");
        AssertKeyIsMember(name, innerKey, "inner");

        if (kind == JoinKind.Right)
        {
            // The side whose rows must all be kept becomes the GroupJoin's outer source, so the old outer is now
            // the inner one, and both have to be plain Query<T>() chains.
            GroupJoinSources.AssertIsDocumentQuery(outerSource, "outer", name);
            GroupJoinSources.AssertIsDocumentQuery(innerSource, "inner", name);
            AssertTenancyIsOnTheKeptSide(name, outerSource);

            return BuildGroupJoin(name,
                groupOuter: innerSource, groupInner: outerSource,
                groupOuterType: innerType, groupInnerType: outerType, keyType: keyType, resultType: resultType,
                groupOuterKey: innerKey, groupInnerKey: outerKey,
                isLeftJoin: true,
                // (o, i) => r: o is the element the SelectMany flattens, i is x.Outer
                elementParameter: resultSelector.Parameters[0],
                outerParameter: resultSelector.Parameters[1],
                resultBody: resultSelector.Body);
        }

        GroupJoinSources.AssertOnlyFiltered(outerSource, "outer", name, isDocumentQuery: false);
        GroupJoinSources.AssertIsDocumentQuery(innerSource, "inner", name);

        return BuildGroupJoin(name,
            groupOuter: outerSource, groupInner: innerSource,
            groupOuterType: outerType, groupInnerType: innerType, keyType: keyType, resultType: resultType,
            groupOuterKey: outerKey, groupInnerKey: innerKey,
            isLeftJoin: kind == JoinKind.Left,
            elementParameter: resultSelector.Parameters[1],
            outerParameter: resultSelector.Parameters[0],
            resultBody: resultSelector.Body);
    }

    // (o, i) => new ValueTuple<TOuter, TInner>(o, i): what the .NET 11 tuple overloads return. The element names
    // (Outer, Inner) are compile-time only, so the tuple is the plain ValueTuple<TOuter, TInner>, and a later
    // operator reads its Item1 / Item2 (see JoinedRows.Inline).
    private static LambdaExpression TupleSelector(Type outerType, Type innerType, Type tupleType)
    {
        var outer = Expression.Parameter(outerType, "outer");
        var inner = Expression.Parameter(innerType, "inner");
        var constructor = tupleType.GetConstructor([outerType, innerType])!;

        return Expression.Lambda(Expression.New(constructor, outer, inner), outer, inner);
    }

    private static Expression BuildGroupJoin(string name,
        Expression groupOuter, Expression groupInner,
        Type groupOuterType, Type groupInnerType, Type keyType, Type resultType,
        LambdaExpression groupOuterKey, LambdaExpression groupInnerKey,
        bool isLeftJoin,
        ParameterExpression elementParameter, ParameterExpression outerParameter, Expression resultBody)
    {
        var rowType = typeof(JoinGroup<,>).MakeGenericType(groupOuterType, groupInnerType);
        var groupType = typeof(IEnumerable<>).MakeGenericType(groupInnerType);
        var rowConstructor = rowType.GetConstructor([groupOuterType, groupType, typeof(string)])!;
        var outerProperty = rowType.GetProperty(nameof(JoinGroup<object, object>.Outer))!;
        var matchesProperty = rowType.GetProperty(nameof(JoinGroup<object, object>.Matches))!;
        var operatorProperty = rowType.GetProperty(nameof(JoinGroup<object, object>.Operator))!;

        // (o, g) => new JoinGroup<TOuter, TInner>(o, g, "Join()") -- the hand-written (o, g) => new { o, g }. The
        // members are given, as an anonymous type's are, because that is how JoinResultSelectorRewriter and
        // JoinedRows tell the two sides apart. The operator's name rides along for the refusals.
        var o = Expression.Parameter(groupOuterType, "outer");
        var g = Expression.Parameter(groupType, "matches");
        var groupResultSelector = Expression.Lambda(
            Expression.New(rowConstructor, [o, g, Expression.Constant(name)],
                outerProperty, matchesProperty, operatorProperty),
            o, g);

        var groupJoin = Expression.Call(
            GroupJoinDefinition.MakeGenericMethod(groupOuterType, groupInnerType, keyType, rowType),
            groupOuter,
            groupInner,
            Expression.Quote(groupOuterKey),
            Expression.Quote(groupInnerKey),
            Expression.Quote(groupResultSelector));

        // x => x.Matches, or x => x.Matches.DefaultIfEmpty() for a left join
        var x = Expression.Parameter(rowType, "x");
        Expression matches = Expression.Property(x, matchesProperty);
        if (isLeftJoin)
        {
            matches = Expression.Call(DefaultIfEmptyDefinition.MakeGenericMethod(groupInnerType), matches);
        }

        var collectionSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(rowType, groupType), matches, x);

        // (x, i) => r with the result selector's outer parameter re-bound to x.Outer. The inner one is reused as
        // the SelectMany element parameter, so the body is otherwise the caller's own.
        var body = new ParameterToExpression(outerParameter, Expression.Property(x, outerProperty)).Visit(resultBody);
        var flattenedSelector = Expression.Lambda(body, x, elementParameter);

        return Expression.Call(
            SelectManyDefinition.MakeGenericMethod(rowType, groupInnerType, resultType),
            groupJoin,
            Expression.Quote(collectionSelector),
            Expression.Quote(flattenedSelector));
    }

    /// <summary>
    ///     A join key is one member of the document. A composite key (<c>new { x.A, x.B }</c>) has no single column
    ///     to compare, and a computed one is not what the ON clause can read.
    /// </summary>
    private static void AssertKeyIsMember(string name, LambdaExpression keySelector, string side)
    {
        var body = keySelector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            body = unary.Operand;
        }

        if (body is NewExpression or MemberInitExpression)
        {
            throw GroupJoinSources.Refusal(name,
                $"the {side} key selector builds a composite key, and only a single member is translated. Join on "
                + "one member and filter the rest with Where() after the join");
        }

        var current = body;
        while (current is MemberExpression member) current = member.Expression;

        if (body is not MemberExpression || current != keySelector.Parameters[0])
        {
            throw GroupJoinSources.Refusal(name,
                $"the {side} key selector must be a member of the document, such as x => x.CustomerId, and it is "
                + $"{keySelector.Body}");
        }
    }

    /// <summary>
    ///     <c>AnyTenant()</c> / <c>TenantIsOneOf()</c> on the outer source of a join covers both sides. A
    ///     <c>RightJoin()</c>'s outer source becomes the GroupJoin's inner one, where it would cover only that
    ///     side, so the same query would read another tenant's rows as a <c>Join()</c> and not as a
    ///     <c>RightJoin()</c>.
    /// </summary>
    private static void AssertTenancyIsOnTheKeptSide(string name, Expression outerSource)
    {
        var current = outerSource;
        while (current is MethodCallExpression call)
        {
            if (call.Method.DeclaringType == typeof(LinqExtensions)
                && call.Method.Name is nameof(LinqExtensions.AnyTenant) or nameof(LinqExtensions.TenantIsOneOf))
            {
                throw GroupJoinSources.Refusal(name,
                    $"{call.Method.Name}() is applied to its outer source, which a right join reads as the side that "
                    + "may have no match, so it would only widen that side's tenancy. Apply it to the inner source, "
                    + "whose rows a right join keeps, where it covers both sides as it does for Join()");
            }

            current = call.Arguments[0];
        }
    }

    private sealed class ParameterToExpression(ParameterExpression from, Expression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) => node == from ? to : node;
    }
}

/// <summary>
///     gh-1015: the GroupJoin row a rewritten <c>Join</c> / <c>LeftJoin</c> / <c>RightJoin</c> carries between its
///     <c>GroupJoin()</c> and <c>SelectMany()</c>: the hand-written <c>new { o, g }</c>, plus the name of the operator
///     the caller wrote. It is only ever named in an expression tree, never constructed.
/// </summary>
internal sealed record JoinGroup<TOuter, TInner>(TOuter Outer, IEnumerable<TInner> Matches, string Operator);
