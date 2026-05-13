using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Polecat.Linq.Joins;

/// <summary>
///     Rewrites the SelectMany result selector from anonymous-type-based references
///     into a direct Func&lt;TOuter, TInner, TResult&gt; by resolving member accesses
///     through the GroupJoin result selector's mapping.
/// </summary>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: compiles a rewritten result selector via Expression.Lambda — runtime code generation. TOuter/TInner/TResult flow in from the LINQ Join registration on the caller side and are preserved per the AOT publishing guide.")]
internal static class JoinResultSelectorRewriter
{
    /// <summary>
    ///     Rewrites the SelectMany result selector to a Func&lt;TOuter, TInner?, TResult&gt;.
    /// </summary>
    /// <param name="groupJoinResultSelector">
    ///     The GroupJoin result selector: (c, orders) => new { c, orders }
    /// </param>
    /// <param name="selectManyResultSelector">
    ///     The SelectMany result selector: (temp, o) => new { temp.c.Name, o.Amount }
    /// </param>
    /// <param name="outerType">The outer element type (e.g., Customer)</param>
    /// <param name="innerType">The inner element type (e.g., Order)</param>
    /// <returns>A LambdaExpression of type Func&lt;TOuter, TInner, TResult&gt;</returns>
    public static LambdaExpression Rewrite(
        LambdaExpression groupJoinResultSelector,
        LambdaExpression selectManyResultSelector,
        Type outerType,
        Type innerType)
    {
        // GroupJoin result selector: (c, orders) => new { c, orders }
        // Parameters: c = outer element, orders = IEnumerable<inner>
        var outerParam = groupJoinResultSelector.Parameters[0]; // c
        var innerCollectionParam = groupJoinResultSelector.Parameters[1]; // orders

        // Parse the GroupJoin result selector body to map anonymous type members
        // to either the outer param or the inner collection param.
        // e.g., new { c, orders } → "c" maps to outer, "orders" maps to inner collection
        var memberMap = BuildMemberMap(groupJoinResultSelector.Body, outerParam, innerCollectionParam);

        // SelectMany result selector: (temp, o) => new { temp.c.Name, o.Amount }
        // Parameters: temp = the anonymous type from GroupJoin, o = single inner element
        var selectManyTempParam = selectManyResultSelector.Parameters[0]; // temp
        var selectManyInnerParam = selectManyResultSelector.Parameters[1]; // o

        // Create new parameters for the rewritten lambda
        var newOuterParam = Expression.Parameter(outerType, "outer");
        var newInnerParam = Expression.Parameter(innerType, "inner");

        // Rewrite the body
        var rewriter = new BodyRewriter(
            selectManyTempParam, selectManyInnerParam,
            newOuterParam, newInnerParam,
            memberMap);

        var rewrittenBody = rewriter.Visit(selectManyResultSelector.Body);

        return Expression.Lambda(rewrittenBody, newOuterParam, newInnerParam);
    }

    /// <summary>
    ///     Builds a map from anonymous type member names to either "outer" or "inner".
    /// </summary>
    private static Dictionary<string, string> BuildMemberMap(
        Expression body, ParameterExpression outerParam, ParameterExpression innerCollectionParam)
    {
        var map = new Dictionary<string, string>();

        if (body is NewExpression newExpr && newExpr.Members != null)
        {
            for (var i = 0; i < newExpr.Members.Count; i++)
            {
                var memberName = newExpr.Members[i].Name;
                var arg = newExpr.Arguments[i];

                if (arg == outerParam)
                {
                    map[memberName] = "outer";
                }
                else if (arg == innerCollectionParam)
                {
                    map[memberName] = "inner";
                }
            }
        }
        else if (body is MemberInitExpression memberInit)
        {
            foreach (var binding in memberInit.Bindings.OfType<MemberAssignment>())
            {
                var memberName = binding.Member.Name;
                if (binding.Expression == outerParam)
                {
                    map[memberName] = "outer";
                }
                else if (binding.Expression == innerCollectionParam)
                {
                    map[memberName] = "inner";
                }
            }
        }

        return map;
    }

    private class BodyRewriter : ExpressionVisitor
    {
        private readonly ParameterExpression _selectManyTempParam;
        private readonly ParameterExpression _selectManyInnerParam;
        private readonly ParameterExpression _newOuterParam;
        private readonly ParameterExpression _newInnerParam;
        private readonly Dictionary<string, string> _memberMap;

        public BodyRewriter(
            ParameterExpression selectManyTempParam,
            ParameterExpression selectManyInnerParam,
            ParameterExpression newOuterParam,
            ParameterExpression newInnerParam,
            Dictionary<string, string> memberMap)
        {
            _selectManyTempParam = selectManyTempParam;
            _selectManyInnerParam = selectManyInnerParam;
            _newOuterParam = newOuterParam;
            _newInnerParam = newInnerParam;
            _memberMap = memberMap;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            // Direct reference to the inner element parameter: o → inner
            if (node == _selectManyInnerParam)
            {
                return _newInnerParam;
            }

            return base.VisitParameter(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // Handle temp.c.Name → outer.Name
            // or temp.c → outer
            if (TryResolveFromTemp(node, out var resolved))
            {
                return resolved!;
            }

            return base.VisitMember(node);
        }

        private bool TryResolveFromTemp(MemberExpression node, out Expression? resolved)
        {
            resolved = null;

            // Walk back the member chain to find if it starts with the temp param
            var chain = new List<MemberExpression>();
            Expression? current = node;
            while (current is MemberExpression member)
            {
                chain.Insert(0, member);
                current = member.Expression;
            }

            // Must start with the temp parameter (selectManyTempParam)
            if (current != _selectManyTempParam || chain.Count == 0)
                return false;

            // First member in chain accesses the anonymous type: temp.c or temp.orders
            var firstMember = chain[0];
            var memberName = firstMember.Member.Name;

            if (!_memberMap.TryGetValue(memberName, out var mapping))
                return false;

            if (mapping == "outer")
            {
                // temp.c → outer, temp.c.Name → outer.Name
                Expression result = _newOuterParam;
                for (var i = 1; i < chain.Count; i++)
                {
                    result = Expression.MakeMemberAccess(result, chain[i].Member);
                }
                resolved = result;
                return true;
            }

            // "inner" mapping shouldn't be accessed directly as a member chain
            // (it's the collection, accessed via the selectManyInnerParam)
            return false;
        }
    }
}
