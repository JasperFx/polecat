using System.Linq.Expressions;
using JasperFx;
using Polecat.Linq.Metadata;
using Polecat.Linq.SoftDeletes;

namespace Polecat.Linq.Joins;

/// <summary>
///     gh-1000: refuses the <c>GroupJoin(...)</c> calls Polecat's join translation cannot answer, before
///     any of the query is parsed. Mirrors the refusals Marten makes for the same shapes (marten#5910,
///     marten#5731).
/// </summary>
/// <remarks>
///     <para>
///         The join is one SQL statement: <c>outer_t JOIN inner_t ON key = key WHERE ...</c>. Anything in
///         front of the join on either source can only become part of that statement's WHERE, so a
///         filter is the one operator that means the same thing there. Everything else used to be
///         silently misapplied rather than refused:
///     </para>
///     <list type="bullet">
///         <item><c>Take()</c>/<c>Skip()</c> before the join landed on the statement's TOP/OFFSET and
///         paged the <b>joined rows</b> instead of the outer documents;</item>
///         <item><c>OrderBy()</c> before the join was never read at all, and a SQL join does not keep an
///         ordering of its sources anyway;</item>
///         <item>an in-memory inner source was read for its element type only, so the join ran against
///         the <b>database table</b> of that type instead of the collection the caller passed;</item>
///         <item>the <c>IEqualityComparer</c> overload's sixth argument was never read, so the server's
///         own comparison was used instead.</item>
///     </list>
///     <para>
///         Paging the outer documents before a join could be translated later with a derived table; until
///         then a refusal is the honest answer.
///     </para>
/// </remarks>
internal static class GroupJoinSources
{
    /// <summary>How the refusals name a hand-written join.</summary>
    public const string GroupJoinOperator = "GroupJoin(...).SelectMany(...)";

    public static void AssertTranslatable(MethodCallExpression groupJoin)
    {
        // (outer, inner, outerKey, innerKey, resultSelector[, comparer])
        AssertNoComparer(groupJoin.Arguments.Count > 5 ? groupJoin.Arguments[5] : null, GroupJoinOperator);
        AssertOnlyFiltered(groupJoin.Arguments[0], "outer", GroupJoinOperator, isDocumentQuery: false);
        AssertIsDocumentQuery(groupJoin.Arguments[1], "inner", GroupJoinOperator);
    }

    /// <summary>
    ///     gh-1015: <paramref name="comparer" />, the <c>IEqualityComparer</c> argument of a join if it has one, is
    ///     absent or the null a .NET 11 overload's optional parameter carries.
    /// </summary>
    public static void AssertNoComparer(Expression? comparer, string operatorName)
    {
        if (comparer is null or ConstantExpression { Value: null }) return;

        throw Refusal(operatorName,
            "the overload that takes an IEqualityComparer cannot be translated to SQL, which compares the "
            + "stored keys with the column's own collation. Drop the comparer, or materialize both sides and "
            + "join them in memory");
    }

    /// <summary>
    ///     Only filters come before the join on <paramref name="source" />. With
    ///     <paramref name="isDocumentQuery" />, the message says the source has to be a filtered
    ///     <c>Query&lt;T&gt;()</c>, which is what an inner source -- and both sources of a <c>RightJoin()</c> -- is.
    /// </summary>
    public static void AssertOnlyFiltered(Expression source, string side, string operatorName, bool isDocumentQuery)
    {
        var current = source;
        while (current is MethodCallExpression call)
        {
            // gh-1015: a Join()/LeftJoin()/RightJoin() earlier on the outer chain arrives here already rewritten
            // to its SelectMany(); one on an inner source is never rewritten, so it arrives under its own name.
            if (call.Method.Name is nameof(Queryable.SelectMany) or nameof(Queryable.GroupJoin)
                or nameof(Queryable.Join) or "LeftJoin" or "RightJoin" or "FullJoin")
            {
                throw Refusal(operatorName,
                    $"its {side} source is itself a join, and Polecat translates one join per query. Query the "
                    + "first join and combine it with the third source in memory, or write the SQL yourself with "
                    + "session.AdvancedSql.QueryAsync<T>(...)");
            }

            if (!IsFilter(call))
            {
                throw Refusal(operatorName, isDocumentQuery
                    ? $"the {side} source may only be session.Query<T>() filtered by Where(), and it applies "
                      + $"{call.Method.Name}(). Move that operator after the join"
                    : $"only Where() may come before it on the {side} source, and this query applies "
                      + $"{call.Method.Name}() first. Apply OrderBy(), Take(), Skip(), Select(), Distinct() and the "
                      + "like after the join instead, where they apply to the joined rows");
            }

            current = call.Arguments[0];
        }
    }

    public static void AssertIsDocumentQuery(Expression source, string side, string operatorName)
    {
        AssertOnlyFiltered(source, side, operatorName, isDocumentQuery: true);

        var root = source;
        while (root is MethodCallExpression call) root = call.Arguments[0];

        // The root has to be the Query<T>() itself. An in-memory collection would be read as the whole
        // document table of its element type, and a Polecat queryable passed through AsEnumerable() is a
        // constant whose own operators are invisible from here.
        if (root is ConstantExpression { Value: IQueryable { Provider: IPolecatAsyncQueryProvider } queryable } constant
            && queryable.Expression is ConstantExpression own && ReferenceEquals(own.Value, constant.Value))
        {
            return;
        }

        throw Refusal(operatorName,
            $"the {side} source must be a Polecat session.Query<T>(), optionally filtered by Where(). Joining to an "
            + "in-memory collection, or to a queryable passed through AsEnumerable(), is not translated: Polecat "
            + "would join the database table of its element type instead");
    }

    /// <summary>
    ///     A plain <c>Where(x =&gt; ...)</c>, or one of Polecat's own operators that only decide which rows
    ///     of the source are visible (tenancy, soft deletes, metadata timestamps, non-stale data).
    /// </summary>
    public static bool IsFilter(MethodCallExpression call)
    {
        var declaring = call.Method.DeclaringType;

        if (declaring == typeof(Queryable))
        {
            return call.Method.Name switch
            {
                nameof(Queryable.Where) => call.Arguments.Count == 2
                                           && QuotedLambda(call.Arguments[1]) is { Parameters.Count: 1 },
                nameof(Queryable.AsQueryable) => true,
                _ => false
            };
        }

        if (declaring == typeof(LinqExtensions))
        {
            return call.Method.Name is nameof(LinqExtensions.AnyTenant) or nameof(LinqExtensions.TenantIsOneOf);
        }

        return declaring == typeof(SoftDeletedExtensions)
               || declaring == typeof(MetadataExtensions)
               || declaring == typeof(CreatedAtExtensions)
               || declaring == typeof(NonStaleDataExtensions);
    }

    public static LambdaExpression? QuotedLambda(Expression expression)
        => (expression is UnaryExpression { NodeType: ExpressionType.Quote } quote ? quote.Operand : expression)
            as LambdaExpression;

    public static BadLinqExpressionException Refusal(string operatorName, string reason)
        => new($"Polecat cannot translate this {operatorName}: {reason}.");
}
