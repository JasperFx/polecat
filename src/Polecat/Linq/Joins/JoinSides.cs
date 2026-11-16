using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Joins;

/// <summary>
///     gh-1001: resolves the members of a lambda over the outer and the inner document (see
///     <see cref="JoinedRows.OnTheDocuments" />) against the table each one comes from.
/// </summary>
internal sealed class JoinSides
{
    internal const string OuterAlias = "outer_t";
    internal const string InnerAlias = "inner_t";

    private const string DistinctColumn = "__pc_d";

    private readonly LambdaExpression _rowSelector;
    private readonly MemberFactory _outer;
    private readonly MemberFactory _inner;

    /// <param name="rowSelector">The joined rows' projection, over the outer and the inner document.</param>
    public JoinSides(LambdaExpression rowSelector, MemberFactory outer, MemberFactory inner)
    {
        _rowSelector = rowSelector;
        _outer = outer;
        _inner = inner;
    }

    public enum DistinctShape
    {
        /// <summary>Read as a JSON object, like the rows of a GroupBy.</summary>
        Object,

        /// <summary>Read as the one column.</summary>
        Scalar
    }

    /// <summary>
    ///     Adds a predicate written after the join to the WHERE of the side it reads. A conjunction is split, so
    ///     each side gets its own half.
    /// </summary>
    public void AddPredicate(GroupJoinData join, LambdaExpression rowPredicate, JoinStatement statement)
    {
        var predicate = JoinedRows.OnTheDocuments(join, rowPredicate);
        var outerParameter = predicate.Parameters[0];
        var innerParameter = predicate.Parameters[1];

        foreach (var conjunct in JoinedRows.Conjuncts(predicate.Body))
        {
            var reads = JoinedRows.ParametersReadBy(Expression.Lambda(conjunct, predicate.Parameters));

            if (reads.Except([outerParameter, innerParameter]).Any())
            {
                throw join.Refusal(
                    $"the filter '{conjunct}' reads the group of matches rather than one joined row. Filter the "
                    + "members the SelectMany() result selector projects");
            }

            if (reads.Contains(outerParameter) && reads.Contains(innerParameter))
            {
                throw join.Refusal(
                    $"the filter '{conjunct}' compares the two sides of the join with each other, which has no "
                    + "translation. Filter each side on its own, or finish the comparison in memory");
            }

            if (reads.Contains(innerParameter))
            {
                // Marten refuses the same thing (marten#5910): the inner filter goes in the LEFT JOIN's ON clause,
                // which keeps every outer row without a surviving match -- the rows the filter after the join
                // should have removed. gh-1015: a RightJoin() is a left join with its sides swapped, so there it is
                // the RightJoin's OUTER side that this refuses a filter on.
                if (join.IsLeftJoin)
                {
                    var (optional, kept) = join.Operator == "RightJoin()"
                        ? ("the outer side of a right join (RightJoin())", "inner")
                        : ("the inner side of a left join (LeftJoin(), or SelectMany over DefaultIfEmpty())", "outer");

                    throw join.Refusal(
                        $"a Where() on {optional} applied after the join would be applied to that side's rows before "
                        + $"joining, which keeps the {kept} rows the filter should remove. If that is what you mean, "
                        + "filter that source before the join; for the rows that have a match, use an inner join");
                }

                statement.InnerWheres.Add(new AliasedSqlFragment(new WhereClauseParser(_inner).Parse(conjunct),
                    InnerAlias));
            }
            else
            {
                statement.OuterWheres.Add(new AliasedSqlFragment(new WhereClauseParser(_outer).Parse(conjunct),
                    OuterAlias));
            }
        }
    }

    /// <summary>
    ///     The document member <paramref name="body" /> reads, and the alias of its table, when it is a member
    ///     of one of the two documents of <paramref name="lambda" />.
    /// </summary>
    public bool TryResolve(Expression body, LambdaExpression lambda, out IQueryableMember member, out string alias)
    {
        member = null!;
        alias = null!;

        if (body is not MemberExpression memberExpression) return false;

        var root = RootOf(memberExpression);
        if (root == lambda.Parameters[0])
        {
            (member, alias) = (_outer.ResolveMember(memberExpression), OuterAlias);
            return true;
        }

        if (root == lambda.Parameters[1])
        {
            (member, alias) = (_inner.ResolveMember(memberExpression), InnerAlias);
            return true;
        }

        return false;
    }

    /// <summary>
    ///     Makes <paramref name="statement" /> read the distinct combinations of the projected columns, and its
    ///     orderings name those columns.
    /// </summary>
    public DistinctShape ApplyDistinct(JoinStatement statement)
    {
        var body = SelectProjectionAnalyzer.StripSafeConvert(_rowSelector.Body);

        if (TryResolve(body, _rowSelector, out var scalar, out var scalarAlias)
            && SelectProjectionAnalyzer.IsScalar(scalar.MemberType) && !scalar.MayBeObject)
        {
            var locator = JoinStatement.AliasLocator(scalar.ValueLocator(), scalarAlias);
            var typed = JoinStatement.AliasLocator(scalar.TypedLocator, scalarAlias);
            MapOrderings(statement, ordering => ordering == typed || ordering == locator ? 0 : -1);

            statement.DistinctColumns = $"{locator} AS {DistinctColumn}0";
            statement.DistinctSelect = new LiteralSqlFragment($"{DistinctColumn}0");
            return DistinctShape.Scalar;
        }

        var columns = AnalyzeColumns(body) ?? throw JoinedRows.Refusal(
            $"Distinct() over '{_rowSelector.Body}' has to compare the projected rows, and SQL can only compare "
            + "columns. Project scalar members of the two documents, as in (x, o) => new { x.c.Name, o.Status }, "
            + "or materialize the rows with ToListAsync() and call Distinct() in memory");

        MapOrderings(statement, ordering => columns.FindIndex(x => x.OrderLocator == ordering));

        statement.DistinctColumns =
            string.Join(", ", columns.Select((x, i) => $"{x.Locator} AS {DistinctColumn}{i}"));

        var members = columns.Select((x, i) =>
            new JsonObjectMember(x.JsonKey, new LiteralSqlFragment($"{DistinctColumn}{i}"), x.MemberType, x.SqlType));
        statement.DistinctSelect = new CompositeSqlFragment(new JsonObjectFragment(members), " AS data");

        return DistinctShape.Object;
    }

    private static void MapOrderings(JoinStatement statement, Func<string, int> positionOf)
    {
        for (var i = 0; i < statement.OrderBys.Count; i++)
        {
            var (locator, descending) = statement.OrderBys[i];
            var position = positionOf(locator);
            if (position < 0)
            {
                throw JoinedRows.Refusal(
                    "an ordering after Distinct() has to order by a member the distinct rows project, and this one "
                    + "is not projected. Project it, or order the rows in memory");
            }

            statement.OrderBys[i] = ($"{DistinctColumn}{position}", descending);
        }
    }

    private List<SimpleProjectionColumn>? AnalyzeColumns(Expression body)
    {
        var assignments = body switch
        {
            NewExpression { Members: not null } created when created.Members.Count == created.Arguments.Count
                => created.Members.Zip(created.Arguments).ToList(),
            MemberInitExpression { NewExpression.Arguments.Count: 0 } initialized
                when initialized.Bindings.All(x => x is MemberAssignment)
                => initialized.Bindings.Cast<MemberAssignment>().Select(x => (x.Member, x.Expression)).ToList(),
            _ => null
        };

        if (assignments == null || assignments.Count == 0) return null;

        var columns = new List<SimpleProjectionColumn>();
        foreach (var (projected, value) in assignments)
        {
            var stripped = SelectProjectionAnalyzer.StripSafeConvert(value);
            if (stripped is not MemberExpression memberExpression) return null;

            var root = RootOf(memberExpression);
            var (factory, alias) = root == _rowSelector.Parameters[0] ? (_outer, OuterAlias)
                : root == _rowSelector.Parameters[1] ? (_inner, InnerAlias)
                : (null, null);
            if (factory == null) return null;

            var column = SelectProjectionAnalyzer.TryColumn(projected, value, factory);
            if (column == null) return null;

            columns.Add(column with
            {
                Locator = JoinStatement.AliasLocator(column.Locator, alias!),
                OrderLocator = JoinStatement.AliasLocator(column.OrderLocator, alias!)
            });
        }

        return columns;
    }

    private static Expression? RootOf(MemberExpression expression)
    {
        Expression? current = expression;
        while (current is MemberExpression member) current = member.Expression;
        return current;
    }
}
