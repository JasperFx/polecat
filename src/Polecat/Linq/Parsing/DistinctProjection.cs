using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.Parsing;

/// <summary>
///     What a <c>Distinct()</c> does to a statement, which depends on what the query is reading.
/// </summary>
/// <remarks>
///     <para>
///         <b>Documents</b> are distinct already: each row is its own id. The <c>DISTINCT</c> is left out,
///         because all it could do is compare JSON, which a <c>json</c> column refuses and an
///         <c>nvarchar(max)</c> one sorts for nothing.
///     </para>
///     <para>
///         <b>A scalar</b> is <c>SELECT DISTINCT</c> over its own column, as it always was.
///     </para>
///     <para>
///         <b>A <c>Select</c> of several members</b> cannot be that, and this is what #916 is about. It
///         reads the document and projects it in memory, so <c>DISTINCT</c> compared whole documents and
///         took out nothing. Its members become the columns of a derived table instead, and its rows are
///         read from there as a JSON object, like the rows of a <c>GroupBy</c>. The derived table is also
///         what an <c>ORDER BY</c> needs. An ordering over a member of the projection is just a column of
///         it, where on one <c>SELECT DISTINCT</c> list a JSON object is all there is to order by (Msg 145).
///     </para>
///     <para>
///         Anything else is refused rather than left to run. A projection computed in memory has no
///         column for SQL to compare, and comparing the documents it came from returns duplicates.
///     </para>
///     <para>
///         ⚠️ <b>Comparing is SQL's business, not .NET's.</b> Two values are the same here if the
///         collation says so, so <c>"abc"</c>, <c>"ABC"</c> and <c>"abc  "</c> are one row on a
///         case-insensitive database. That is what <c>Distinct()</c> over a scalar has always meant. A
///         <c>GROUP BY</c> can tell the last two apart for free, by adding a column that goes unread. Here
///         every column is part of the row, so the same answer needs another derived table, and the
///         docs say what it means instead.
///     </para>
/// </remarks>
internal static class DistinctProjection
{
    /// <summary>The name of a column of the derived table, before its position.</summary>
    private const string Column = "__pc_d";

    /// <summary>
    ///     Applies the query's <c>Distinct()</c> to its statement. Comes before
    ///     <c>ApplySingleValueMode</c>, which would replace the select list a count has to read from.
    /// </summary>
    public static void ApplyTo(LinqQueryParser parser, MemberFactory members)
    {
        if (!parser.IsDistinct) return;

        // Removing duplicates cannot change whether there is a row.
        if (parser.SelectExpression == null || parser.ValueMode == SingleValueMode.Any) return;

        if (parser.ValueMode is SingleValueMode.Sum or SingleValueMode.Min or SingleValueMode.Max
            or SingleValueMode.Average)
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate {parser.ValueMode}() after Distinct(), which would aggregate the "
                + "rows of the table rather than the distinct ones. Materialize the query with ToListAsync() "
                + "and aggregate in memory, or use a GroupBy.");
        }

        var statement = parser.Statement;

        if (statement.DistinctByLocator != null)
        {
            throw new BadLinqExpressionException(
                "Polecat cannot translate Distinct() together with DistinctBy(). Use one of them.");
        }

        var counts = parser.ValueMode is SingleValueMode.Count or SingleValueMode.LongCount;

        // A derived table may only carry an ORDER BY that decides which rows it holds.
        if (counts && !statement.Limit.HasValue && !statement.Offset.HasValue) statement.OrderBys.Clear();

        if (parser.IsScalarSelect)
        {
            ApplyToScalar(statement, parser.ScalarSelectMember!, counts);
        }
        else
        {
            var columns = SelectProjectionAnalyzer.TryAnalyze(parser.SelectExpression, members);
            if (columns == null || columns.Count == 0) throw Refuse(parser.SelectExpression);

            statement.SelectFragment = ReadColumns(statement, columns);
        }

        // SELECT DISTINCT COUNT(*) is the count of the table, with one row to be distinct from. The count
        // has to go outside, over the rows the statement leaves.
        if (counts)
        {
            statement.OuterAggregate =
                parser.ValueMode == SingleValueMode.Count ? "COUNT(*)" : "CAST(COUNT(*) AS bigint)";
        }
    }

    /// <summary>
    ///     Makes the statement read <paramref name="columns" /> from a derived table holding the distinct
    ///     combinations of them, answering what to select from it.
    /// </summary>
    public static ISqlFragment ReadColumns(Statement statement, IReadOnlyList<SimpleProjectionColumn> columns)
    {
        for (var i = 0; i < statement.OrderBys.Count; i++)
        {
            var ordering = statement.OrderBys[i];
            var position = columns.ToList().FindIndex(x => x.OrderLocator == ordering.Literal);
            if (position < 0) throw Refuse(ordering);

            statement.OrderBys[i] = ($"{Column}{position}", ordering.Descending);
        }

        statement.DistinctColumns = string.Join(", ", columns.Select((x, i) => $"{x.Locator} AS {Column}{i}"));

        var members = columns.Select((x, i) =>
            new JsonObjectMember(x.JsonKey, new LiteralSqlFragment($"{Column}{i}"), x.MemberType, x.SqlType));

        return new CompositeSqlFragment(new JsonObjectFragment(members), " AS data");
    }

    private static void ApplyToScalar(Statement statement, IQueryableMember member, bool counts)
    {
        for (var i = 0; i < statement.OrderBys.Count; i++)
        {
            var ordering = statement.OrderBys[i];
            if (ordering.Literal != statement.SelectColumns && ordering.Literal != member.TypedLocator)
            {
                throw Refuse(ordering);
            }

            // The same member, but SELECT DISTINCT only orders by what it selects, in those words.
            statement.OrderBys[i] = (statement.SelectColumns, ordering.Descending);
        }

        statement.IsDistinct = true;

        // A column of a derived table has to have a name.
        if (counts) statement.SelectColumns += $" AS {Column}0";
    }

    private static BadLinqExpressionException Refuse(OrderByClause ordering)
        => new($"Polecat cannot order by '{ordering.Literal ?? "a parameterised key"}' over a Distinct(): the "
               + "rows are what the Select projected, and it did not project that. Order by what it projected, "
               + "or materialize the query and order it in memory.");

    private static BadLinqExpressionException Refuse(LambdaExpression select)
        => new($"Polecat cannot translate Distinct() over '{select.Body}': SQL has to compare the rows, and it "
               + "can only compare columns. Project scalar members of the document, as in "
               + "Select(x => new { x.Name, x.Address.City }), or materialize the query with ToListAsync() and "
               + "use Distinct() in memory.");
}
