using System.Text.RegularExpressions;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Linq.Joins;

/// <summary>
///     Generates JOIN SQL for GroupJoin queries.
///     Selects both outer and inner data columns for in-memory projection.
/// </summary>
internal partial class JoinStatement
{
    public string OuterTable { get; set; } = "";
    public string InnerTable { get; set; } = "";
    public string OuterAlias { get; set; } = "outer_t";
    public string InnerAlias { get; set; } = "inner_t";
    public string OuterKeyLocator { get; set; } = "";
    public string InnerKeyLocator { get; set; } = "";
    public bool IsLeftJoin { get; set; }
    public List<ISqlFragment> OuterWheres { get; } = [];
    public List<ISqlFragment> InnerWheres { get; } = [];
    public List<(string Locator, bool Descending)> OrderBys { get; } = [];
    public int? Limit { get; set; }
    public int? Offset { get; set; }

    /// <summary>gh-1001: what the statement answers instead of the joined rows, if anything.</summary>
    public JoinAggregate Aggregate { get; set; }

    /// <summary>
    ///     gh-1001: for a <c>Distinct()</c>, the projected columns as <c>locator AS __pc_dN</c>. The rows are
    ///     read from a derived table of their distinct combinations, and <see cref="OrderBys" /> name those
    ///     columns rather than the documents.
    /// </summary>
    public string? DistinctColumns { get; set; }

    /// <summary>What a <c>Distinct()</c> reads from the derived table <see cref="DistinctColumns" /> makes.</summary>
    public ISqlFragment? DistinctSelect { get; set; }

    private bool IsPaged => Limit.HasValue || Offset.HasValue;

    public void Apply(ICommandBuilder builder)
    {
        switch (Aggregate)
        {
            // gh-1001: counted over the rows the statement would return, so a Take() before a Count() counts
            // the page rather than rendering TOP(n) COUNT(*) -- the one row a count has -- and a Distinct()
            // counts the distinct rows.
            case JoinAggregate.Count:
            case JoinAggregate.LongCount:
                builder.Append(Aggregate == JoinAggregate.Count ? "SELECT COUNT(*) FROM (" : "SELECT CAST(COUNT(*) AS bigint) FROM (");
                WriteRows(builder, "1 AS __pc_one");
                builder.Append(") AS __pc_rows");
                break;

            // gh-1001: whether the join has a row, which paging can decide too
            case JoinAggregate.Any:
                builder.Append("SELECT CASE WHEN EXISTS (");
                WriteRows(builder, "1 AS __pc_one");
                builder.Append(") THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END");
                break;

            default:
                WriteRows(builder, null);
                break;
        }
    }

    private void WriteRows(ICommandBuilder builder, string? aggregateColumn)
    {
        // A derived table or an EXISTS may only carry an ORDER BY that decides which rows it holds
        var orderBys = aggregateColumn == null || IsPaged ? OrderBys : [];

        builder.Append("SELECT ");

        if (Limit.HasValue && !Offset.HasValue)
        {
            builder.Append($"TOP({Limit.Value}) ");
        }

        if (aggregateColumn != null)
        {
            builder.Append(aggregateColumn);
        }
        else if (DistinctSelect != null)
        {
            DistinctSelect.Apply(builder);
        }
        else
        {
            builder.Append($"{OuterAlias}.data, {InnerAlias}.data");
        }

        builder.Append(" FROM ");

        if (DistinctColumns != null)
        {
            builder.Append("(SELECT DISTINCT ");
            builder.Append(DistinctColumns);
            builder.Append(" FROM ");
            WriteJoin(builder);
            builder.Append(") AS __pc_distinct");
        }
        else
        {
            WriteJoin(builder);
        }

        if (orderBys.Count > 0)
        {
            builder.Append(" ORDER BY ");
            for (var i = 0; i < orderBys.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.Append(orderBys[i].Locator);
                if (orderBys[i].Descending) builder.Append(" DESC");
            }
        }

        if (Offset.HasValue)
        {
            if (orderBys.Count == 0)
            {
                builder.Append(" ORDER BY (SELECT NULL)");
            }

            builder.Append($" OFFSET {Offset.Value} ROWS");
            if (Limit.HasValue)
            {
                builder.Append($" FETCH NEXT {Limit.Value} ROWS ONLY");
            }
        }
    }

    private void WriteJoin(ICommandBuilder builder)
    {
        builder.Append(OuterTable);
        builder.Append(" ");
        builder.Append(OuterAlias);

        builder.Append(IsLeftJoin ? " LEFT JOIN " : " INNER JOIN ");

        builder.Append(InnerTable);
        builder.Append(" ");
        builder.Append(InnerAlias);
        builder.Append(" ON ");
        builder.Append(OuterKeyLocator);
        builder.Append(" = ");
        builder.Append(InnerKeyLocator);

        // For LEFT JOIN, inner table filters must be in the ON clause (not WHERE)
        // to avoid filtering out non-matching outer rows where inner columns are NULL.
        if (IsLeftJoin && InnerWheres.Count > 0)
        {
            foreach (var where in InnerWheres)
            {
                builder.Append(" AND ");
                where.Apply(builder);
            }
        }

        // Combine WHERE clauses: always include outer, include inner only for INNER JOIN
        var whereFragments = new List<ISqlFragment>();
        whereFragments.AddRange(OuterWheres);
        if (!IsLeftJoin)
        {
            whereFragments.AddRange(InnerWheres);
        }

        if (whereFragments.Count > 0)
        {
            builder.Append(" WHERE ");
            for (var i = 0; i < whereFragments.Count; i++)
            {
                if (i > 0) builder.Append(" AND ");
                whereFragments[i].Apply(builder);
            }
        }
    }

    /// <summary>
    ///     Replaces bare column references (data, id) with aliased versions for a given table alias.
    /// </summary>
    /// <remarks>
    ///     #875: every column a locator can name, wherever it sits in the locator, because both sides of
    ///     a join have one. This used to know the functions a member reads the document with and the
    ///     literal <c>id</c>, so <c>x.Id.Length</c>, <c>x.Id.ToLower()</c> and <c>x.Id != null</c> all
    ///     left an <c>id</c> behind for the server to call ambiguous. A quoted JSON path is left alone:
    ///     <c>'$.id'</c> is a key, and splitting on the quote keeps the odd pieces out of the match,
    ///     doubled quotes included.
    /// </remarks>
    public static string AliasLocator(string locator, string alias)
    {
        var pieces = locator.Split('\'');

        for (var i = 0; i < pieces.Length; i += 2)
        {
            pieces[i] = BareColumn().Replace(pieces[i], $"{alias}.$0");
        }

        return string.Join('\'', pieces);
    }

    // Not when it is part of a longer name, already qualified, a parameter or a bracketed identifier.
    [GeneratedRegex(@"(?<![\w.@\[])(?:data|id)(?![\w\]])")]
    private static partial Regex BareColumn();
}

/// <summary>gh-1001: what a <see cref="JoinStatement" /> answers instead of its rows.</summary>
internal enum JoinAggregate
{
    None,
    Count,
    LongCount,
    Any
}
