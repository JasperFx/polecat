using JasperFx;
using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     Builds a complete SELECT statement with WHERE, ORDER BY, and pagination.
/// </summary>
internal class Statement
{
    public string FromTable { get; set; } = "";
    public string SelectColumns { get; set; } = "data";

    /// <summary>
    ///     When set, the select list instead of <see cref="SelectColumns" />. A select list that has to
    ///     bind a parameter, like <c>COUNT(CASE WHEN x &gt; @p0 THEN 1 END)</c>, cannot be a string.
    /// </summary>
    public ISqlFragment? SelectFragment { get; set; }

    public List<ISqlFragment> Wheres { get; } = [];
    public List<OrderByClause> OrderBys { get; } = [];
    public int? Limit { get; set; }
    public int? Offset { get; set; }
    public bool IsExistsWrapper { get; set; }
    public bool IsDistinct { get; set; }
    public List<string> GroupByColumns { get; } = [];
    public List<ISqlFragment> HavingClauses { get; } = [];

    /// <summary>
    ///     When set, the SQL locator for the LINQ DistinctBy() key. SQL Server has no
    ///     PostgreSQL-style DISTINCT ON, so the query is wrapped in a ROW_NUMBER() windowed
    ///     subquery partitioned by this locator, keeping one row per distinct key.
    /// </summary>
    public string? DistinctByLocator { get; set; }

    /// <summary>
    ///     When set, the rows are the distinct combinations of these columns, in a derived table, and the
    ///     select list and the ordering read them from there. See <c>DistinctProjection</c>.
    /// </summary>
    public string? DistinctColumns { get; set; }

    /// <summary>
    ///     When set, this statement becomes a derived table and the query is this aggregate over it.
    ///     That is what counting the groups of a grouped query takes, because <c>COUNT(*)</c> in its
    ///     own select list would count the rows of each group instead.
    /// </summary>
    public string? OuterAggregate { get; set; }

    /// <summary>
    ///     When set, the page an earlier <c>Take()</c>/<c>Skip()</c> chose, which everything else on this
    ///     statement applies to: <see cref="PageFilters" />, <see cref="OrderBys" />, <see cref="Limit" />,
    ///     <see cref="Offset" />, the select list and any aggregate. <see cref="Wheres" /> still decide which
    ///     rows the page is chosen from, so the scoping an execution path adds after parsing (tenant, soft
    ///     deletes, <c>doc_type</c>) is applied before the page, as it has to be.
    /// </summary>
    /// <remarks>
    ///     gh-996: one flat statement could only rewrite its own clauses, so an operator after a page applied to
    ///     the whole query. <c>OrderBy(x =&gt; x.Number).Take(4).OrderByDescending(x =&gt; x.Number)</c> cleared the
    ///     first ordering and took the TOP(4) of the second, a different page.
    /// </remarks>
    public PageDefinition? InnerPage { get; private set; }

    /// <summary>Filters on the rows of <see cref="InnerPage" />, from a <c>Where</c> written after the page.</summary>
    public List<ISqlFragment> PageFilters { get; } = [];

    /// <summary>
    ///     Moves the current ordering and page into <see cref="InnerPage" />, so what comes next applies to the
    ///     page. Answers false when there is already one, which would need a second level of derived table.
    /// </summary>
    public bool TryWrapPage()
    {
        if (InnerPage != null) return false;

        InnerPage = new PageDefinition([..OrderBys], Limit, Offset);
        OrderBys.Clear();
        Limit = null;
        Offset = null;
        return true;
    }

    /// <summary>
    ///     Makes the statement read the last row of its ordering, as the first row of the reversed one.
    /// </summary>
    /// <remarks>
    ///     gh-996: after a page, the last row is the page's, so the page is wrapped and its ordering reversed
    ///     outside it. Reversing the statement's own ordering took the last row of the whole query.
    /// </remarks>
    public void ReadLastRow()
    {
        if (IsPaged)
        {
            if (!TryWrapPage())
            {
                throw new BadLinqExpressionException(
                    "Polecat cannot translate Last() after a Take() or Skip() over rows that an earlier Take() "
                    + "or Skip() had already paged and then reordered or filtered. Materialize the query with "
                    + "ToListAsync() and take the last row in memory.");
            }

            OrderBys.AddRange(InnerPage!.OrderBys);
        }

        for (var i = 0; i < OrderBys.Count; i++)
        {
            OrderBys[i] = OrderBys[i].Reversed();
        }

        Limit = 1;
    }

    /// <summary>The filters the next row-level <c>Where</c> belongs with.</summary>
    public List<ISqlFragment> RowFilters => InnerPage == null ? Wheres : PageFilters;

    /// <summary>Whether a <c>Take()</c> or a <c>Skip()</c> decides which rows this statement holds.</summary>
    public bool IsPaged => Limit.HasValue || Offset.HasValue;

    /// <summary>
    ///     The name of the one column of the derived table a paged aggregate reads. See
    ///     <see cref="SelectAggregate" />.
    /// </summary>
    internal const string PagedValueColumn = "__pc_paged";

    /// <summary>
    ///     Makes this statement answer <paramref name="aggregateOf" /> over <paramref name="value" />, e.g.
    ///     <c>v =&gt; $"MAX({v})"</c>. Unpaged, the aggregate is the select list, as it always was.
    /// </summary>
    /// <remarks>
    ///     gh-995 (marten#5920): paged, the aggregate cannot be the select list. <c>TOP(3) COUNT(*)</c> is the
    ///     count of the table with three rows allowed, and <c>OFFSET 4 ROWS</c> skips the one row an aggregate
    ///     returns, so <c>Take(3).Count()</c> came back as the whole count and <c>Skip(4).Count()</c> read no
    ///     row at all. The paged statement becomes a derived table selecting the value, and keeps its
    ///     ordering because that decides which rows are on the page; the aggregate runs over it.
    /// </remarks>
    public void SelectAggregate(Func<string, string> aggregateOf, string value)
    {
        if (!IsPaged)
        {
            SelectColumns = aggregateOf(value);
            return;
        }

        SelectFragment = null;
        SelectColumns = $"{value} AS {PagedValueColumn}";
        OuterAggregate = aggregateOf(PagedValueColumn);
    }

    /// <summary>
    ///     Whether the page is rendered with <c>TOP</c>. <c>Take(0)</c> is, even after a <c>Skip</c>: nothing
    ///     follows an empty page, and SQL Server refuses <c>FETCH NEXT 0 ROWS</c>.
    /// </summary>
    private bool PagesWithTop => Limit.HasValue && (!Offset.HasValue || Limit.Value == 0);

    private bool PagesWithOffset => Offset.HasValue && Limit != 0;

    public void Apply(ICommandBuilder builder)
    {
        if (OuterAggregate != null)
        {
            builder.Append("SELECT ");
            builder.Append(OuterAggregate);
            builder.Append(" FROM (");
            ApplyInner(builder);
            builder.Append(") AS __pc_groups");
            return;
        }

        if (IsExistsWrapper)
        {
            builder.Append("SELECT CASE WHEN EXISTS (");
            ApplyInner(builder);
            builder.Append(") THEN 1 ELSE 0 END");
            return;
        }

        ApplyInner(builder);
    }

    private void ApplyInner(ICommandBuilder builder)
    {
        if (DistinctByLocator != null)
        {
            ApplyDistinctBy(builder);
            return;
        }

        if (DistinctColumns != null)
        {
            ApplyDistinctColumns(builder);
            return;
        }

        builder.Append("SELECT ");

        if (IsDistinct) builder.Append("DISTINCT ");

        // Use TOP when there's a limit but no offset
        if (PagesWithTop)
        {
            builder.Append($"TOP({Limit.Value}) ");
        }

        AppendSelectList(builder);
        builder.Append(" FROM ");
        AppendSource(builder);

        if (GroupByColumns.Count > 0)
        {
            builder.Append(" GROUP BY ");
            for (var i = 0; i < GroupByColumns.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.Append(GroupByColumns[i]);
            }
        }

        if (HavingClauses.Count > 0)
        {
            builder.Append(" HAVING ");
            for (var i = 0; i < HavingClauses.Count; i++)
            {
                if (i > 0) builder.Append(" AND ");
                HavingClauses[i].Apply(builder);
            }
        }

        // Skip ORDER BY for aggregate queries (COUNT, SUM, etc.) — SQL Server disallows it
        if (OrderBys.Count > 0 && !IsAggregateSelect())
        {
            builder.Append(" ORDER BY ");
            AppendOrderByList(builder);
        }

        // OFFSET/FETCH pagination (requires ORDER BY)
        if (PagesWithOffset)
        {
            if (OrderBys.Count == 0 || IsAggregateSelect())
            {
                // gh-995: SELECT DISTINCT only orders by what it selects, and (SELECT NULL) is not that.
                // The first column is, by position, and the order is arbitrary anyway.
                builder.Append(IsDistinct ? " ORDER BY 1" : " ORDER BY (SELECT NULL)");
            }

            AppendOffsetFetch(builder);
        }
    }

    private void AppendOffsetFetch(ICommandBuilder builder)
    {
        builder.Append($" OFFSET {Offset!.Value} ROWS");
        if (Limit.HasValue)
        {
            builder.Append($" FETCH NEXT {Limit.Value} ROWS ONLY");
        }
    }

    /// <summary>
    ///     Emits a DistinctBy() query as a ROW_NUMBER() windowed subquery. The inner query keeps the
    ///     original SELECT columns plus a row number partitioned by the key locator; the outer query
    ///     returns only the first row of each partition. The inner window orders by any supplied
    ///     ORDER BY (so it decides which row survives per key), falling back to an arbitrary order.
    ///     The outer query re-applies ORDER BY / pagination to the de-duplicated result set.
    /// </summary>
    private void ApplyDistinctBy(ICommandBuilder builder)
    {
        builder.Append("SELECT ");

        // TOP applies to the de-duplicated outer result when there's no offset.
        if (PagesWithTop)
        {
            builder.Append($"TOP({Limit.Value}) ");
        }

        AppendSelectList(builder);

        builder.Append(" FROM (SELECT ");
        AppendSelectList(builder);
        builder.Append(", ROW_NUMBER() OVER (PARTITION BY ");
        builder.Append(DistinctByLocator!);
        builder.Append(" ORDER BY ");
        builder.Append(WindowOrderBy());
        builder.Append(") AS __pc_distinct_rn FROM ");
        AppendSource(builder);

        builder.Append(") AS __pc_distinct WHERE __pc_distinct_rn = 1");

        AppendOuterOrderingAndPaging(builder);
    }

    /// <summary>
    ///     Emits the distinct combinations of <see cref="DistinctColumns" /> as a derived table, reading
    ///     the select list, the ordering and the page from outside it.
    /// </summary>
    private void ApplyDistinctColumns(ICommandBuilder builder)
    {
        builder.Append("SELECT ");

        if (PagesWithTop)
        {
            builder.Append($"TOP({Limit.Value}) ");
        }

        AppendSelectList(builder);

        builder.Append(" FROM (SELECT DISTINCT ");
        builder.Append(DistinctColumns!);
        builder.Append(" FROM ");
        AppendSource(builder);

        builder.Append(") AS __pc_distinct");

        AppendOuterOrderingAndPaging(builder);
    }

    private void AppendOuterOrderingAndPaging(ICommandBuilder builder)
    {
        var skipOrderBy = IsAggregateSelect();

        if (OrderBys.Count > 0 && !skipOrderBy)
        {
            builder.Append(" ORDER BY ");
            AppendOrderByList(builder);
        }

        if (PagesWithOffset)
        {
            if (OrderBys.Count == 0 || skipOrderBy)
            {
                builder.Append(" ORDER BY (SELECT NULL)");
            }

            AppendOffsetFetch(builder);
        }
    }

    private string WindowOrderBy()
    {
        if (OrderBys.Count == 0)
        {
            // No deterministic representative requested; any row in the partition is acceptable.
            return "(SELECT NULL)";
        }

        // ⚠️ A window function's ordering is rendered into a STRING, so a parameterised term cannot
        // go here -- there is nowhere to bind it. Refused by name rather than rendered as the fragment's
        // ToString(), which would be valid-looking SQL naming a parameter that was never added.
        var parameterised = OrderBys.FirstOrDefault(o => o.Literal is null);
        if (parameterised is not null)
        {
            throw new BadLinqExpressionException(
                "An ordering that binds a parameter (a vector distance, say) cannot be used inside the "
                + "window function this query needs. Order by a literal key, or run the search through "
                + "VectorSearchAsync.");
        }

        return string.Join(", ", OrderBys.Select(o => o.Descending ? $"{o.Literal} DESC" : o.Literal));
    }

    private void AppendSelectList(ICommandBuilder builder)
    {
        if (SelectFragment != null)
        {
            SelectFragment.Apply(builder);
        }
        else
        {
            builder.Append(SelectColumns);
        }
    }

    /// <summary>
    ///     The rows this statement reads: its table and <see cref="Wheres" />, or, with an
    ///     <see cref="InnerPage" />, that page of them as a derived table and <see cref="PageFilters" />.
    /// </summary>
    private void AppendSource(ICommandBuilder builder)
    {
        if (InnerPage == null)
        {
            builder.Append(FromTable);
            AppendWheres(builder, Wheres);
            return;
        }

        // Every column of the table, so the select list, the orderings and the filters out here read the
        // same columns they would off the table itself.
        builder.Append("(SELECT ");
        var (orderBys, limit, offset) = InnerPage;
        if (limit.HasValue && (!offset.HasValue || limit.Value == 0))
        {
            builder.Append($"TOP({limit.Value}) ");
        }

        builder.Append("* FROM ");
        builder.Append(FromTable);
        AppendWheres(builder, Wheres);

        if (orderBys.Count > 0)
        {
            builder.Append(" ORDER BY ");
            for (var i = 0; i < orderBys.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                orderBys[i].Apply(builder);
            }
        }

        if (offset.HasValue && limit != 0)
        {
            if (orderBys.Count == 0) builder.Append(" ORDER BY (SELECT NULL)");

            builder.Append($" OFFSET {offset.Value} ROWS");
            if (limit.HasValue) builder.Append($" FETCH NEXT {limit.Value} ROWS ONLY");
        }

        builder.Append(") AS __pc_page");
        AppendWheres(builder, PageFilters);
    }

    private static void AppendWheres(ICommandBuilder builder, List<ISqlFragment> wheres)
    {
        if (wheres.Count == 0) return;

        builder.Append(" WHERE ");
        for (var i = 0; i < wheres.Count; i++)
        {
            if (i > 0) builder.Append(" AND ");
            wheres[i].Apply(builder);
        }
    }

    private void AppendOrderByList(ICommandBuilder builder)
    {
        for (var i = 0; i < OrderBys.Count; i++)
        {
            if (i > 0) builder.Append(", ");
            OrderBys[i].Apply(builder);
        }
    }

    /// <summary>
    ///     Whether this is one aggregate over the whole table, where an ORDER BY means nothing and SQL
    ///     Server rejects it. A GROUP BY yields a row per group, so the same select list can be ordered
    ///     (#837): <c>Select(g =&gt; g.Count()).OrderByDescending(x =&gt; x)</c> lost its ordering here.
    /// </summary>
    private bool IsAggregateSelect()
    {
        if (GroupByColumns.Count > 0) return false;

        return SelectColumns.StartsWith("COUNT(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("CAST(COUNT(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("SUM(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("ISNULL(SUM(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("MIN(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("MAX(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("AVG(", StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>The ordering and the page of <see cref="Statement.InnerPage" />.</summary>
internal sealed record PageDefinition(List<OrderByClause> OrderBys, int? Limit, int? Offset);
