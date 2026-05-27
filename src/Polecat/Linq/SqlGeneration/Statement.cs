using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     Builds a complete SELECT statement with WHERE, ORDER BY, and pagination.
/// </summary>
internal class Statement
{
    public string FromTable { get; set; } = "";
    public string SelectColumns { get; set; } = "data";
    public List<ISqlFragment> Wheres { get; } = [];
    public List<(string Locator, bool Descending)> OrderBys { get; } = [];
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

    public void Apply(ICommandBuilder builder)
    {
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

        builder.Append("SELECT ");

        if (IsDistinct) builder.Append("DISTINCT ");

        // Use TOP when there's a limit but no offset
        if (Limit.HasValue && !Offset.HasValue)
        {
            builder.Append($"TOP({Limit.Value}) ");
        }

        builder.Append(SelectColumns);
        builder.Append(" FROM ");
        builder.Append(FromTable);

        AppendWheres(builder);

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
        if (Offset.HasValue)
        {
            if (OrderBys.Count == 0 || IsAggregateSelect())
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

    /// <summary>
    ///     Emits a DistinctBy() query as a ROW_NUMBER() windowed subquery. The inner query keeps the
    ///     original SELECT columns plus a row number partitioned by the key locator; the outer query
    ///     returns only the first row of each partition. The inner window orders by any supplied
    ///     ORDER BY (so it decides which row survives per key), falling back to an arbitrary order.
    ///     The outer query re-applies ORDER BY / pagination to the de-duplicated result set.
    /// </summary>
    private void ApplyDistinctBy(ICommandBuilder builder)
    {
        var skipOrderBy = IsAggregateSelect();

        builder.Append("SELECT ");

        // TOP applies to the de-duplicated outer result when there's no offset.
        if (Limit.HasValue && !Offset.HasValue)
        {
            builder.Append($"TOP({Limit.Value}) ");
        }

        builder.Append(SelectColumns);

        builder.Append(" FROM (SELECT ");
        builder.Append(SelectColumns);
        builder.Append(", ROW_NUMBER() OVER (PARTITION BY ");
        builder.Append(DistinctByLocator!);
        builder.Append(" ORDER BY ");
        builder.Append(WindowOrderBy());
        builder.Append(") AS __pc_distinct_rn FROM ");
        builder.Append(FromTable);

        AppendWheres(builder);

        builder.Append(") AS __pc_distinct WHERE __pc_distinct_rn = 1");

        if (OrderBys.Count > 0 && !skipOrderBy)
        {
            builder.Append(" ORDER BY ");
            AppendOrderByList(builder);
        }

        if (Offset.HasValue)
        {
            if (OrderBys.Count == 0 || skipOrderBy)
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

    private string WindowOrderBy()
    {
        if (OrderBys.Count == 0)
        {
            // No deterministic representative requested; any row in the partition is acceptable.
            return "(SELECT NULL)";
        }

        return string.Join(", ", OrderBys.Select(o => o.Descending ? $"{o.Locator} DESC" : o.Locator));
    }

    private void AppendWheres(ICommandBuilder builder)
    {
        if (Wheres.Count == 0) return;

        builder.Append(" WHERE ");
        for (var i = 0; i < Wheres.Count; i++)
        {
            if (i > 0) builder.Append(" AND ");
            Wheres[i].Apply(builder);
        }
    }

    private void AppendOrderByList(ICommandBuilder builder)
    {
        for (var i = 0; i < OrderBys.Count; i++)
        {
            if (i > 0) builder.Append(", ");
            builder.Append(OrderBys[i].Locator);
            if (OrderBys[i].Descending) builder.Append(" DESC");
        }
    }

    private bool IsAggregateSelect()
    {
        return SelectColumns.StartsWith("COUNT(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("CAST(COUNT(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("SUM(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("ISNULL(SUM(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("MIN(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("MAX(", StringComparison.OrdinalIgnoreCase)
               || SelectColumns.StartsWith("AVG(", StringComparison.OrdinalIgnoreCase);
    }
}
