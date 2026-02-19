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

    public void Apply(CommandBuilder builder)
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

    private void ApplyInner(CommandBuilder builder)
    {
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

        if (Wheres.Count > 0)
        {
            builder.Append(" WHERE ");
            for (var i = 0; i < Wheres.Count; i++)
            {
                if (i > 0) builder.Append(" AND ");
                Wheres[i].Apply(builder);
            }
        }

        if (OrderBys.Count > 0)
        {
            builder.Append(" ORDER BY ");
            for (var i = 0; i < OrderBys.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.Append(OrderBys[i].Locator);
                if (OrderBys[i].Descending) builder.Append(" DESC");
            }
        }

        // OFFSET/FETCH pagination (requires ORDER BY)
        if (Offset.HasValue)
        {
            if (OrderBys.Count == 0)
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
}
