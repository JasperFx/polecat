using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Linq.Joins;

/// <summary>
///     Generates JOIN SQL for GroupJoin queries.
///     Selects both outer and inner data columns for in-memory projection.
/// </summary>
internal class JoinStatement
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
    public string SelectColumns { get; set; } = "";

    public void Apply(ICommandBuilder builder)
    {
        builder.Append("SELECT ");

        if (Limit.HasValue && !Offset.HasValue)
        {
            builder.Append($"TOP({Limit.Value}) ");
        }

        if (string.IsNullOrEmpty(SelectColumns))
        {
            builder.Append($"{OuterAlias}.data, {InnerAlias}.data");
        }
        else
        {
            builder.Append(SelectColumns);
        }

        builder.Append(" FROM ");
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

    /// <summary>
    ///     Replaces bare column references (data, id) with aliased versions for a given table alias.
    /// </summary>
    public static string AliasLocator(string locator, string alias)
    {
        // Replace "JSON_VALUE(data," with "JSON_VALUE(alias.data,", and the OPENJSON read a string
        // member uses on nvarchar(max) storage (#835), which is otherwise ambiguous in a join.
        var result = locator
            .Replace("JSON_VALUE(data,", $"JSON_VALUE({alias}.data,")
            .Replace("OPENJSON(data)", $"OPENJSON({alias}.data)");

        // Replace bare "id" with "alias.id" (but not inside JSON paths or CAST)
        if (result == "id")
        {
            result = $"{alias}.id";
        }

        return result;
    }
}
