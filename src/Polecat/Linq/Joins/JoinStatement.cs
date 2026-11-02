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
