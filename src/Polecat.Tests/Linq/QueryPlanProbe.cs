using System.Xml.Linq;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Weasel.SqlServer;

namespace Polecat.Tests.Linq;

/// <summary>One access operator of an actual execution plan.</summary>
/// <param name="Operator">Its physical operator: <c>Index Seek</c>, <c>Index Scan</c>, <c>Table Scan</c>.</param>
/// <param name="Table">The table it read, unquoted.</param>
/// <param name="Index">The index it read, unquoted, or null for a heap.</param>
public record PlanAccess(string Operator, string Table, string? Index);

/// <summary>
///     Runs a query's real statement, with its real parameters, under <c>SET STATISTICS XML</c>, and reads
///     back how SQL Server got to the rows. Parameter binding is the point of the facts that use this, so
///     hand-writing the SQL in a test would measure the test.
/// </summary>
public static class QueryPlanProbe
{
    private static readonly XNamespace Showplan = DocumentStore.ShowPlanNamespace;

    /// <summary>The access operators of <paramref name="queryable" />'s statement.</summary>
    public static async Task<IReadOnlyList<PlanAccess>> AccessesAsync<T>(IQuerySession session,
        IQueryable<T> queryable, CancellationToken token) where T : class
    {
        var provider = (PolecatLinqQueryProvider)queryable.Provider;

        var builder = new BatchBuilder();
        provider.BuildStatement(queryable.Expression, session.TenantId).Apply(builder);
        var batch = builder.Compile();

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);

        await using (var on = conn.CreateCommand())
        {
            on.CommandText = "SET STATISTICS XML ON;";
            await on.ExecuteNonQueryAsync(token);
        }

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = batch.BatchCommands[0].CommandText;
        foreach (SqlParameter parameter in batch.BatchCommands[0].Parameters)
        {
            cmd.Parameters.Add(((ICloneable)parameter).Clone());
        }

        await using var reader = await cmd.ExecuteReaderAsync(token);

        string? plan = null;
        do
        {
            while (await reader.ReadAsync(token))
            {
                if (reader.FieldCount == 1 && reader.GetValue(0) is string text
                                           && text.Contains("ShowPlanXML", StringComparison.Ordinal))
                {
                    plan = text;
                }
            }
        } while (await reader.NextResultAsync(token));

        plan.ShouldNotBeNull("SQL Server returned no execution plan for the query.");
        return ReadAccesses(plan);
    }

    public static PlanAccess[] ReadAccesses(string plan)
    {
        var accesses = new List<PlanAccess>();

        foreach (var relOp in XDocument.Parse(plan).Descendants(Showplan + "RelOp"))
        {
            var access = relOp.Elements().FirstOrDefault(x =>
                x.Name == Showplan + "IndexScan" || x.Name == Showplan + "TableScan");
            var target = access?.Elements(Showplan + "Object").FirstOrDefault();
            if (target == null) continue;

            accesses.Add(new PlanAccess(
                (string)relOp.Attribute("PhysicalOp")!,
                Unquote((string?)target.Attribute("Table"))!,
                Unquote((string?)target.Attribute("Index"))));
        }

        return accesses.ToArray();
    }

    private static string? Unquote(string? name) => name?.Trim('[', ']');
}
