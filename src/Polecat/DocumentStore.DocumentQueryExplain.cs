using System.Xml.Linq;
using JasperFx.Documents;
using Microsoft.Data.SqlClient;

namespace Polecat;

/// <summary>
///     jasperfx#1078 (CritterWatch#1313): how SQL Server executed a document query — rows examined versus
///     returned, the indexes it used, and whether it scanned the table.
/// </summary>
/// <remarks>
///     <para>
///         <b>Explains the statement the query runs, not a reconstruction of it.</b>
///         <see cref="PrepareDiagnosticsQueryAsync" /> builds the page statement once for both
///         <see cref="IDocumentStoreDiagnostics.QueryDocumentsAsync" /> and this, so every condition (tenant,
///         <c>AllTenants</c>, soft deletes, the <c>doc_type</c> filter, <c>IdEquals</c> as the stored identity
///         type, the metadata filters, the translated <c>Where</c>/<c>OrderBy</c>, paging) and every refusal is
///         the same by construction.
///     </para>
///     <para>
///         <b>An ACTUAL plan.</b> <c>SET STATISTICS XML ON</c> makes SQL Server return each statement's actual
///         execution plan as an extra one-row result set after the statement's own rows, with per-operator
///         runtime counters (<c>RunTimeCountersPerThread/@ActualRows</c> and <c>@ActualRowsRead</c>). The page
///         query is executed for that — inside a transaction that is always rolled back, so even a query that
///         could write cannot. <c>SET SHOWPLAN_XML</c> would avoid executing it, but its numbers are the
///         optimizer's guesses, and "did it read far more rows than it returned?" is a question about what
///         happened.
///     </para>
///     <para>
///         Only the page statement is explained, not the count beside it: it is the statement whose rows a
///         console shows, and the count reads the same WHERE.
///     </para>
/// </remarks>
public partial class DocumentStore
{
    internal const string ShowPlanNamespace = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    async Task<DocumentQueryPlan?> IDocumentStoreDiagnostics.ExplainDocumentQueryAsync(
        string documentTypeName, DocumentQueryOptions options, CancellationToken token)
    {
        var query = await PrepareDiagnosticsQueryAsync(documentTypeName, options).ConfigureAwait(false);

        // An unknown type has no query to explain.
        if (query == null) return null;

        if (query.MatchesNothing)
        {
            // An id that cannot be the stored identity type matches nothing without a statement being run,
            // which is the honest plan: nothing was read.
            return new DocumentQueryPlan("SqlServer",
                $"No statement was executed: '{options.IdEquals}' cannot be a {query.Mapping.InnerIdType.Name} identity, so it matches no row.")
            {
                RowsExamined = 0,
                RowsReturned = 0,
                IndexesUsed = [],
                ScansTable = false,
                IsActual = true
            };
        }

        try
        {
            return await Options.ResiliencePipeline.ExecuteAsync(static async (state, ct) =>
            {
                var (store, query) = state;
                return await store.ExplainAsync(query, ct).ConfigureAwait(false);
            }, (this, query), token).ConfigureAwait(false);
        }
        catch (SqlException e) when (e.Number == 262)
        {
            // "SHOWPLAN permission denied": this login cannot read plans, so this store cannot explain the
            // query — which the contract spells null, never "no scan".
            return null;
        }
    }

    private async Task<DocumentQueryPlan> ExplainAsync(DiagnosticsPageQuery query, CancellationToken token)
    {
        await using var conn = new SqlConnection(query.ConnectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);

        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(token).ConfigureAwait(false);

        try
        {
            await setStatisticsXmlAsync(conn, tx, on: true, token).ConfigureAwait(false);

            long returned = 0;
            string? planXml = null;

            await using (var page = query.BuildPage(conn))
            {
                page.Transaction = tx;
                await using var reader = await page.ExecuteReaderAsync(token).ConfigureAwait(false);

                while (await reader.ReadAsync(token).ConfigureAwait(false)) returned++;

                // The plan follows the statement's own rows as a one-row, one-column result set.
                while (planXml == null && await reader.NextResultAsync(token).ConfigureAwait(false))
                {
                    if (await reader.ReadAsync(token).ConfigureAwait(false)
                        && reader.FieldCount == 1
                        && reader.GetValue(0) is string text
                        && text.Contains("ShowPlanXML", StringComparison.Ordinal))
                    {
                        planXml = text;
                    }
                }
            }

            await setStatisticsXmlAsync(conn, tx, on: false, token).ConfigureAwait(false);

            return planXml == null
                ? new DocumentQueryPlan("SqlServer", "SQL Server returned no execution plan for the page query.")
                {
                    RowsReturned = returned,
                    IsActual = true
                }
                : ReadShowPlan(planXml, query.Mapping, returned);
        }
        finally
        {
            // Always rolled back: an explain must not be able to write.
            await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }

        static async Task setStatisticsXmlAsync(SqlConnection conn, SqlTransaction tx, bool on, CancellationToken ct)
        {
            await using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = on ? "SET STATISTICS XML ON;" : "SET STATISTICS XML OFF;";
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Read the facts a console needs out of an actual showplan, restricted to the operators that read the
    ///     document table itself.
    /// </summary>
    /// <remarks>
    ///     <list type="bullet">
    ///     <item><b>RowsExamined</b>: per access operator on the table, <c>ActualRowsRead</c> where SQL Server
    ///     reports it (the rows the operator touched before its residual predicate) and <c>ActualRows</c>
    ///     otherwise, summed over threads and operators. Key lookups are left out: they re-read a row an index
    ///     operator already counted.</item>
    ///     <item><b>IndexesUsed</b>: the distinct <c>Object/@Index</c> names on those operators, lookups
    ///     included (a lookup does use the clustered index). A heap scan names no index.</item>
    ///     <item><b>ScansTable</b>: any <c>Table Scan</c>, <c>Clustered Index Scan</c> or <c>Index Scan</c>
    ///     over the table — reading every row (or every index entry) rather than seeking.</item>
    ///     </list>
    /// </remarks>
    internal static DocumentQueryPlan ReadShowPlan(string planXml, Storage.DocumentMapping mapping, long rowsReturned)
    {
        XNamespace ns = ShowPlanNamespace;
        var document = XDocument.Parse(planXml);

        long examined = 0;
        var anyCounted = false;
        var scans = false;
        var indexes = new List<string>();

        foreach (var relOp in document.Descendants(ns + "RelOp"))
        {
            // The access element is a DIRECT child of its RelOp; descendants would reach nested operators.
            var access = relOp.Elements().FirstOrDefault(x =>
                x.Name == ns + "IndexScan" || x.Name == ns + "TableScan");
            if (access == null) continue;

            var target = access.Elements(ns + "Object").FirstOrDefault();
            if (target == null || !isDocumentTable(target)) continue;

            var indexName = unquote((string?)target.Attribute("Index"));
            if (indexName != null && !indexes.Contains(indexName, StringComparer.OrdinalIgnoreCase))
            {
                indexes.Add(indexName);
            }

            var physicalOp = (string?)relOp.Attribute("PhysicalOp");
            if (physicalOp is "Table Scan" or "Clustered Index Scan" or "Index Scan")
            {
                scans = true;
            }

            var isLookup = (string?)access.Attribute("Lookup") is "1" or "true";
            if (isLookup) continue;

            foreach (var counters in relOp.Elements(ns + "RunTimeInformation").Elements(ns + "RunTimeCountersPerThread"))
            {
                var read = (long?)counters.Attribute("ActualRowsRead") ?? (long?)counters.Attribute("ActualRows");
                if (read is not { } value) continue;

                examined += value;
                anyCounted = true;
            }
        }

        return new DocumentQueryPlan("SqlServer", planXml)
        {
            RowsReturned = rowsReturned,
            // Never fewer than were returned: every returned row was read.
            RowsExamined = anyCounted ? Math.Max(examined, rowsReturned) : null,
            IndexesUsed = indexes,
            ScansTable = scans,
            IsActual = true
        };

        bool isDocumentTable(XElement target)
            => string.Equals(unquote((string?)target.Attribute("Table")), mapping.TableName,
                   StringComparison.OrdinalIgnoreCase)
               && string.Equals(unquote((string?)target.Attribute("Schema")), mapping.DatabaseSchemaName,
                   StringComparison.OrdinalIgnoreCase);

        static string? unquote(string? name) => name?.Trim('[', ']');
    }
}
