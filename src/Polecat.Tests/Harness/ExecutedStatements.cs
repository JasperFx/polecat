using Microsoft.Data.SqlClient;

namespace Polecat.Tests.Harness;

/// <summary>
///     The statements SQL Server ran against one table while a piece of test code ran, read back from
///     <c>sys.dm_exec_query_stats</c>. Take a <see cref="SnapshotAsync" /> before the code runs and ask the snapshot
///     what has run since.
/// </summary>
/// <remarks>
///     <para>
///         The query stats rows are compared before and after rather than against a clock: comparing
///         <c>last_execution_time</c> with a <c>SYSDATETIME()</c> read just before the code ran both missed
///         statements that had run and picked up ones from before, often enough to fail CI on most days (#1046).
///     </para>
///     <para>
///         The store under test needs <c>Pooling = false</c> on its connection string. Disposing the executing
///         session then closes its connection, which is what finishes the request on the server. A pooled
///         connection that goes back idle leaves a cached statement's re-run out of the query stats -- its
///         <c>last_execution_time</c> does not move -- so a statement that had already run once would not be
///         read back as executed.
///     </para>
/// </remarks>
public static class ExecutedStatements
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <param name="conn">An open connection of the test's own, not the store's.</param>
    /// <param name="pattern">A LIKE pattern naming the table, e.g. <c>%schema%pc_doc_previewdoc%</c>.</param>
    public static async Task<Snapshot> SnapshotAsync(SqlConnection conn, string pattern)
    {
        var since = await serverNowAsync(conn);
        return new Snapshot(conn, pattern, since, await queryStatsAsync(conn, pattern, since));
    }

    public sealed class Snapshot
    {
        private readonly SqlConnection _conn;
        private readonly string _pattern;
        private readonly DateTime _since;
        private readonly Dictionary<string, (string Text, string State)> _before;

        internal Snapshot(SqlConnection conn, string pattern, DateTime since,
            Dictionary<string, (string Text, string State)> before)
        {
            _conn = conn;
            _pattern = pattern;
            _since = since;
            _before = before;
        }

        /// <summary>
        ///     The normalized text of every statement whose query stats row is new since the snapshot or has
        ///     changed. A run can reach the query stats a moment after the session that ran it is gone, so this
        ///     waits up to five seconds for <paramref name="until" /> to hold (by default, for anything at all).
        /// </summary>
        public async Task<List<string>> ExecutedSinceAsync(Func<List<string>, bool>? until = null)
        {
            until ??= x => x.Count != 0;

            var executed = await executedSinceAsync();
            for (var attempt = 0; !until(executed) && attempt < 100; attempt++)
            {
                await Task.Delay(50, Ct);
                executed = await executedSinceAsync();
            }

            return executed;
        }

        private async Task<List<string>> executedSinceAsync()
        {
            var after = await queryStatsAsync(_conn, _pattern, _since);
            return after
                .Where(x => !_before.TryGetValue(x.Key, out var earlier) || earlier.State != x.Value.State)
                .Select(x => x.Value.Text)
                .Distinct()
                .ToList();
        }
    }

    // A lower bound on last_execution_time that only keeps the scan of the plan cache short. Nothing is decided
    // by it: a minute either side of the server's clock is far wider than any difference between the clock
    // SYSDATETIME() reads and the one the query stats are stamped with.
    private static async Task<DateTime> serverNowAsync(SqlConnection conn)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT DATEADD(minute, -1, SYSDATETIME())";
        return (DateTime)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    // The query stats rows of the statements against the table, each with the text it ran and a fingerprint of
    // its state. A row is one statement of one cached plan. Running the statement moves its execution count and
    // last execution time, and a recompile (the facts drop and recreate the table) starts the count again on a
    // new creation time, so any run shows up as a changed fingerprint without comparing clocks. The recent
    // entries are narrowed by time BEFORE their text is looked up: sys.dm_exec_sql_text over the whole
    // server's plan cache takes tens of seconds on a busy test server, which a CROSS APPLY with both filters
    // in one WHERE does not avoid.
    private static async Task<Dictionary<string, (string Text, string State)>> queryStatsAsync(SqlConnection conn,
        string pattern, DateTime since)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT qs.sql_handle, qs.plan_handle, qs.statement_start_offset, qs.creation_time,
                   qs.plan_generation_num, qs.execution_count, qs.last_execution_time
            INTO #recent FROM sys.dm_exec_query_stats qs WHERE qs.last_execution_time >= @since;
            SELECT CONVERT(varchar(max), r.sql_handle, 1) + CONVERT(varchar(max), r.plan_handle, 1)
                       + ':' + CAST(r.statement_start_offset AS varchar(20)),
                   CONVERT(varchar(30), r.creation_time, 126) + '/' + CAST(r.plan_generation_num AS varchar(20))
                       + '/' + CAST(r.execution_count AS varchar(20)) + '/'
                       + CONVERT(varchar(30), r.last_execution_time, 126),
                   st.text
            FROM #recent r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
            WHERE st.text LIKE @pattern;
            DROP TABLE #recent;
            """;
        cmd.Parameters.Add("@since", System.Data.SqlDbType.DateTime2).Value = since;
        cmd.Parameters.AddWithValue("@pattern", pattern);

        var rows = new Dictionary<string, (string Text, string State)>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct))
        {
            rows[reader.GetString(0)] = (Normalize(reader.GetString(2)), reader.GetString(1));
        }

        return rows;
    }

    /// <summary>
    ///     sp_executesql text starts with the parameter declarations, "(@p0 int,@p1 nvarchar(4000))SELECT ...",
    ///     which a preview doesn't have.
    /// </summary>
    public static string Normalize(string sql)
    {
        var text = sql.Trim();
        if (text.StartsWith("(@", StringComparison.Ordinal))
        {
            var depth = 0;
            for (var i = 0; i < text.Length; i++)
            {
                if (text[i] == '(') depth++;
                else if (text[i] == ')') depth--;

                if (depth == 0)
                {
                    text = text[(i + 1)..].Trim();
                    break;
                }
            }
        }

        return text.TrimEnd(';').Trim();
    }
}
