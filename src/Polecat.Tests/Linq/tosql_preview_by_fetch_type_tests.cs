using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     gh-1018 (ported from marten#5916, Marten's <c>ToCommand(FetchType)</c>): <c>ToSql()</c> previewed only
///     the many-rows statement, so there was no way to see what <c>CountAsync()</c>, <c>AnyAsync()</c>,
///     <c>FirstAsync()</c> or an aggregate runs -- the shapes gh-995 changed. Every fact previews a query shape
///     for each terminal operator, executes the query with that operator, and compares the preview with the
///     text SQL Server ran, read back from <c>sys.dm_exec_query_stats</c> as gh-1002's tests do.
/// </summary>
/// <remarks>
///     The executed text is every statement against the table that started after the operator's execution
///     did, and a comparison passes only when that is exactly one statement equal to the preview, so one
///     terminal's statement can't stand in for another's.
/// </remarks>
public class tosql_preview_by_fetch_type_tests : OneOffConfigurationsContext
{
    private const string Schema = "tosql_preview_by_fetch_type_tests";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public tosql_preview_by_fetch_type_tests()
    {
        // Without pooling, disposing the executing session closes its connection, which is what finishes the
        // request on the server. A pooled connection that goes back idle leaves a cached statement's re-run
        // out of sys.dm_exec_query_stats -- its last_execution_time does not move -- so a statement an
        // earlier fact had already run would not be read back as executed.
        ConfigureStore(opts => opts.ConnectionString =
            new SqlConnectionStringBuilder(ConnectionSource.ConnectionString) { Pooling = false }.ConnectionString);
    }

    /// <summary>One way of executing a query, and the preview that is supposed to show its SQL.</summary>
    private sealed record Terminal<T>(
        string Name,
        Func<IQuerySession, IQueryable<T>, string> Preview,
        Func<IQueryable<T>, Task> Execute);

    private static Terminal<T>[] TerminalsFor<T>(bool rowOperators = true)
    {
        var terminals = new List<Terminal<T>>
        {
            new("ToListAsync() previewed by FetchType.FetchMany",
                (s, q) => s.ToSql(q, FetchType.FetchMany), q => q.ToListAsync(Ct)),
            new("CountAsync() previewed by FetchType.Count",
                (s, q) => s.ToSql(q, FetchType.Count), q => q.CountAsync(Ct)),
            new("AnyAsync() previewed by FetchType.Any",
                (s, q) => s.ToSql(q, FetchType.Any), q => q.AnyAsync(Ct)),
            new("LongCountAsync() previewed by q => q.LongCount()",
                (s, q) => s.ToSql(q, x => x.LongCount()), q => q.LongCountAsync(Ct)),
            new("CountAsync() previewed by q => q.Count()",
                (s, q) => s.ToSql(q, x => x.Count()), q => q.CountAsync(Ct)),
        };

        if (rowOperators)
        {
            terminals.AddRange(
            [
                new("FirstAsync() previewed by FetchType.FetchOne",
                    (s, q) => s.ToSql(q, FetchType.FetchOne), q => q.FirstAsync(Ct)),
                new("FirstOrDefaultAsync() previewed by FetchType.FetchOne",
                    (s, q) => s.ToSql(q, FetchType.FetchOne), q => q.FirstOrDefaultAsync(Ct)),
                new("SingleAsync() previewed by q => q.Single()",
                    (s, q) => s.ToSql(q, x => x.Single()), q => q.SingleAsync(Ct)),
                new("SingleOrDefaultAsync() previewed by q => q.SingleOrDefault()",
                    (s, q) => s.ToSql(q, x => x.SingleOrDefault()), q => q.SingleOrDefaultAsync(Ct)),
                new("LastAsync() previewed by q => q.Last()",
                    (s, q) => s.ToSql(q, x => x.Last()), q => q.LastAsync(Ct)),
                new("LastOrDefaultAsync() previewed by q => q.LastOrDefault()",
                    (s, q) => s.ToSql(q, x => x.LastOrDefault()), q => q.LastOrDefaultAsync(Ct))
            ]);
        }

        return terminals.ToArray();
    }

    private static Terminal<PreviewDoc>[] DocumentTerminals() =>
    [
        new("CountAsync(predicate) previewed by q => q.Count(predicate)",
            (s, q) => s.ToSql(q, x => x.Count(d => d.Number > 1)), q => q.CountAsync(d => d.Number > 1, Ct)),
        new("AnyAsync(predicate) previewed by q => q.Any(predicate)",
            (s, q) => s.ToSql(q, x => x.Any(d => d.Number > 4)), q => q.AnyAsync(d => d.Number > 4, Ct)),
        new("FirstAsync(predicate) previewed by q => q.First(predicate)",
            (s, q) => s.ToSql(q, x => x.First(d => d.Number > 1)), q => q.FirstAsync(d => d.Number > 1, Ct)),
        new("FirstOrDefaultAsync(predicate) previewed by q => q.FirstOrDefault(predicate)",
            (s, q) => s.ToSql(q, x => x.FirstOrDefault(d => d.Number > 1)),
            q => q.FirstOrDefaultAsync(d => d.Number > 1, Ct)),
        new("LastAsync(predicate) previewed by q => q.Last(predicate)",
            (s, q) => s.ToSql(q, x => x.Last(d => d.Number > 1)), q => q.LastAsync(d => d.Number > 1, Ct)),
        new("SumAsync() previewed by q => q.Sum(selector)",
            (s, q) => s.ToSql(q, x => x.Sum(d => d.Number)), q => q.SumAsync(d => d.Number, Ct)),
        new("MinAsync() previewed by q => q.Min(selector)",
            (s, q) => s.ToSql(q, x => x.Min(d => d.Number)), q => q.MinAsync(d => d.Number, Ct)),
        new("MaxAsync() previewed by q => q.Max(selector)",
            (s, q) => s.ToSql(q, x => x.Max(d => d.Number)), q => q.MaxAsync(d => d.Number, Ct)),
        new("AverageAsync() previewed by q => q.Average(selector)",
            (s, q) => s.ToSql(q, x => x.Average(d => d.Number)), q => q.AverageAsync(d => d.Number, Ct))
    ];

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        for (var i = 1; i <= 5; i++)
        {
            session.Store(new PreviewDoc { Id = Guid.NewGuid(), Number = i, Name = "n" + (i % 3) });
            session.Store(new PreviewVersionedDoc { Id = Guid.NewGuid(), Number = i, Name = "n" + (i % 3) });
        }

        await session.SaveChangesAsync(Ct);
    }

    private async Task previewShouldBeWhatExecutes<T>(string tableTypeName,
        Func<IQuerySession, IQueryable<T>> query, IEnumerable<Terminal<T>> terminals)
    {
        await SeedAsync();

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);

        var pattern = $"%{Schema}%pc_doc_{tableTypeName}%";
        var mismatches = new List<string>();
        foreach (var terminal in terminals)
        {
            var mismatch = await compareAsync(conn, pattern, query, terminal);
            if (mismatch != null) mismatches.Add(mismatch);
        }

        if (mismatches.Count != 0)
        {
            throw new ShouldAssertException(string.Join(Environment.NewLine + Environment.NewLine, mismatches));
        }
    }

    private async Task<string?> compareAsync<T>(SqlConnection conn, string pattern,
        Func<IQuerySession, IQueryable<T>> query, Terminal<T> terminal)
    {
        string? previewed = null;
        BadLinqExpressionException? previewRefusal = null;
        await using (var session = theStore.QuerySession())
        {
            try
            {
                previewed = normalize(terminal.Preview(session, query(session)));
            }
            catch (BadLinqExpressionException e)
            {
                previewRefusal = e;
            }
        }

        var start = await serverNowAsync(conn);

        BadLinqExpressionException? executionRefusal = null;
        await using (var session = theStore.QuerySession())
        {
            try
            {
                await terminal.Execute(query(session));
            }
            catch (BadLinqExpressionException e)
            {
                executionRefusal = e;
            }
            catch (InvalidOperationException)
            {
                // No element, or more than one for Single(): the statement ran, and that is what is compared
            }
        }

        var executed = await executedSinceAsync(conn, pattern, start);

        if (executionRefusal != null || previewRefusal != null)
        {
            return executionRefusal != null && previewRefusal != null
                ? null
                : $"{terminal.Name}: execution {(executionRefusal == null ? "ran" : "was refused")}, "
                  + $"but the preview {(previewRefusal == null ? "rendered" : "was refused")}"
                  + $"{Environment.NewLine}  {(object?)previewRefusal?.Message ?? previewed}"
                  + $"{Environment.NewLine}  {(object?)executionRefusal?.Message ?? string.Join(" | ", executed)}";
        }

        if (executed.Count == 1 && executed[0] == previewed) return null;

        return $"{terminal.Name}:{Environment.NewLine}  previewed: {previewed}{Environment.NewLine}"
               + $"  executed: {string.Join(Environment.NewLine + "            ", executed)}";
    }

    // The server's clock, to read back what ran after it. The pause puts the previous comparison's statements
    // (last_execution_time is a datetime, rounded to ~3ms) strictly before it.
    private static async Task<DateTime> serverNowAsync(SqlConnection conn)
    {
        await Task.Delay(20, Ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT SYSDATETIME()";
        return (DateTime)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    // The statements against the table that started at or after start. The recent entries are narrowed by time
    // BEFORE their text is looked up: sys.dm_exec_sql_text over the whole server's plan cache takes tens of
    // seconds on a busy test server, which a CROSS APPLY with both filters in one WHERE does not avoid.
    private static async Task<List<string>> executedSinceAsync(SqlConnection conn, string pattern, DateTime start)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT qs.sql_handle INTO #recent FROM sys.dm_exec_query_stats qs WHERE qs.last_execution_time >= @start;
            SELECT DISTINCT st.text FROM #recent r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
            WHERE st.text LIKE @pattern;
            DROP TABLE #recent;
            """;
        cmd.Parameters.Add("@start", System.Data.SqlDbType.DateTime2).Value = start;
        cmd.Parameters.AddWithValue("@pattern", pattern);

        var executed = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct))
        {
            var text = normalize(reader.GetString(0));
            if (!executed.Contains(text)) executed.Add(text);
        }

        return executed;
    }

    // sp_executesql text starts with the parameter declarations, "(@p0 int,@p1 nvarchar(4000))SELECT ..."
    private static string normalize(string sql)
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

    [Fact]
    public Task a_plain_query() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>(), TerminalsFor<PreviewDoc>());

    [Fact]
    public Task a_where() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 2), TerminalsFor<PreviewDoc>());

    // gh-1002's defect: the select list execution reads with is data, guid_version
    [Fact]
    public Task a_document_with_metadata_read_columns() =>
        previewShouldBeWhatExecutes("previewversioneddoc",
            s => s.Query<PreviewVersionedDoc>().Where(x => x.Number > 2), TerminalsFor<PreviewVersionedDoc>());

    // gh-995: a count, an existence check or a row of a page is taken over a derived table of the page
    [Fact]
    public Task an_order_by_and_take() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 0).OrderBy(x => x.Number).Take(3),
            TerminalsFor<PreviewDoc>());

    [Fact]
    public Task a_skip_and_take() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().OrderByDescending(x => x.Number).Skip(1).Take(2),
            TerminalsFor<PreviewDoc>());

    [Fact]
    public Task a_distinct() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 1).Select(x => x.Name).Distinct(),
            TerminalsFor<string>());

    [Fact]
    public Task a_scalar_select() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 1).Select(x => x.Number),
            TerminalsFor<int>());

    [Fact]
    public Task a_projection() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 1).Select(x => new { x.Id, x.Number }),
            TerminalsForAnonymous(s0 => s0.Query<PreviewDoc>().Select(x => new { x.Id, x.Number })));

    [Fact]
    public Task a_group_by_projecting_an_object() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().GroupBy(x => x.Name).Select(g => new { g.Key, Count = g.Count() }),
            TerminalsForAnonymous(s0 => s0.Query<PreviewDoc>().GroupBy(x => x.Name)
                .Select(g => new { g.Key, Count = g.Count() })));

    [Fact]
    public Task a_group_by_projecting_its_key() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().GroupBy(x => x.Name).Select(g => g.Key).OrderBy(x => x),
            TerminalsFor<string>());

    [Fact]
    public Task a_paged_group_by() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().GroupBy(x => x.Name).Select(g => g.Key).OrderBy(x => x).Take(2),
            TerminalsFor<string>());

    // Operators with a predicate or a selector, and the aggregates
    [Fact]
    public Task operators_with_a_predicate_and_the_aggregates() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 0), DocumentTerminals());

    [Fact]
    public Task operators_with_a_predicate_and_the_aggregates_over_a_page() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().OrderBy(x => x.Number).Skip(1).Take(3), DocumentTerminals());

    // The anonymous element type is only nameable through inference
    private static Terminal<T>[] TerminalsForAnonymous<T>(Func<IQuerySession, IQueryable<T>> _) => TerminalsFor<T>();

    [Fact]
    public async Task fetch_many_is_what_the_plain_overload_previews()
    {
        await using var session = theStore.QuerySession();

        var query = session.Query<PreviewVersionedDoc>().Where(x => x.Number > 0).OrderBy(x => x.Number).Take(3);

        session.ToSql(query, FetchType.FetchMany).ShouldBe(session.ToSql(query));
    }

    [Fact]
    public async Task each_fetch_type_previews_its_own_statement()
    {
        await using var session = theStore.QuerySession();

        var query = session.Query<PreviewDoc>().Where(x => x.Number > 0).OrderBy(x => x.Number).Take(3);

        var previews = Enum.GetValues<FetchType>().Select(x => session.ToSql(query, x)).ToList();
        previews.Distinct().Count().ShouldBe(previews.Count);

        // gh-995: Take(3).Count() counts the page, not the table
        session.ToSql(query, FetchType.Count).ShouldContain("COUNT(*)");
        session.ToSql(query, FetchType.Count).ShouldContain("TOP");
    }

    [Fact]
    public async Task a_group_join_is_refused_for_every_fetch_type()
    {
        await using var session = theStore.QuerySession();

        var joined = session.Query<PreviewDoc>()
            .GroupJoin(session.Query<PreviewVersionedDoc>(), d => d.Number, v => v.Number, (d, vs) => new { d, vs })
            .SelectMany(x => x.vs, (x, v) => new { x.d.Name, v.Number });

        foreach (var fetchType in Enum.GetValues<FetchType>())
        {
            Should.Throw<BadLinqExpressionException>(() => session.ToSql(joined, fetchType))
                .Message.ShouldContain("GroupJoin");
        }

        Should.Throw<BadLinqExpressionException>(() => session.ToSql(joined, q => q.LongCount()))
            .Message.ShouldContain("GroupJoin");
    }

    [Fact]
    public async Task a_terminal_that_is_not_a_queryable_operator_is_refused()
    {
        await using var session = theStore.QuerySession();
        var query = session.Query<PreviewDoc>();

        // The async operator executes the query; the preview wants the operator it executes, q.Count()
        Should.Throw<BadLinqExpressionException>(() => session.ToSql(query, q => q.CountAsync(CancellationToken.None)))
            .Message.ShouldContain("CountAsync");

        Should.Throw<BadLinqExpressionException>(() => session.ToSql(query, q => q.ToList()))
            .Message.ShouldContain("ToList");
    }

    [Fact]
    public async Task a_terminal_that_does_not_read_the_query_is_refused()
    {
        await using var session = theStore.QuerySession();
        var query = session.Query<PreviewDoc>();
        var other = session.Query<PreviewVersionedDoc>();

        Should.Throw<BadLinqExpressionException>(() => session.ToSql(query, _ => other.Count()))
            .Message.ShouldContain("does not read");
    }

    [Fact]
    public async Task a_tenant_session_previews_through_its_parent()
    {
        await using var session = theStore.LightweightSession();
        var tenant = session.ForTenant("tenant_a");

        var query = tenant.Query<PreviewDoc>().Where(x => x.Number > 1);

        tenant.ToSql(query, FetchType.Count).ShouldContain("COUNT(*)");
        tenant.ToSql(query, q => q.Sum(x => x.Number)).ShouldContain("SUM(");
    }
}
