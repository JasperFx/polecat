using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;

namespace Polecat.Tests;

/// <summary>
///     polecat#517 — a statement queued with <c>QueueSqlCommand</c> was silently discarded whenever
///     the same session also carried another operation. <c>SaveChangesAsync</c> reported success and
///     the documents committed, so a caller who queued SQL alongside a document write believed both
///     had landed.
///     <para>
///     The statement was not merely failing to report an error: it never ran at all. Weasel's
///     BatchBuilder creates the underlying batch command as a side effect of appending a PARAMETER,
///     so a parameterless statement left no current command and the next <c>StartNewCommand()</c>
///     cleared its text out of the shared buffer. That is why the parameterized cases below always
///     worked and the parameterless ones did not, and why the bug was invisible with the queued
///     command alone on the session.
///     </para>
/// </summary>
public class queued_sql_command_tests : OneOffConfigurationsContext
{
    private const string BadSql = "delete from a_table_that_does_not_exist_anywhere";

    // ---- the statement actually runs -------------------------------------------------------

    [Fact]
    public async Task parameterless_queued_sql_runs_when_it_is_the_only_operation()
    {
        await StartAsync();

        await using (var session = theStore.LightweightSession())
        {
            session.QueueSqlCommand($"insert into {ProbeTable} (note) values ('alone')");
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await ProbeCountAsync("alone")).ShouldBe(1);
    }

    /// <summary>The regression: this one silently did nothing.</summary>
    [Fact]
    public async Task parameterless_queued_sql_runs_alongside_a_document()
    {
        await StartAsync();

        await using (var session = theStore.LightweightSession())
        {
            session.QueueSqlCommand($"insert into {ProbeTable} (note) values ('with_doc')");
            session.Store(new Target { Id = Guid.NewGuid() });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await ProbeCountAsync("with_doc")).ShouldBe(1);
    }

    /// <summary>
    ///     Ordering matters to the underlying mechanism, so cover the queued command arriving after
    ///     the document as well as before it.
    /// </summary>
    [Fact]
    public async Task parameterless_queued_sql_runs_when_queued_after_a_document()
    {
        await StartAsync();

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Target { Id = Guid.NewGuid() });
            session.QueueSqlCommand($"insert into {ProbeTable} (note) values ('after_doc')");
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await ProbeCountAsync("after_doc")).ShouldBe(1);
    }

    [Fact]
    public async Task several_parameterless_queued_commands_all_run()
    {
        await StartAsync();

        await using (var session = theStore.LightweightSession())
        {
            session.QueueSqlCommand($"insert into {ProbeTable} (note) values ('multi')");
            session.QueueSqlCommand($"insert into {ProbeTable} (note) values ('multi')");
            session.QueueSqlCommand($"insert into {ProbeTable} (note) values ('multi')");
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await ProbeCountAsync("multi")).ShouldBe(3);
    }

    /// <summary>Parameterized statements always worked; kept so a fix cannot regress them.</summary>
    [Fact]
    public async Task parameterized_queued_sql_runs_alongside_a_document()
    {
        await StartAsync();

        await using (var session = theStore.LightweightSession())
        {
            session.QueueSqlCommand($"insert into {ProbeTable} (note) values (?)", "parameterized");
            session.Store(new Target { Id = Guid.NewGuid() });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await ProbeCountAsync("parameterized")).ShouldBe(1);
    }

    // ---- failures surface, and the batch is atomic -----------------------------------------

    [Fact]
    public async Task invalid_queued_sql_fails_the_commit_when_it_is_the_only_operation()
    {
        await StartAsync();

        await using var session = theStore.LightweightSession();
        session.QueueSqlCommand(BadSql);

        await Should.ThrowAsync<Exception>(async () =>
            await session.SaveChangesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task invalid_queued_sql_fails_the_commit_when_a_document_is_pending()
    {
        await StartAsync();

        await using var session = theStore.LightweightSession();
        session.QueueSqlCommand(BadSql);
        session.Store(new Target { Id = Guid.NewGuid() });

        await Should.ThrowAsync<Exception>(async () =>
            await session.SaveChangesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task the_document_is_not_persisted_when_the_queued_sql_is_invalid()
    {
        await StartAsync();

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.QueueSqlCommand(BadSql);
            session.Store(new Target { Id = id });
            try { await session.SaveChangesAsync(TestContext.Current.CancellationToken); }
            catch { /* expected */ }
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(id, TestContext.Current.CancellationToken))
            .ShouldBeNull("the batch contained an invalid statement, so nothing in it should have committed");
    }

    /// <summary>
    ///     Unparseable text, not just a missing table — the reporter noted both behave identically,
    ///     which ruled out deferred name resolution as the explanation.
    /// </summary>
    [Fact]
    public async Task unparseable_queued_sql_fails_the_commit_when_a_document_is_pending()
    {
        await StartAsync();

        await using var session = theStore.LightweightSession();
        session.QueueSqlCommand("this text cannot parse as sql on any engine");
        session.Store(new Target { Id = Guid.NewGuid() });

        await Should.ThrowAsync<Exception>(async () =>
            await session.SaveChangesAsync(TestContext.Current.CancellationToken));
    }

    // ---- harness ---------------------------------------------------------------------------

    private string ProbeTable => $"{theStore.Options.DatabaseSchemaName}.queued_sql_probe";

    private async Task StartAsync()
    {
        ConfigureStore(_ => { });
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        await using var conn = new SqlConnection(theStore.Options.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF OBJECT_ID('{ProbeTable}', 'U') IS NULL
                CREATE TABLE {ProbeTable} (id int identity(1,1) primary key, note nvarchar(50));
            DELETE FROM {ProbeTable};
            """;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<int> ProbeCountAsync(string note)
    {
        await using var conn = new SqlConnection(theStore.Options.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {ProbeTable} WHERE note = @n";
        cmd.Parameters.AddWithValue("@n", note);
        return (int)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }
}
