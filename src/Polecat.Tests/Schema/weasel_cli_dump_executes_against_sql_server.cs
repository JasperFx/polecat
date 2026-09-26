using JasperFx;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Text.RegularExpressions;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Schema;

/// <summary>
///     The SQL that Weasel's own command line writes for a Polecat store has to run against a real
///     SQL Server, as one file, and run a second time without failing.
/// </summary>
/// <remarks>
///     <para>
///     This closes a gap the existing schema diagnostics tests leave open: they assert that
///     <c>ToDatabaseScript</c> and <c>WriteCreationScriptToFileAsync</c> contain the table names they
///     should, which is a string assertion over generated text. Nothing executed the result, so any
///     defect in the rendering that a string search does not notice — a missing batch separator, a
///     statement that cannot run twice — would have shipped looking green.
///     </para>
///     <para>
///     The script is executed the way <c>sqlcmd</c> or SSMS executes a file, by splitting it on
///     <c>GO</c> lines (weasel#593). That is the point of the test rather than an implementation
///     detail: the file a developer is handed by <c>db-dump</c> is a file they will open in SSMS.
///     </para>
///     <para>
///     Driven through the real <c>db-dump</c> command rather than by calling
///     <c>WriteCreationScriptToFileAsync</c> directly, because the command is what people run, and it
///     is the command that decides which database, whether the script is transactional, and where the
///     file lands.
///     </para>
/// </remarks>
[Collection("integration")]
public class weasel_cli_dump_executes_against_sql_server
{
    private const string Schema = "weasel_cli_dump";

    [Fact]
    public async Task db_dump_writes_a_script_that_runs_twice()
    {
        await dropSchemaAsync();

        var path = Path.Combine(Path.GetTempPath(), $"polecat_db_dump_{Guid.NewGuid():N}.sql");

        try
        {
            var exitCode = await Host.CreateDefaultBuilder()
                .ConfigureServices(services => services.AddPolecat(configure))
                .RunJasperFxCommands(["db-dump", path]);

            exitCode.ShouldBe(0);
            File.Exists(path).ShouldBeTrue();

            var script = await File.ReadAllTextAsync(path, TestContext.Current.CancellationToken);

            // The header sqlcmd needs and does not set for itself. Without it SQL Server refuses to
            // create a filtered index or an index on a computed column, the batch aborts, and sqlcmd
            // still exits 0 -- so its absence is invisible right up until the schema is wrong.
            script.Trim().ShouldStartWith("SET QUOTED_IDENTIFIER ON;");

            script.ShouldContain("pc_streams");
            script.ShouldContain("pc_events");

            // First run: against the empty schema this is a creation script
            await executeAsSqlcmdWouldAsync(script, TestContext.Current.CancellationToken);

            await using (var store = new DocumentStore(optionsFor()))
            {
                // Nothing left to migrate: the script really did build the configured schema, rather
                // than merely running without error
                await store.Database.AssertDatabaseMatchesConfigurationAsync();
            }

            // Second run: every statement in it now describes something that already exists. This is
            // the half that used to fail, on the first unguarded object, and one failure aborts every
            // statement after it in the same batch.
            await executeAsSqlcmdWouldAsync(script, TestContext.Current.CancellationToken);

            await using (var store = new DocumentStore(optionsFor()))
            {
                await store.Database.AssertDatabaseMatchesConfigurationAsync();
            }
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    private static StoreOptions optionsFor()
    {
        var options = new StoreOptions();
        configure(options);
        return options;
    }

    private static void configure(StoreOptions opts)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

        // Indexes and a foreign key on purpose: those are the statements that carry the existence
        // guards a second run depends on, and a table-only script would not exercise any of them.
        opts.Schema.For<DumpedCustomer>()
            .UniqueIndex(x => x.Code);

        opts.Schema.For<DumpedOrder>()
            .Index(x => x.Status)
            .ForeignKey<DumpedCustomer>(x => x.CustomerId);
    }

    /// <summary>
    ///     A line whose entire content is <c>GO</c> ends the batch — sqlcmd's rule, spelled out here
    ///     rather than borrowed from <c>Weasel.SqlServer.SqlServerBatchSplitter</c> on purpose.
    ///     Splitting the output with the same code that produced it would make the test agree with
    ///     Weasel about what a batch is instead of agreeing with SQL Server.
    /// </summary>
    private static readonly Regex BatchSeparator =
        new(@"^[ \t]*GO[ \t]*\r?$", RegexOptions.Multiline | RegexOptions.IgnoreCase);

    /// <summary>
    ///     Run the script the way a file is run, one <c>GO</c> separated batch at a time. Handing the
    ///     whole text to one command is what fails with "Incorrect syntax near 'GO'".
    /// </summary>
    private static async Task executeAsSqlcmdWouldAsync(string script, CancellationToken ct)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(ct);

        foreach (var batch in BatchSeparator.Split(script))
        {
            if (string.IsNullOrWhiteSpace(batch)) continue;

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = batch;
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task dropSchemaAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();

        // Foreign keys first: a table cannot be dropped while something references it, and the
        // script under test creates one.
        cmd.CommandText = $"""
            IF SCHEMA_ID('{Schema}') IS NOT NULL
            BEGIN
                DECLARE @sql NVARCHAR(MAX) = '';

                SELECT @sql += 'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
                             + ' DROP CONSTRAINT ' + QUOTENAME(f.name) + ';' + CHAR(13)
                FROM sys.foreign_keys f
                JOIN sys.tables t ON t.object_id = f.parent_object_id
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE s.name = '{Schema}';

                SELECT @sql += 'DROP TABLE IF EXISTS ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';' + CHAR(13)
                FROM sys.tables t
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE s.name = '{Schema}';

                SELECT @sql += 'DROP SEQUENCE ' + QUOTENAME(s.name) + '.' + QUOTENAME(q.name) + ';' + CHAR(13)
                FROM sys.sequences q
                JOIN sys.schemas s ON s.schema_id = q.schema_id
                WHERE s.name = '{Schema}';

                EXEC sp_executesql @sql;
            END
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    public class DumpedCustomer
    {
        public Guid Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }

    public class DumpedOrder
    {
        public Guid Id { get; set; }
        public Guid CustomerId { get; set; }
        public string Status { get; set; } = string.Empty;
    }
}
