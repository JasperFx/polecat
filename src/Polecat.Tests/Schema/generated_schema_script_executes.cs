using System.Text.RegularExpressions;
using JasperFx;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Schema;

/// <summary>
///     The DDL Polecat generates has to <b>run</b> — against a real SQL Server, as one file, and a
///     second time without failing.
/// </summary>
/// <remarks>
///     <para>
///         This closes a gap the schema diagnostics tests leave open. Those assert that
///         <c>ToDatabaseScript</c> and <c>WriteCreationScriptToFileAsync</c> <em>contain</em> the table
///         names they should, which is a string search over generated text. Nothing executed the
///         result, so any defect the search does not notice ships looking green — and two did. Against
///         the hand-rolled renderer this replaces (#686), <c>Advanced.ToDatabaseScript()</c> emitted no
///         <c>CREATE SCHEMA</c>, so its first statement failed with "The specified schema name … either
///         does not exist"; and no <c>SET QUOTED_IDENTIFIER ON;</c>, which SQL Server requires before it
///         will add a <c>PERSISTED</c> computed column or a filtered index — both of which Polecat
///         emits. Every table name a string search looks for was present in that broken script.
///     </para>
///     <para>
///         Both entry points are covered because they are different code. The <c>db-dump</c> command is
///         what people run and what decides the database, the transactional flag and where the file
///         lands; <c>Advanced.ToDatabaseScript()</c> is the API a consumer calls in process. They render
///         the same text today, and that is worth pinning rather than assuming.
///     </para>
/// </remarks>
[Collection("integration")]
public class generated_schema_script_executes
{
    private const string Schema = "generated_script";

    /// <summary>
    ///     The <c>db-dump</c> command writes a file that builds the schema and can be run again.
    /// </summary>
    /// <remarks>
    ///     Driven through <c>RunJasperFxCommands</c> rather than by calling
    ///     <c>WriteCreationScriptToFileAsync</c>, because the command is the surface a developer uses.
    /// </remarks>
    [Fact]
    public async Task db_dump_writes_a_script_that_runs_twice()
    {
        await DropSchemaAsync();

        var path = Path.Combine(Path.GetTempPath(), $"polecat_db_dump_{Guid.NewGuid():N}.sql");

        try
        {
            var exitCode = await Host.CreateDefaultBuilder()
                .ConfigureServices(services => services.AddPolecat(Configure))
                .RunJasperFxCommands(["db-dump", path]);

            exitCode.ShouldBe(0);
            File.Exists(path).ShouldBeTrue();

            var script = await File.ReadAllTextAsync(path, TestContext.Current.CancellationToken);

            AssertIsRunnableScript(script);

            // First run, against an empty schema: this is a creation script.
            await ExecuteAsSqlcmdWouldAsync(script, TestContext.Current.CancellationToken);
            await AssertSchemaWasBuiltAsync();

            // Second run: every statement now describes something that already exists. One
            // unguarded object here fails, and a failure aborts every statement after it in the
            // same batch — so the schema is left half built while the script has already exited.
            await ExecuteAsSqlcmdWouldAsync(script, TestContext.Current.CancellationToken);
            await AssertSchemaWasBuiltAsync();
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    /// <summary>
    ///     The same claim for the in-process API, <c>Advanced.ToDatabaseScript()</c> and
    ///     <c>WriteCreationScriptToFileAsync</c> — which is where #686's two defects were.
    /// </summary>
    [Fact]
    public async Task to_database_script_runs_twice_and_builds_the_schema()
    {
        await DropSchemaAsync();

        await using var store = new DocumentStore(OptionsFor());

        // The providers have to be resolved before the script is rendered, or the document tables are
        // not in the feature schemas yet and the script is event-store-only. That is the documented
        // behaviour rather than a wrinkle of this test -- schema_diagnostics_tests does the same.
        store.Options.Providers.GetProvider<ScriptedCustomer>();
        store.Options.Providers.GetProvider<ScriptedOrder>();

        var script = store.Advanced.ToDatabaseScript();

        AssertIsRunnableScript(script);
        script.ShouldContain("pc_doc_scriptedcustomer");
        script.ShouldContain("pc_doc_scriptedorder");

        await ExecuteAsSqlcmdWouldAsync(script, TestContext.Current.CancellationToken);
        await AssertSchemaWasBuiltAsync();

        await ExecuteAsSqlcmdWouldAsync(script, TestContext.Current.CancellationToken);
        await AssertSchemaWasBuiltAsync();

        // The file overload is the same text on disk.
        var path = Path.Combine(Path.GetTempPath(), $"polecat_script_{Guid.NewGuid():N}.sql");
        try
        {
            await store.Advanced.WriteCreationScriptToFileAsync(path, TestContext.Current.CancellationToken);
            (await File.ReadAllTextAsync(path, TestContext.Current.CancellationToken)).ShouldBe(script);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    /// <summary>
    ///     A generated script carries no <c>GO</c>, so it also runs as a single command.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>This is a statement about Polecat, not about Weasel, and it is the assertion that
    ///         would catch a regression in either direction.</b> Weasel 9.35 emits <c>GO</c> in exactly
    ///         one place — bracketing a stored procedure body, which SQL Server requires to be the first
    ///         statement of its batch (weasel#593). Polecat registers no stored procedures (QuickAppend,
    ///         direct INSERTs), so none of its schema objects produce one and its scripts carry none.
    ///     </para>
    ///     <para>
    ///         The value of pinning it: <c>GO</c> is a sqlcmd directive, not T-SQL, so the moment a
    ///         script contains one it can no longer be handed to a <see cref="SqlCommand" /> — it fails
    ///         with "Incorrect syntax near 'GO'". The renderer this test's siblings replaced (#686) wrote
    ///         a <c>GO</c> after every object and had exactly that problem. If Polecat ever registers a
    ///         procedure, or hand-writes a separator again, this fact fails and the sibling tests above
    ///         keep passing — they split on <c>GO</c> the way a file is run, so they cannot see it.
    ///     </para>
    ///     <para>
    ///         Deliberately <em>not</em> asserted as "never contains GO" in the abstract: a script that
    ///         legitimately needed a batch break should get one. What is asserted is the consequence —
    ///         that the script as generated is executable in one command — so a future separator has to
    ///         be accompanied by a decision about this path rather than silently breaking it.
    ///     </para>
    /// </remarks>
    [Fact]
    public async Task a_generated_script_has_no_batch_separators_and_runs_as_one_command()
    {
        await DropSchemaAsync();

        await using var store = new DocumentStore(OptionsFor());
        store.Options.Providers.GetProvider<ScriptedCustomer>();

        var script = store.Advanced.ToDatabaseScript();

        BatchSeparator.IsMatch(script).ShouldBeFalse(
            "The generated script carries a GO line, so it can no longer be executed as a single "
            + "command. See the remarks: this is fine for a file and fatal for SqlCommand.");

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = script;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await AssertSchemaWasBuiltAsync();
    }

    /// <summary>
    ///     The two headers that decide whether a script can run at all.
    /// </summary>
    /// <remarks>
    ///     <c>SET QUOTED_IDENTIFIER ON;</c> is first because sqlcmd leaves the setting off and SQL
    ///     Server refuses a <c>PERSISTED</c> computed column or a filtered index while it is — and the
    ///     failure is quiet and cascading: the batch aborts at that statement, everything after it in
    ///     the batch is skipped, and sqlcmd still exits 0. <c>CREATE SCHEMA</c> is next because without
    ///     it the script cannot create anything in a database that does not already have the schema,
    ///     which is precisely the case a creation script exists for.
    /// </remarks>
    private static void AssertIsRunnableScript(string script)
    {
        script.Trim().ShouldStartWith("SET QUOTED_IDENTIFIER ON;");
        script.ShouldContain($"CREATE SCHEMA [{Schema}]");
        script.ShouldContain("pc_streams");
        script.ShouldContain("pc_events");
    }

    /// <summary>
    ///     The script <em>built</em> the schema, rather than merely running without error.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Asserted against the catalog rather than through
    ///         <c>AssertDatabaseMatchesConfigurationAsync</c>, which is the check this reaches for first
    ///         and the one that does not discriminate here: a store built from these options has nothing
    ///         to migrate whether the script ran or not, because Polecat applies document indexes and
    ///         foreign keys through <c>DocumentTableEnsurer</c> at first use rather than modelling them
    ///         as Weasel schema objects — so they are absent from the generated script and absent from
    ///         what the assert compares. See #684; the gap is real and is not what these tests are for.
    ///     </para>
    ///     <para>
    ///         So the tables are counted directly. Every table the script declares has to be there, on
    ///         both passes, which is what the second-run half is about: one unguarded object fails and
    ///         takes the rest of its batch with it, leaving a schema that is missing tables while the
    ///         script reported success.
    ///     </para>
    /// </remarks>
    private static async Task AssertSchemaWasBuiltAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT t.name FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = '{Schema}'
            ORDER BY t.name;
            """;

        var tables = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tables.Add(reader.GetString(0));
        }

        tables.ShouldContain("pc_streams");
        tables.ShouldContain("pc_events");
        tables.ShouldContain("pc_event_progression");
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
    ///     Run the script the way a file is run: one <c>GO</c> separated batch at a time.
    /// </summary>
    private static async Task ExecuteAsSqlcmdWouldAsync(string script, CancellationToken ct)
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

    private static StoreOptions OptionsFor()
    {
        var options = new StoreOptions();
        Configure(options);
        return options;
    }

    private static void Configure(StoreOptions opts)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

        // Indexes and a foreign key on purpose. They are NOT in the generated script today (#684), and
        // declaring them anyway is what makes these tests fail rather than change meaning on the day
        // that is fixed: the guards those statements need to survive a second run are exactly what the
        // run-twice halves above are here to hold.
        opts.Schema.For<ScriptedCustomer>()
            .UniqueIndex(x => x.Code);

        opts.Schema.For<ScriptedOrder>()
            .Index(x => x.Status)
            .ForeignKey<ScriptedCustomer>(x => x.CustomerId);
    }

    private static async Task DropSchemaAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();

        // Foreign keys first: a table cannot be dropped while something references it, and the event
        // store's own DDL creates one.
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

    public class ScriptedCustomer
    {
        public Guid Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }

    public class ScriptedOrder
    {
        public Guid Id { get; set; }
        public Guid CustomerId { get; set; }
        public string Status { get; set; } = string.Empty;
    }
}
