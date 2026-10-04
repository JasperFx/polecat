using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.Core;
using Weasel.SqlServer;
using Shouldly;

namespace Polecat.Tests.Storage;

/// <summary>
///     #684 — the computed columns, secondary indexes and foreign keys a document type declares are
///     Weasel schema objects, so every consumer of the schema-object model agrees with the database.
/// </summary>
/// <remarks>
///     <para>
///         Three consumers, and before this they all disagreed: the generated creation script omitted
///         these objects, <c>AssertDatabaseMatchesConfigurationAsync()</c> reported a match without
///         them, and <c>AutoCreate.None</c> could not refuse what it could not see. They were raw DDL
///         run by <c>DocumentTableEnsurer</c> and deliberately stripped out of the diff.
///     </para>
///     <para>
///         The <b>no-churn</b> fact is the one that matters most and the one most likely to break. A
///         modeled computed column is compared by its canonicalized expression, so if Polecat's
///         rendering and SQL Server's stored definition do not canonicalize to the same text, the diff
///         is never empty: every storage-ensure re-runs DDL, forever, and nothing else here notices.
///     </para>
/// </remarks>
public class declared_objects_are_modeled_tests : OneOffConfigurationsContext
{
    private void ConfigureWithDeclarations()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<DeclaredCustomer>()
                .UniqueIndex(x => x.Code);

            opts.Schema.For<DeclaredOrder>()
                .Index(x => x.Status)
                .Index(x => x.Placed, i =>
                {
                    i.IndexName = "ix_declared_order_placed_desc";
                    i.SortOrder = Polecat.Storage.SortOrder.Descending;
                })
                .ForeignKey<DeclaredCustomer>(x => x.CustomerId);
        });
    }

    [Fact]
    public async Task the_declarations_reach_the_generated_script()
    {
        ConfigureWithDeclarations();

        theStore.Options.Providers.GetProvider<DeclaredCustomer>();
        theStore.Options.Providers.GetProvider<DeclaredOrder>();

        var script = theStore.Advanced.ToDatabaseScript();

        // The computed columns the index and the foreign key sit on, rendered inline in the CREATE
        // TABLE. Unbracketed because Weasel leaves an ordinary identifier alone.
        //
        // ⚠️ The EXPRESSION is asked of the product rather than written out, because it depends on the
        // store's json mode: on native `json` most types use JSON_VALUE(... RETURNING t), and without it
        // (Azure SQL Edge, which the `edge` CI lane runs) every type falls back to CONVERT(t, ...).
        // Spelling the native form here made this pass locally and fail on that lane. Asking
        // ComputedColumnExpression keeps the assertion about "the declaration reached the script" --
        // which is what the test is for -- rather than about which branch the renderer took.
        var useReturning = ConnectionSource.SupportsNativeJson;

        script.ShouldContain(
            $"cc_code AS ({DocumentIndex.ComputedColumnExpression("$.code", "varchar(250)", IndexCasing.Default, useReturning)}) PERSISTED");
        script.ShouldContain(
            $"cc_status AS ({DocumentIndex.ComputedColumnExpression("$.status", "varchar(250)", IndexCasing.Default, useReturning)}) PERSISTED");

        // uniqueidentifier has no RETURNING support, so this one is CONVERT in either mode -- and that
        // is the column the foreign key sits on, which is why it is spelled out.
        script.ShouldContain(
            $"cc_customerid AS ({DocumentIndex.ComputedColumnExpression("$.customerId", "uniqueidentifier", IndexCasing.Default, useReturning)}) PERSISTED");
        script.ShouldContain("cc_customerid AS (CONVERT(uniqueidentifier, JSON_VALUE(data, '$.customerId'))) PERSISTED");

        // ...the indexes over them, each guarded so the script runs twice...
        script.ShouldContain("CREATE UNIQUE INDEX ux_pc_doc_declaredcustomer_code");
        script.ShouldContain("CREATE INDEX ix_pc_doc_declaredorder_status");
        script.ShouldContain("CREATE INDEX ix_declared_order_placed_desc");

        // ...and the foreign key. It is emitted right after its own table rather than deferred to the
        // end -- a creation script renders each object's CREATE in the order the feature schema yields
        // them, with no migration involved and so none of SchemaMigration's deferral. What makes it run
        // is that DocumentFeatureSchema yields the REFERENCED table first; see InDependencyOrder there.
        // Before that ordering existed this script was luck: provider order is dictionary order.
        script.ShouldContain("ADD CONSTRAINT fk_pc_doc_declaredorder_cc_customerid FOREIGN KEY(cc_customerid)");
        script.ShouldContain("REFERENCES");

        await Task.CompletedTask;
    }

    /// <summary>
    ///     Applying the schema leaves nothing to migrate — the fact that catches a canonicalization
    ///     mismatch between what Polecat renders and what SQL Server stores.
    /// </summary>
    /// <remarks>
    ///     <c>AssertDatabaseMatchesConfigurationAsync</c> is the assertion, and it only became capable
    ///     of failing for these objects when they entered the model. Run twice: the second call is what
    ///     proves the first apply reached a fixed point rather than merely succeeding.
    /// </remarks>
    [Fact]
    public async Task applying_the_schema_reaches_a_fixed_point()
    {
        ConfigureWithDeclarations();

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();

        // No second round of DDL: a computed column whose rendering does not canonicalize to the
        // catalog's own text would report a difference here every single time.
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();
    }

    /// <summary>
    ///     The objects are really in the database after a plain write, and they are the ones declared.
    /// </summary>
    /// <remarks>
    ///     Through a session rather than through a migration, because <c>DocumentTableEnsurer</c> is the
    ///     path an application actually takes — and it is the path whose raw-DDL loops this replaced.
    /// </remarks>
    [Fact]
    public async Task a_write_creates_the_declared_objects()
    {
        ConfigureWithDeclarations();

        var customerId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new DeclaredCustomer { Id = customerId, Code = "ACME" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new DeclaredOrder
            {
                Id = Guid.NewGuid(), CustomerId = customerId, Status = "open", Placed = DateTimeOffset.UtcNow
            });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await IndexNamesAsync("pc_doc_declaredcustomer")).ShouldContain("ux_pc_doc_declaredcustomer_code");
        (await IndexNamesAsync("pc_doc_declaredorder")).ShouldContain("ix_pc_doc_declaredorder_status");
        (await ForeignKeyNamesAsync("pc_doc_declaredorder"))
            .ShouldContain("fk_pc_doc_declaredorder_cc_customerid");

        // The unique index has teeth, which is the only assertion here that the index is the declared
        // one rather than an index that happens to carry the name.
        await using var duplicate = theStore.LightweightSession();
        duplicate.Store(new DeclaredCustomer { Id = Guid.NewGuid(), Code = "ACME" });
        await Should.ThrowAsync<Exception>(() => duplicate.SaveChangesAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>
    ///     Writing the foreign key's OWNING document first, with no table yet for the type it
    ///     references, still works.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>This is the fact that catches the ordering trap, and nothing else here does.</b>
    ///         Every other test in this file reaches the referenced table first — by storing a customer
    ///         before an order, or by going through the whole-database migration, where Weasel has every
    ///         table in one migration and defers the constraint itself. Take the incremental path with
    ///         the order first and the constraint has nowhere to point.
    ///     </para>
    ///     <para>
    ///         The trap: <c>SchemaMigration</c> only defers a foreign key whose linked table is created
    ///         <em>later in the same migration</em>, and Weasel's migrator then executes the deferred set
    ///         as the last command of the same <c>ApplyAllAsync</c> call. So "defer it" does not mean
    ///         "skip it" — it reorders within the call. <c>DocumentTableEnsurer</c> therefore builds its
    ///         first-pass table with the constraints left out of the model entirely, and adds them in a
    ///         second pass once the referenced tables exist.
    ///     </para>
    /// </remarks>
    [Fact]
    public async Task the_referencing_document_can_be_written_before_the_referenced_table_exists()
    {
        ConfigureWithDeclarations();

        // Deliberately no customer first, and no migration: the very first thing that touches the
        // database is a write of the document that OWNS the foreign key.
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new DeclaredOrder
            {
                Id = Guid.NewGuid(),
                CustomerId = Guid.NewGuid(),
                Status = "open",
                Placed = DateTimeOffset.UtcNow
            });

            // A foreign key to a customer that does not exist would be a constraint violation, so the
            // store itself has to fail -- but on the CONSTRAINT, not on a missing table. Either way the
            // schema must have been built, which is what the assertions below check.
            try
            {
                await session.SaveChangesAsync(TestContext.Current.CancellationToken);
            }
            catch (Exception e) when (e.ToString().Contains("FOREIGN KEY", StringComparison.OrdinalIgnoreCase)
                                      || e.ToString().Contains("conflicted", StringComparison.OrdinalIgnoreCase))
            {
                // Expected: the row is genuinely orphaned. The schema is what is under test.
            }
        }

        // Both tables exist, and the constraint was created -- which is the whole point: the referenced
        // table was provisioned on demand and the constraint followed it rather than preceding it.
        (await ForeignKeyNamesAsync("pc_doc_declaredorder"))
            .ShouldContain("fk_pc_doc_declaredorder_cc_customerid");
        (await IndexNamesAsync("pc_doc_declaredcustomer")).ShouldContain("ux_pc_doc_declaredcustomer_code");

        // Deliberately NOT AssertDatabaseMatchesConfigurationAsync: nothing here wrote an event, so the
        // event store's own tables were never provisioned and a whole-database assertion would fail on
        // pc_streams -- a true statement about the database and the wrong question for this test.
        //
        // The repeatable claim instead: a second write over the same path adds nothing and the
        // constraint is still there exactly once. A second pass that re-created it would fail outright,
        // and one that dropped it would show up here.
        var customerId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new DeclaredCustomer { Id = customerId, Code = "LATE" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new DeclaredOrder
            {
                Id = Guid.NewGuid(), CustomerId = customerId, Status = "open", Placed = DateTimeOffset.UtcNow
            });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await ForeignKeyNamesAsync("pc_doc_declaredorder"))
            .Count(x => x == "fk_pc_doc_declaredorder_cc_customerid").ShouldBe(1);
    }

    /// <summary>
    ///     A descending index keeps its direction, per key column.
    /// </summary>
    /// <remarks>
    ///     Polecat's <c>SortOrder</c> has always meant "every key path descending", while Weasel's own
    ///     <c>SortOrder</c> means a single trailing <c>DESC</c>. The mapping therefore goes through
    ///     <c>DescendingColumns</c>, and this asserts the catalog agrees — a silent fall back to the
    ///     coarse form would flip a multi-column index's leading columns to ascending.
    /// </remarks>
    [Fact]
    public async Task a_descending_index_keeps_its_direction()
    {
        ConfigureWithDeclarations();

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        await using var conn = new SqlConnection(TestUtils.ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT ic.is_descending_key
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            WHERE i.name = 'ix_declared_order_placed_desc'
              AND i.object_id = OBJECT_ID('{theStore.Options.DatabaseSchemaName}.pc_doc_declaredorder')
              AND ic.is_included_column = 0;
            """;

        var descending = new List<bool>();
        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            descending.Add(reader.GetBoolean(0));
        }

        descending.ShouldNotBeEmpty("The descending index was not created at all.");
        descending.ShouldAllBe(x => x);
    }

    /// <summary>
    ///     An index or foreign key Polecat does not declare is still left alone — the #267 guarantee,
    ///     which #684 narrowed without weakening.
    /// </summary>
    /// <remarks>
    ///     This is the fact that would catch "model Polecat's objects" turning into "drop everything
    ///     else". A user's index, or a column an EF migration added, is not in the model and must
    ///     survive a migration; only the objects Polecat itself declares reconcile.
    /// </remarks>
    [Fact]
    public async Task an_undeclared_index_survives_a_migration()
    {
        ConfigureWithDeclarations();

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var table = $"{theStore.Options.DatabaseSchemaName}.pc_doc_declaredcustomer";

        await using (var conn = new SqlConnection(TestUtils.ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                SET QUOTED_IDENTIFIER ON;
                IF COL_LENGTH('{table}', 'user_added') IS NULL
                    ALTER TABLE {table} ADD [user_added] AS (CAST(JSON_VALUE(data, '$.code') AS varchar(50))) PERSISTED;
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_user_added' AND object_id = OBJECT_ID('{table}'))
                    CREATE INDEX [ix_user_added] ON {table} ([user_added]);
                """;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        (await IndexNamesAsync("pc_doc_declaredcustomer")).ShouldContain("ix_user_added");
        (await IndexNamesAsync("pc_doc_declaredcustomer")).ShouldContain("ux_pc_doc_declaredcustomer_code");
    }

    /// <summary>
    ///     <see cref="AutoCreate.None" /> refuses a missing declared index, which it could not do while
    ///     the index was invisible to the model.
    /// </summary>
    [Fact]
    public async Task auto_create_none_refuses_a_missing_declared_index()
    {
        // Build the tables WITHOUT the index declaration, so the index is the only thing missing.
        ConfigureStore(opts => { });
        theStore.Options.Providers.GetProvider<DeclaredCustomer>();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Now declare it, under AutoCreate.None.
        ConfigureStore(opts =>
        {
            opts.AutoCreateSchemaObjects = AutoCreate.None;
            opts.Schema.For<DeclaredCustomer>().UniqueIndex(x => x.Code);
        });

        await Should.ThrowAsync<Exception>(
            () => theStore.Database.AssertDatabaseMatchesConfigurationAsync());
    }

    // ── #685: the JSON index, formerly the last raw DDL on the document path ─────────

    private void ConfigureWithJsonIndex(AutoCreate? autoCreate = null)
    {
        ConfigureStore(opts =>
        {
            if (autoCreate.HasValue) opts.AutoCreateSchemaObjects = autoCreate.Value;
            opts.Schema.For<DeclaredCustomer>()
                .JsonIndex(x => x.Code, i => i.IndexName = "jidx_declared_customer");
        });
    }

    /// <summary>
    ///     #685 — the JSON index reaches the generated script. While it was raw DDL rendered at first
    ///     use, <c>ToDatabaseScript()</c> and <c>db-dump</c> omitted it, so the script did not reproduce
    ///     the configured schema.
    /// </summary>
    [Fact]
    public async Task a_json_index_reaches_the_generated_script()
    {
        if (!TestUtils.ConnectionSource.SupportsNativeJson) return;

        ConfigureWithJsonIndex();
        theStore.Options.Providers.GetProvider<DeclaredCustomer>();

        var script = theStore.Advanced.ToDatabaseScript();

        script.ShouldContain("CREATE JSON INDEX");
        script.ShouldContain("jidx_declared_customer");
    }

    /// <summary>
    ///     The fixed point: applying then asserting must report a match, twice. This is the trap #684
    ///     hit through weasel#637 — a declaration that cannot canonicalize against the catalog reports
    ///     drift on every pass and then tries to drop and re-add.
    /// </summary>
    [Fact]
    public async Task applying_a_json_index_reaches_a_fixed_point()
    {
        if (!TestUtils.ConnectionSource.SupportsNativeJson) return;

        ConfigureWithJsonIndex();

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();

        (await IndexNamesAsync("pc_doc_declaredcustomer")).ShouldContain("jidx_declared_customer");
    }

    /// <summary>
    ///     <see cref="AutoCreate.None" /> refuses a missing JSON index. It could not while the index was
    ///     invisible to the model — and that invisibility is what made the gap hard to notice, because
    ///     the natural "did this work?" assertion passed over a database missing it.
    /// </summary>
    [Fact]
    public async Task auto_create_none_refuses_a_missing_json_index()
    {
        if (!TestUtils.ConnectionSource.SupportsNativeJson) return;

        // Build the table WITHOUT the declaration, so the JSON index is the only thing missing.
        ConfigureStore(opts => { });
        theStore.Options.Providers.GetProvider<DeclaredCustomer>();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        ConfigureWithJsonIndex(AutoCreate.None);

        await Should.ThrowAsync<Exception>(
            () => theStore.Database.AssertDatabaseMatchesConfigurationAsync());
    }

    /// <summary>
    ///     A JSON index Polecat does NOT declare survives a migration. Modeling the declared ones must
    ///     not turn the migration into "reconcile everything" — and this one is worth its own fact
    ///     because a JSON index is the shape Weasel used to read as a zero-column phantom and drop
    ///     (weasel#661).
    /// </summary>
    [Fact]
    public async Task an_undeclared_json_index_survives_a_migration()
    {
        if (!TestUtils.ConnectionSource.SupportsNativeJson) return;

        ConfigureStore(opts => { });
        theStore.Options.Providers.GetProvider<DeclaredCustomer>();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var table = $"{theStore.Options.DatabaseSchemaName}.pc_doc_declaredcustomer";

        await using (var conn = new SqlConnection(TestUtils.ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                SET QUOTED_IDENTIFIER ON;
                IF NOT EXISTS (SELECT 1 FROM sys.json_indexes WHERE object_id = OBJECT_ID('{table}'))
                    CREATE JSON INDEX [jidx_user_added] ON {table} (data);
                """;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        (await IndexNamesAsync("pc_doc_declaredcustomer")).ShouldContain("jidx_user_added");
    }

    /// <summary>
    ///     Two JSON indexes on one table is refused with a message that says why, rather than one of
    ///     them being silently dropped from the model — SQL Server allows only one per json column, so
    ///     the second would fail at the database anyway, much further from the declaration.
    /// </summary>
    [Fact]
    public void two_json_indexes_on_one_type_are_refused()
    {
        if (!TestUtils.ConnectionSource.SupportsNativeJson) return;

        ConfigureStore(opts =>
        {
            opts.Schema.For<DeclaredCustomer>()
                .JsonIndex(x => x.Code, i => i.IndexName = "jidx_one")
                .JsonIndex(x => x.Id, i => i.IndexName = "jidx_two");
        });

        var ex = Should.Throw<InvalidOperationException>(() =>
        {
            theStore.Options.Providers.GetProvider<DeclaredCustomer>();
            theStore.Advanced.ToDatabaseScript();
        });

        ex.Message.ShouldContain("only one JSON index");
    }

    // ── #685: the full-text index, the last raw DDL on the document path ────────────

    private void ConfigureWithFullText(AutoCreate? autoCreate = null)
    {
        ConfigureStore(opts =>
        {
            if (autoCreate.HasValue) opts.AutoCreateSchemaObjects = autoCreate.Value;
            opts.Schema.For<DeclaredCustomer>().FullTextIndex(x => x.Code);
        });
    }

    private const string FtTable = "pc_ft_declaredcustomer";
    private const string FtTrigger = "tr_pc_ft_declaredcustomer";

    /// <summary>
    ///     #685 — the token table, its index and the maintaining trigger all reach the generated
    ///     script. While they were raw DDL rendered at first use, <c>ToDatabaseScript()</c> and
    ///     <c>db-dump</c> omitted every one of them, so the script did not reproduce the configured
    ///     schema: a database built from it would accept writes and answer every full-text search
    ///     empty.
    /// </summary>
    [Fact]
    public void the_full_text_objects_reach_the_generated_script()
    {
        ConfigureWithFullText();
        theStore.Options.Providers.GetProvider<DeclaredCustomer>();

        var script = theStore.Advanced.ToDatabaseScript();

        script.ShouldContain($"CREATE TABLE {theStore.Options.DatabaseSchemaName}.{FtTable}");
        script.ShouldContain($"CREATE INDEX ix_{FtTable}_term");
        script.ShouldContain($"CREATE TRIGGER {theStore.Options.DatabaseSchemaName}.{FtTrigger}");

        // The trigger has to come AFTER both tables it touches: a creation script renders each CREATE
        // in the order the feature schema yields them, with no migration involved and therefore none of
        // SchemaMigration's deferral. Asserted by position rather than by presence, because all three
        // being present in the wrong order is a script that will not run.
        script.IndexOf($"CREATE TABLE {theStore.Options.DatabaseSchemaName}.{FtTable}", StringComparison.Ordinal)
            .ShouldBeLessThan(script.IndexOf($"CREATE TRIGGER {theStore.Options.DatabaseSchemaName}.{FtTrigger}",
                StringComparison.Ordinal));
    }

    /// <summary>
    ///     The fixed point. For the trigger this is a body comparison against
    ///     <c>sys.sql_modules</c>, so it is the fact that catches a rendering SQL Server stores
    ///     differently from the way Polecat declares it — that reports drift on every pass, and then
    ///     drops and re-creates the trigger on every storage-ensure forever.
    /// </summary>
    [Fact]
    public async Task applying_the_full_text_objects_reaches_a_fixed_point()
    {
        ConfigureWithFullText();

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();

        (await IndexNamesAsync(FtTable)).ShouldContain($"ix_{FtTable}_term");
        (await TriggerNamesAsync("pc_doc_declaredcustomer")).ShouldContain(FtTrigger);

        // And the objects the migration built actually WORK, which is a different claim from their
        // existing under the right names. The write below is served by the trigger this migration
        // created -- the fixed point above is what establishes that, since a storage-ensure with
        // nothing to do cannot have replaced it.
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new DeclaredCustomer { Id = Guid.NewGuid(), Code = "quick brown fox" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await NamesAsync($"""
            SELECT term FROM {theStore.Options.DatabaseSchemaName}.{FtTable} ORDER BY pos;
            """)).ShouldBe(["quick", "brown", "fox"]);
    }

    /// <summary>
    ///     <see cref="AutoCreate.None" /> refuses a missing token table. It could not while the table
    ///     was invisible to the model — and that invisibility is what made the gap hard to notice,
    ///     because the natural "did this work?" assertion passed over a database that had none of it.
    /// </summary>
    [Fact]
    public async Task auto_create_none_refuses_a_missing_full_text_table()
    {
        // Build the document table WITHOUT the declaration, so the full-text objects are the only
        // things missing.
        ConfigureStore(opts => { });
        theStore.Options.Providers.GetProvider<DeclaredCustomer>();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        ConfigureWithFullText(AutoCreate.None);

        await Should.ThrowAsync<Exception>(
            () => theStore.Database.AssertDatabaseMatchesConfigurationAsync());
    }

    /// <summary>
    ///     A trigger Polecat does not declare survives a migration — the #267 guarantee, restated for
    ///     the object kind this issue added.
    /// </summary>
    /// <remarks>
    ///     Worth its own fact rather than resting on the index one. A trigger is an independent schema
    ///     object that merely names its target (weasel#452), not something the table owns, so nothing
    ///     about the table's own delta protects it: what protects it is that the migration only visits
    ///     objects in the model. A reconcile-everything reading would silently delete a user's
    ///     data-integrity logic.
    /// </remarks>
    [Fact]
    public async Task an_undeclared_trigger_survives_a_migration()
    {
        ConfigureWithFullText();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var schema = theStore.Options.DatabaseSchemaName;
        var table = $"{schema}.pc_doc_declaredcustomer";

        await using (var conn = new SqlConnection(TestUtils.ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF OBJECT_ID('{schema}.tr_user_added', 'TR') IS NULL
                    EXEC sp_executesql N'CREATE TRIGGER {schema}.tr_user_added ON {table} AFTER INSERT AS BEGIN SET NOCOUNT ON; END';
                """;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var triggers = await TriggerNamesAsync("pc_doc_declaredcustomer");
        triggers.ShouldContain("tr_user_added");
        triggers.ShouldContain(FtTrigger);
    }

    /// <summary>
    ///     Declaring a second member re-renders the trigger rather than leaving the first member's
    ///     body in place.
    /// </summary>
    /// <remarks>
    ///     This is the behaviour the old renderer bought with <c>CREATE OR ALTER</c>, and it has to
    ///     survive the move to a modeled object, where it is instead bought by the delta reporting
    ///     <c>Update</c> on a changed body. If it did not, the new member would be covered by the
    ///     backfill (which is unconditional) and then never maintained again — searchable until its
    ///     document is next written, and silently not afterwards.
    /// </remarks>
    [Fact]
    public async Task redeclaring_the_index_re_renders_the_trigger()
    {
        ConfigureWithFullText();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        (await TriggerDefinitionAsync(FtTrigger)).ShouldNotContain("$.notes");

        // Same type, a second declared member. The token table is unchanged; only the trigger body is.
        ConfigureStore(opts => opts.Schema.For<DeclaredCustomer>()
            .FullTextIndex(x => x.Code)
            .FullTextIndex(x => x.Notes));

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();

        var definition = await TriggerDefinitionAsync(FtTrigger);
        definition.ShouldContain("$.code");
        definition.ShouldContain("$.notes");
    }

    private async Task<List<string>> TriggerNamesAsync(string tableName)
        => await NamesAsync($"""
            SELECT t.name FROM sys.triggers t
            WHERE t.parent_id = OBJECT_ID('{theStore.Options.DatabaseSchemaName}.{tableName}');
            """);

    private async Task<string> TriggerDefinitionAsync(string triggerName)
        => (await NamesAsync($"""
            SELECT sm.definition FROM sys.sql_modules sm
            WHERE sm.object_id = OBJECT_ID('{theStore.Options.DatabaseSchemaName}.{triggerName}');
            """)).Single();

    private async Task<List<string>> IndexNamesAsync(string tableName)
        => await NamesAsync($"""
            SELECT i.name FROM sys.indexes i
            WHERE i.object_id = OBJECT_ID('{theStore.Options.DatabaseSchemaName}.{tableName}')
              AND i.name IS NOT NULL;
            """);

    private async Task<List<string>> ForeignKeyNamesAsync(string tableName)
        => await NamesAsync($"""
            SELECT f.name FROM sys.foreign_keys f
            WHERE f.parent_object_id = OBJECT_ID('{theStore.Options.DatabaseSchemaName}.{tableName}');
            """);

    private static async Task<List<string>> NamesAsync(string sql)
    {
        await using var conn = new SqlConnection(TestUtils.ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    public class DeclaredCustomer
    {
        public Guid Id { get; set; }
        public string Code { get; set; } = string.Empty;

        /// <summary>A second text member, so a full-text declaration can gain one (#685).</summary>
        public string Notes { get; set; } = string.Empty;
    }

    public class DeclaredOrder
    {
        public Guid Id { get; set; }
        public Guid CustomerId { get; set; }
        public string Status { get; set; } = string.Empty;
        public DateTimeOffset Placed { get; set; }
    }
}
