using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Polecat.Tests.Harness;
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
        script.ShouldContain("cc_code AS (JSON_VALUE(data, '$.code' RETURNING varchar(250))) PERSISTED");
        script.ShouldContain("cc_status AS (JSON_VALUE(data, '$.status' RETURNING varchar(250))) PERSISTED");
        script.ShouldContain("cc_customerid AS (CONVERT(uniqueidentifier, JSON_VALUE(data, '$.customerId'))) PERSISTED");

        // ...the indexes over them, each guarded so the script runs twice...
        script.ShouldContain("CREATE UNIQUE INDEX ux_pc_doc_declaredcustomer_code");
        script.ShouldContain("CREATE INDEX ix_pc_doc_declaredorder_status");
        script.ShouldContain("CREATE INDEX ix_declared_order_placed_desc");

        // ...and the foreign key, which Weasel defers to the end of the script because the table it
        // references is created by the same script.
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
    }

    public class DeclaredOrder
    {
        public Guid Id { get; set; }
        public Guid CustomerId { get; set; }
        public string Status { get; set; } = string.Empty;
        public DateTimeOffset Placed { get; set; }
    }
}
