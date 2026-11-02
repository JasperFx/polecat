using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections.Flattened;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Tests.Projections;

public record FlatRowOpened(string Name, decimal Amount);

public record FlatRowClosed;

public class FlatRowNeighbor
{
    public Guid Id { get; set; }
}

/// <summary>
///     Takes its schema from the test, so that nothing here shares <c>dbo</c> with another test.
/// </summary>
public class FlatRowProjection : FlatTableProjection
{
    public const string TableName = "flat_rows";

    public FlatRowProjection(string schema) : base(TableName, schema)
    {
        Table.AddColumn("id", "uniqueidentifier").AsPrimaryKey();

        Project<FlatRowOpened>(map =>
        {
            map.Map(x => x.Name, "name");
            map.Map(x => x.Amount, "amount");
        });

        Delete<FlatRowClosed>();
    }
}

/// <summary>
///     Every column shape a flat table can get, whether a mapping inferred it or the projection declared
///     it, plus a declared index. This is what has to come out of the catalog looking like its declaration.
/// </summary>
public class WideFlatRowProjection : FlatTableProjection
{
    public record Happened(
        Guid GuidValue,
        int IntValue,
        long LongValue,
        short ShortValue,
        decimal DecimalValue,
        double DoubleValue,
        float FloatValue,
        bool BoolValue,
        string StringValue,
        DateTime DateValue,
        DateTimeOffset OffsetValue,
        byte[] BytesValue,
        int? NullableValue);

    public WideFlatRowProjection(string schema) : base("flat_wide_rows", schema)
    {
        Table.AddColumn("Id", "uniqueidentifier").AsPrimaryKey();
        Table.AddColumn("code", "varchar(50)").NotNull().AddIndex();
        Table.AddColumn("counted", "int").NotNull().DefaultValue(0);
        Table.AddColumn("scale", "decimal(10,2)");

        // The form docs/events/projections/flat.md shows, which leaves the database type to Weasel
        Table.AddColumn<Guid>("typed_guid");
        Table.AddColumn<string>("typed_string");
        Table.AddColumn<decimal>("typed_decimal");
        Table.AddColumn<DateTimeOffset>("typed_offset");
        Table.AddColumn<bool>("typed_bool");
        Table.AddColumn<long>("typed_long");

        Project<Happened>(map =>
        {
            map.Map(x => x.GuidValue);
            map.Map(x => x.IntValue);
            map.Map(x => x.LongValue);
            map.Map(x => x.ShortValue);
            map.Map(x => x.DecimalValue);
            map.Map(x => x.DoubleValue);
            map.Map(x => x.FloatValue);
            map.Map(x => x.BoolValue);
            map.Map(x => x.StringValue);
            map.Map(x => x.DateValue);
            map.Map(x => x.OffsetValue);
            map.Map(x => x.BytesValue);
            map.Map(x => x.NullableValue);
        });
    }
}

/// <summary>
///     #882: a flat table was created by its projection the first time an event reached it, with its own
///     bool to remember that by, so it answered to neither <c>AutoCreate</c>, the migration lock, nor the
///     idea that a store can have more than one database. It is declared by the event store feature now,
///     so all three come with it, along with the generated script and the database assertion.
/// </summary>
public class flat_table_schema_tests
{
    private static string NewSchema() => "fts_" + Guid.NewGuid().ToString("N")[..10];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DocumentStore BuildStore(string schema, ProjectionLifecycle lifecycle,
        AutoCreate autoCreate = AutoCreate.All, Action<StoreOptions>? configure = null) => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = schema;
        opts.AutoCreateSchemaObjects = autoCreate;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Projections.Add(new FlatRowProjection(schema), lifecycle);
        configure?.Invoke(opts);
    });

    /// <summary>
    ///     The event store with no flat table in it, which is what an existing store has on the day it
    ///     registers one, and what a store that manages its own schema has until somebody adds it.
    /// </summary>
    private static async Task ProvisionEventStoreOnlyAsync(string schema, string? connectionString = null)
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = connectionString ?? ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private static async Task<bool> TableExistsAsync(string schema, string table,
        string? connectionString = null)
    {
        await using var conn = new SqlConnection(connectionString ?? ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT OBJECT_ID('[{schema}].[{table}]', 'U')";
        return await cmd.ExecuteScalarAsync(Ct) is not (null or DBNull);
    }

    private static async Task ExecuteAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<int> RowCountAsync(string schema, string table = FlatRowProjection.TableName,
        string? connectionString = null)
    {
        await using var conn = new SqlConnection(connectionString ?? ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM [{schema}].[{table}];";
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private static async Task OpenRowAsync(DocumentStore store, Guid? id = null, string? tenantId = null)
    {
        await using var session = tenantId == null
            ? store.LightweightSession()
            : store.LightweightSession(tenantId);
        session.Events.StartStream(id ?? Guid.NewGuid(), new FlatRowOpened("x", 1m));
        await session.SaveChangesAsync(Ct);
    }

    /// <summary>
    ///     What <c>FlatTableProjection.EnsureTableAsync</c> used to run, verbatim.
    /// </summary>
    private static async Task CreateTheWayTheOldPathDidAsync(Table table)
    {
        var writer = new StringWriter();
        table.WriteCreateStatement(new SqlServerMigrator(), writer);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var createSchema = conn.CreateCommand();
        createSchema.CommandText =
            $"IF SCHEMA_ID('{table.Identifier.Schema}') IS NULL EXEC('CREATE SCHEMA [{table.Identifier.Schema}]');";
        await createSchema.ExecuteNonQueryAsync(Ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = writer.ToString();
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<SchemaPatchDifference> DifferenceOfAsync(Table table)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        var migration = await SchemaMigration.DetermineAsync(conn, Ct, table);
        return migration.Difference;
    }

    // -------------------------------------------------------------------------------------
    // AutoCreate
    // -------------------------------------------------------------------------------------

    /// <summary>
    ///     #267 closed this leak for documents. Under <c>AutoCreate.None</c> a missing table is a clean
    ///     "invalid object" failure, because the connection may have no right to create one at all.
    /// </summary>
    [Fact]
    public async Task an_inline_save_under_auto_create_none_does_not_create_the_table()
    {
        var schema = NewSchema();
        await ProvisionEventStoreOnlyAsync(schema);

        using var store = BuildStore(schema, ProjectionLifecycle.Inline, AutoCreate.None);

        var failure = await Should.ThrowAsync<SqlException>(() => OpenRowAsync(store));
        failure.Number.ShouldBe(208);

        (await TableExistsAsync(schema, FlatRowProjection.TableName)).ShouldBeFalse();
    }

    [Fact]
    public async Task a_daemon_batch_under_auto_create_none_does_not_create_the_table()
    {
        var schema = NewSchema();
        await ProvisionEventStoreOnlyAsync(schema);

        var projection = new FlatRowProjection(schema);
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.None;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Projections.Add(projection, ProjectionLifecycle.Async);
        });

        await using var session = store.LightweightSession();
        IJasperFxProjection<IDocumentSession> asDaemonSeesIt = projection;
        await asDaemonSeesIt.ApplyAsync(session, [], Ct);

        (await TableExistsAsync(schema, FlatRowProjection.TableName)).ShouldBeFalse();
    }

    // -------------------------------------------------------------------------------------
    // The event store's own ensure, with the lock that comes with it
    // -------------------------------------------------------------------------------------

    [Fact]
    public async Task the_daemons_ensure_builds_the_table()
    {
        var schema = NewSchema();
        using var store = BuildStore(schema, ProjectionLifecycle.Async);

        await store.Database.EnsureStorageExistsAsync(typeof(IEvent), Ct);

        (await TableExistsAsync(schema, FlatRowProjection.TableName)).ShouldBeTrue();
    }

    [Fact]
    public async Task a_sessions_first_use_of_the_event_store_builds_the_table()
    {
        var schema = NewSchema();
        using var store = BuildStore(schema, ProjectionLifecycle.Inline);

        await using var session = store.QuerySession();
        await session.Events.FetchStreamStateAsync(Guid.NewGuid(), Ct);

        (await TableExistsAsync(schema, FlatRowProjection.TableName)).ShouldBeTrue();
    }

    [Fact]
    public async Task a_startup_apply_builds_the_table_on_a_store_that_already_had_the_event_store()
    {
        var schema = NewSchema();
        await ProvisionEventStoreOnlyAsync(schema);

        using var store = BuildStore(schema, ProjectionLifecycle.Inline);

        (await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct)).ShouldBe(SchemaPatchDifference.Create);

        (await TableExistsAsync(schema, FlatRowProjection.TableName)).ShouldBeTrue();
    }

    /// <summary>
    ///     The inline ensure ran on the session's connection, inside the save's transaction, and
    ///     remembered the table as soon as the statement returned. So a first save that failed after that
    ///     point rolled the table away and left the projection sure it existed: every save after that
    ///     failed on a table nothing would ever create again.
    /// </summary>
    [Fact]
    public async Task a_first_save_that_fails_does_not_cost_the_store_its_table()
    {
        var schema = NewSchema();
        await ProvisionEventStoreOnlyAsync(schema);

        using var store = BuildStore(schema, ProjectionLifecycle.Inline,
            configure: opts => opts.Schema.For<FlatRowNeighbor>());

        var taken = new FlatRowNeighbor { Id = Guid.NewGuid() };
        await using (var session = store.LightweightSession())
        {
            session.Insert(taken);
            await session.SaveChangesAsync(Ct);
        }

        // The duplicate fails when the unit of work flushes, by which time the inline projection has run
        await Should.ThrowAsync<Exception>(async () =>
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new FlatRowOpened("x", 1m));
            session.Insert(new FlatRowNeighbor { Id = taken.Id });
            await session.SaveChangesAsync(Ct);
        });

        await OpenRowAsync(store);

        (await RowCountAsync(schema)).ShouldBe(1);
    }

    // -------------------------------------------------------------------------------------
    // One table per database, for as long as the database has it
    // -------------------------------------------------------------------------------------

    /// <summary>
    ///     The projection is one object, and it remembered one bool. So the first tenant's database got
    ///     the table, and the second tenant's first event failed against a table nothing would ever make.
    /// </summary>
    [Fact]
    public async Task every_tenant_database_gets_the_table()
    {
        var first = ConnectionSource.Scoped("fts_first");
        var second = ConnectionSource.Scoped("fts_second");

        await CreateDatabaseAsync(first);
        await CreateDatabaseAsync(second);

        try
        {
            var schema = NewSchema();
            using var store = BuildStore(schema, ProjectionLifecycle.Inline, configure: opts =>
            {
                opts.ConnectionString = ConnectionSource.ConnectionStringFor(first);
                opts.Events.TenancyStyle = TenancyStyle.Single;
                opts.MultiTenantedDatabases(tenancy =>
                {
                    tenancy.AddTenant("first", ConnectionSource.ConnectionStringFor(first));
                    tenancy.AddTenant("second", ConnectionSource.ConnectionStringFor(second));
                });
            });

            await OpenRowAsync(store, tenantId: "first");
            await OpenRowAsync(store, tenantId: "second");

            (await RowCountAsync(schema, connectionString: ConnectionSource.ConnectionStringFor(first))).ShouldBe(1);
            (await RowCountAsync(schema, connectionString: ConnectionSource.ConnectionStringFor(second))).ShouldBe(1);
        }
        finally
        {
            await DropDatabaseAsync(first);
            await DropDatabaseAsync(second);
        }
    }

    private static async Task CreateDatabaseAsync(string database)
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"IF DB_ID('{database}') IS NULL CREATE DATABASE [{database}];";
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task DropDatabaseAsync(string database)
    {
        SqlConnection.ClearAllPools();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync(CancellationToken.None);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"IF DB_ID('{database}') IS NOT NULL BEGIN "
            + $"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
            + $"DROP DATABASE [{database}]; END";
        await cmd.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task an_inline_store_gets_everything_back_after_completely_remove_all()
    {
        var schema = NewSchema();
        using var store = BuildStore(schema, ProjectionLifecycle.Inline);

        await OpenRowAsync(store);

        await store.Advanced.CompletelyRemoveAllAsync(Ct);
        (await TableExistsAsync(schema, FlatRowProjection.TableName)).ShouldBeFalse();

        await OpenRowAsync(store);

        (await RowCountAsync(schema)).ShouldBe(1);
    }

    [Fact]
    public async Task a_daemon_gets_the_table_back_after_completely_remove_all()
    {
        var schema = NewSchema();
        using var store = BuildStore(schema, ProjectionLifecycle.Async);

        await OpenRowAsync(store);
        await store.WaitForProjectionAsync();

        await store.Advanced.CompletelyRemoveAllAsync(Ct);

        await OpenRowAsync(store);
        await store.WaitForProjectionAsync();

        (await RowCountAsync(schema)).ShouldBe(1);
    }

    // -------------------------------------------------------------------------------------
    // The model
    // -------------------------------------------------------------------------------------

    [Fact]
    public async Task the_generated_script_builds_the_table_and_runs_twice()
    {
        var schema = NewSchema();
        using var store = BuildStore(schema, ProjectionLifecycle.Inline);

        var script = store.Advanced.ToDatabaseScript();

        await ExecuteAsync(script);
        (await TableExistsAsync(schema, FlatRowProjection.TableName)).ShouldBeTrue();

        await ExecuteAsync(script);
        (await TableExistsAsync(schema, FlatRowProjection.TableName)).ShouldBeTrue();

        await store.Database.AssertDatabaseMatchesConfigurationAsync(Ct);
    }

    [Fact]
    public async Task the_database_assertion_notices_a_missing_table()
    {
        var schema = NewSchema();
        await ProvisionEventStoreOnlyAsync(schema);

        using var store = BuildStore(schema, ProjectionLifecycle.Inline);

        var failure = await Should.ThrowAsync<DatabaseValidationException>(
            () => store.Database.AssertDatabaseMatchesConfigurationAsync(Ct));

        failure.Message.ShouldContain(FlatRowProjection.TableName);
    }

    /// <summary>
    ///     A composite member is not in <c>Projections.All</c>, only the composite is. The old ensure did not
    ///     care, because it ran from inside the member, so a declaration that walked only the top level
    ///     would have left this table with nothing to create it.
    /// </summary>
    [Fact]
    public async Task a_composite_members_table_is_declared()
    {
        var schema = NewSchema();
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Projections.CompositeProjectionFor("flat", composite => composite.Add(new FlatRowProjection(schema)));
        });

        store.Advanced.ToDatabaseScript().ShouldContain(FlatRowProjection.TableName);

        await OpenRowAsync(store);
        await store.WaitForProjectionAsync();

        (await RowCountAsync(schema)).ShouldBe(1);

        await store.Advanced.CompletelyRemoveAllAsync(Ct);
        (await TableExistsAsync(schema, FlatRowProjection.TableName)).ShouldBeFalse();
    }

    // -------------------------------------------------------------------------------------
    // Upgrading: every table the old path ever created is about to be diffed for the first time
    // -------------------------------------------------------------------------------------

    [Fact]
    public async Task a_table_the_old_path_created_needs_no_migration()
    {
        var schema = NewSchema();

        var narrow = new FlatRowProjection(schema).Table;
        await CreateTheWayTheOldPathDidAsync(narrow);
        (await DifferenceOfAsync(narrow)).ShouldBe(SchemaPatchDifference.None);

        var wide = new WideFlatRowProjection(schema).Table;
        await CreateTheWayTheOldPathDidAsync(wide);
        (await DifferenceOfAsync(wide)).ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Column names were lower-cased before #49, so a table from then disagrees with its declaration
    ///     about case, and nothing about SQL Server's identifiers cares.
    /// </summary>
    [Fact]
    public async Task a_table_whose_column_names_differ_only_by_case_needs_no_migration()
    {
        var schema = NewSchema();

        var declared = new AdditiveTable(new SqlServerObjectName(schema, "flat_cased"));
        declared.AddColumn("id", "uniqueidentifier").AsPrimaryKey();
        declared.AddColumn("Amount", "decimal(18,4)");

        var lowerCased = new Table(new SqlServerObjectName(schema, "flat_cased"));
        lowerCased.AddColumn("id", "uniqueidentifier").AsPrimaryKey();
        lowerCased.AddColumn("amount", "decimal(18,4)");

        await CreateTheWayTheOldPathDidAsync(lowerCased);

        (await DifferenceOfAsync(declared)).ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Nothing ever compared a flat table to its declaration, so a real one has had years to pick up an
    ///     index for a report, or to keep a column whose mapping has since gone. A <c>Table</c>'s delta drops
    ///     whatever it does not declare, which on these tables would have thrown data away on the first
    ///     start after upgrading.
    /// </summary>
    [Fact]
    public async Task columns_and_indexes_nobody_declared_are_left_alone()
    {
        var schema = NewSchema();
        using var store = BuildStore(schema, ProjectionLifecycle.Inline);

        var table = new FlatRowProjection(schema).Table;
        await CreateTheWayTheOldPathDidAsync(table);

        await ExecuteAsync($"""
            ALTER TABLE [{schema}].[{FlatRowProjection.TableName}] ADD legacy_note nvarchar(100) NULL;
            CREATE INDEX ix_report_by_name ON [{schema}].[{FlatRowProjection.TableName}] (amount);
            """);

        (await DifferenceOfAsync(table)).ShouldBe(SchemaPatchDifference.None);

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT (SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('[{schema}].[{FlatRowProjection.TableName}]') AND name = 'legacy_note')
                 + (SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('[{schema}].[{FlatRowProjection.TableName}]') AND name = 'ix_report_by_name');
            """;
        (await cmd.ExecuteScalarAsync(Ct)).ShouldBe(2);
    }

    /// <summary>
    ///     The other half of being diffed: a mapping added to a projection reaches a table that already
    ///     exists, where it used to fail at the first event that needed it.
    /// </summary>
    [Fact]
    public async Task a_column_the_projection_declares_later_is_added_to_the_table()
    {
        var schema = NewSchema();

        var before = new Table(new SqlServerObjectName(schema, FlatRowProjection.TableName));
        before.AddColumn("id", "uniqueidentifier").AsPrimaryKey();
        before.AddColumn("name", "nvarchar(max)");
        await CreateTheWayTheOldPathDidAsync(before);

        using var store = BuildStore(schema, ProjectionLifecycle.Inline);

        await OpenRowAsync(store);

        (await RowCountAsync(schema)).ShouldBe(1);
    }
}
