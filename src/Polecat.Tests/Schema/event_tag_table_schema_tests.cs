using System.Text.RegularExpressions;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Events;
using Polecat.Tests.Harness;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Tests.Schema;

/// <summary>
///     The tag table's <c>seq_id</c> index and what it costs an existing database (#841). Its primary
///     key leads on <c>value</c>, so without an index leading on <c>seq_id</c> every event delete scans
///     the tag table, once for the foreign key's cascade and again for anything that starts from a
///     stream. Each shape the table can take gets the same treatment, because each declares it apart.
/// </summary>
[Collection("tenant-partitioning")]
public class event_tag_table_schema_tests : IAsyncLifetime
{
    private const string Schema = "tag_schema";
    private const string TagTable = "pc_event_tag_schema_course";
    private const string IndexName = "ix_pc_event_tag_schema_course_seq_id";

    public enum Shape
    {
        Single,
        Conjoined,
        ArchivedPartitioning,
        ConjoinedArchivedPartitioning,
        TenantPartitioning
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await DropAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await DropAsync();
    }

    private static async Task DropAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await PartitionTestCleanup.DropArchivedPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    private static DocumentStore StoreFor(Shape shape) => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

        if (shape is Shape.Conjoined or Shape.ConjoinedArchivedPartitioning or Shape.TenantPartitioning)
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        }

        opts.EventGraph.UseArchivedStreamPartitioning =
            shape is Shape.ArchivedPartitioning or Shape.ConjoinedArchivedPartitioning;
        opts.EventGraph.UseTenantPartitionedEvents = shape == Shape.TenantPartitioning;

        opts.Events.RegisterTagType<CourseId>("schema_course");
    });

    private static async Task ExecuteAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static Task<List<object[]>> SeqIdIndexesAsync() => TestSchema.QueryAsync($"""
        SELECT i.name FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE i.object_id = OBJECT_ID('[{Schema}].[{TagTable}]')
          AND ic.key_ordinal = 1 AND c.name = 'seq_id'
        """);

    private static async Task<SchemaMigration> MigrationForTagTableAsync(DocumentStore store)
    {
        var table = store.Options.EventGraph.BuildEventTagTable(store.Options.EventGraph.TagTypes.Single());

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        return await SchemaMigration.DetermineAsync(conn, Ct, table);
    }

    [Theory]
    [InlineData(Shape.Single)]
    [InlineData(Shape.Conjoined)]
    [InlineData(Shape.ArchivedPartitioning)]
    [InlineData(Shape.ConjoinedArchivedPartitioning)]
    [InlineData(Shape.TenantPartitioning)]
    public async Task every_shape_has_an_index_leading_on_seq_id(Shape shape)
    {
        using var store = StoreFor(shape);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        (await SeqIdIndexesAsync()).Single()[0].ShouldBe(IndexName);
    }

    /// <summary>
    ///     An existing database is a table with rows and no index. It gets exactly the index, and
    ///     nothing that touches the table's rows or the constraints that a table of real data, full of
    ///     stale flags and orphans, might not satisfy.
    /// </summary>
    [Theory]
    [InlineData(Shape.Single)]
    [InlineData(Shape.Conjoined)]
    [InlineData(Shape.ArchivedPartitioning)]
    [InlineData(Shape.ConjoinedArchivedPartitioning)]
    public async Task an_existing_table_gains_the_index_and_nothing_else(Shape shape)
    {
        using var store = StoreFor(shape);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var session = store.LightweightSession())
        {
            var tagged = session.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
            tagged.WithTag(new CourseId(Guid.NewGuid()));
            session.Events.Append(Guid.NewGuid(), tagged);
            await session.SaveChangesAsync(Ct);
        }

        await ExecuteAsync($"DROP INDEX [{IndexName}] ON [{Schema}].[{TagTable}];");

        var migration = await MigrationForTagTableAsync(store);
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        var updates = new StringWriter();
        migration.WriteAllUpdates(updates, new SqlServerMigrator(), AutoCreate.CreateOrUpdate);

        var ddl = updates.ToString();
        ddl.ShouldContain(IndexName);
        Regex.Matches(ddl, @"\b(CREATE|ALTER|DROP)\b", RegexOptions.IgnoreCase).Count.ShouldBe(1,
            $"Expected a single CREATE INDEX, and the update was:\n{ddl}");
        ddl.ShouldContain("CREATE");

        (await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct))
            .ShouldBe(SchemaPatchDifference.Update);
        (await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct))
            .ShouldBe(SchemaPatchDifference.None);
    }

    [Theory]
    [InlineData(Shape.Single)]
    [InlineData(Shape.Conjoined)]
    [InlineData(Shape.ArchivedPartitioning)]
    [InlineData(Shape.ConjoinedArchivedPartitioning)]
    [InlineData(Shape.TenantPartitioning)]
    public async Task an_index_built_out_of_band_is_left_alone(Shape shape)
    {
        using var store = StoreFor(shape);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // Someone with a large table builds it ONLINE ahead of the upgrade, under the same name
        var column = shape == Shape.TenantPartitioning ? "seq_id, tenant_ordinal" : "seq_id";
        await ExecuteAsync($"""
            DROP INDEX [{IndexName}] ON [{Schema}].[{TagTable}];
            CREATE NONCLUSTERED INDEX [{IndexName}] ON [{Schema}].[{TagTable}] ({column}) WITH (ONLINE = ON);
            """);

        var migration = await MigrationForTagTableAsync(store);
        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     <c>generated_schema_script_executes</c> for the tag table: the script has to build each shape
    ///     from nothing, run a second time, and leave a database the model has nothing to say about.
    /// </summary>
    [Theory]
    [InlineData(Shape.Single)]
    [InlineData(Shape.Conjoined)]
    public async Task the_generated_script_builds_the_table_and_runs_twice(Shape shape)
    {
        using var store = StoreFor(shape);
        var script = store.Advanced.ToDatabaseScript();

        await ExecuteAsync(script);
        (await SeqIdIndexesAsync()).Single()[0].ShouldBe(IndexName);

        await ExecuteAsync(script);
        (await SeqIdIndexesAsync()).Single()[0].ShouldBe(IndexName);

        await AssertNothingLeftToApplyAsync(store);
    }

    /// <summary>
    ///     Once rather than twice, and that has nothing to do with tags: a partitioned table's generated
    ///     DDL starts with an unguarded <c>DROP PARTITION SCHEME</c>, which SQL Server refuses while the
    ///     table is on it, so the script of any partitioned <c>pc_events</c> fails its second run. That
    ///     is Weasel's to change. What the second run would prove, the second apply proves here.
    /// </summary>
    [Theory]
    [InlineData(Shape.ArchivedPartitioning)]
    [InlineData(Shape.ConjoinedArchivedPartitioning)]
    [InlineData(Shape.TenantPartitioning)]
    public async Task the_generated_script_builds_the_table_under_partitioning(Shape shape)
    {
        using var store = StoreFor(shape);

        await ExecuteAsync(store.Advanced.ToDatabaseScript());
        (await SeqIdIndexesAsync()).Single()[0].ShouldBe(IndexName);

        await AssertNothingLeftToApplyAsync(store);
    }

    private static async Task AssertNothingLeftToApplyAsync(DocumentStore store)
    {
        (await MigrationForTagTableAsync(store)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct))
            .ShouldBe(SchemaPatchDifference.None);
    }
}
