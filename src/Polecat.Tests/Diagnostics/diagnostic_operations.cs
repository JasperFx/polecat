using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Diagnostics;

[Collection("integration")]
public class diagnostic_operations : IntegrationContext
{
    public diagnostic_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task can_clean_specific_document_type()
    {
        // Store some documents
        theSession.Store(new Target { Id = Guid.NewGuid(), Color = "CleanMe" });
        theSession.Store(new Target { Id = Guid.NewGuid(), Color = "CleanMe" });
        await theSession.SaveChangesAsync();

        // Verify they exist
        await using var queryBefore = theStore.QuerySession();
        var before = await queryBefore.Query<Target>()
            .Where(t => t.Color == "CleanMe")
            .CountAsync();
        before.ShouldBeGreaterThanOrEqualTo(2);

        // Clean all Targets
        await theStore.Advanced.CleanAsync<Target>();

        // Verify they're gone
        await using var queryAfter = theStore.QuerySession();
        var after = await queryAfter.Query<Target>().CountAsync();
        after.ShouldBe(0);
    }

    [Fact]
    public async Task can_clean_all_documents()
    {
        theSession.Store(new User { Id = Guid.NewGuid(), FirstName = "CleanAll" });
        theSession.Store(new Target { Id = Guid.NewGuid(), Color = "CleanAll" });
        await theSession.SaveChangesAsync();

        await theStore.Advanced.CleanAllDocumentsAsync();

        await using var query = theStore.QuerySession();
        var users = await query.Query<User>().CountAsync();
        var targets = await query.Query<Target>().CountAsync();

        users.ShouldBe(0);
        targets.ShouldBe(0);
    }

    [Fact]
    public async Task can_clean_all_event_data()
    {
        theSession.Events.StartStream(Guid.NewGuid(), new QuestStarted { Name = "Clean" });
        await theSession.SaveChangesAsync();

        await theStore.Advanced.CleanAllEventDataAsync();

        await using var query = theStore.QuerySession();
        var stream = await query.Events.FetchStreamAsync(Guid.NewGuid());
        stream.ShouldBeEmpty();
    }

    [Fact]
    public async Task clean_all_event_data_is_safe_when_tables_do_not_exist()
    {
        // Repro for a bootstrap pain point reported in #42: calling
        // CleanAllEventDataAsync() against a fresh database (no pc_events /
        // pc_streams / pc_event_progression yet) should be a no-op rather
        // than blowing up with "Invalid object name".
        var schema = $"clean_missing_{Guid.NewGuid():N}".Substring(0, 24);

        // Create an empty schema but do NOT migrate any tables into it.
        await using (var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA [{schema}]');";
            await cmd.ExecuteNonQueryAsync();
        }

        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            AutoCreateSchemaObjects = JasperFx.AutoCreate.None,
            DatabaseSchemaName = schema,
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        };

        await using var pristineStore = new DocumentStore(options);

        // Should not throw — every DELETE is now guarded by IF OBJECT_ID(...) IS NOT NULL.
        await Should.NotThrowAsync(() => pristineStore.Advanced.CleanAllEventDataAsync());
    }

    [Fact]
    public async Task to_sql_returns_expected_query()
    {
        await using var query = theStore.QuerySession();
        var queryable = query.Query<User>().Where(u => u.FirstName == "Alice");

        var sql = query.ToSql(queryable);

        sql.ShouldContain("SELECT");
        sql.ShouldContain("pc_doc_user");
        sql.ShouldContain("tenant_id");
    }

    [Fact]
    public async Task to_sql_includes_where_clause()
    {
        await using var query = theStore.QuerySession();
        var queryable = query.Query<Target>().Where(t => t.Number > 5);

        var sql = query.ToSql(queryable);

        sql.ShouldContain("SELECT");
        sql.ShouldContain("pc_doc_target");
    }

    [Fact]
    public async Task to_sql_with_ordering()
    {
        await using var query = theStore.QuerySession();
        var queryable = query.Query<User>().OrderBy(u => u.LastName).Take(10);

        var sql = query.ToSql(queryable);

        sql.ShouldContain("ORDER BY");
        sql.ShouldContain("10");
    }
}

public class QuestStarted
{
    public string Name { get; set; } = string.Empty;
}
