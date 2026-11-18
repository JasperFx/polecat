using JasperFx;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.Batching;

/// <summary>
///     polecat#676 — raw SQL enlisted in an <c>IBatchedQuery</c>. The point is the saved round trip, so
///     the first test asserts the round trip and not only the rows: an implementation that returned the
///     right values from a separate command would pass a rows-only test while doing nothing this issue
///     asked for.
/// </summary>
[Collection("integration")]
public class batch_raw_sql : IntegrationContext
{
    public batch_raw_sql(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private const string Schema = "batch_raw_sql";
    private static string UserTable => $"[{Schema}].[pc_doc_user]";

    private async Task<(Guid, Guid)> SeedTwoUsersAsync()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = Schema);
        await theStore.Advanced.CleanAllDocumentsAsync(TestContext.Current.CancellationToken);

        var alice = new User { Id = Guid.NewGuid(), FirstName = "Alice", LastName = "A" };
        var bob = new User { Id = Guid.NewGuid(), FirstName = "Bob", LastName = "B" };
        theSession.Store(alice, bob);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        return (alice.Id, bob.Id);
    }

    [Fact]
    public async Task raw_sql_shares_one_round_trip_with_a_load_and_a_linq_query()
    {
        var (aliceId, _) = await SeedTwoUsersAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var load = batch.Load<User>(aliceId);
        var linq = batch.Query<User>().Where(x => x.FirstName == "Bob").ToList();
        var raw = batch.Query<int>($"SELECT COUNT(*) FROM {UserTable}");

        await batch.Execute(TestContext.Current.CancellationToken);

        (await load).ShouldNotBeNull()!.FirstName.ShouldBe("Alice");
        (await linq).Single().FirstName.ShouldBe("Bob");
        (await raw).Single().ShouldBe(2);

        // The whole point: one command to the server for all three reads.
        query.RequestCount.ShouldBe(1);
    }

    [Fact]
    public async Task raw_sql_binds_its_parameters_in_order()
    {
        await SeedTwoUsersAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var names = batch.Query<string>(
            $"SELECT JSON_VALUE(data, '$.firstName') FROM {UserTable} WHERE JSON_VALUE(data, '$.firstName') = ? OR JSON_VALUE(data, '$.lastName') = ?",
            "Alice", "B");

        await batch.Execute(TestContext.Current.CancellationToken);

        (await names).OrderBy(x => x).ShouldBe(["Alice", "Bob"]);
    }

    [Fact]
    public async Task two_raw_sql_queries_in_one_batch_keep_their_own_parameters()
    {
        await SeedTwoUsersAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        // Both items would name their first parameter @p0 if the batch numbered parameters globally;
        // the builder names them per batch command, which is what keeps these apart.
        var first = batch.Query<string>(
            $"SELECT JSON_VALUE(data, '$.lastName') FROM {UserTable} WHERE JSON_VALUE(data, '$.firstName') = ?",
            "Alice");
        var second = batch.Query<string>(
            $"SELECT JSON_VALUE(data, '$.lastName') FROM {UserTable} WHERE JSON_VALUE(data, '$.firstName') = ?",
            "Bob");

        await batch.Execute(TestContext.Current.CancellationToken);

        (await first).Single().ShouldBe("A");
        (await second).Single().ShouldBe("B");
        query.RequestCount.ShouldBe(1);
    }

    [Fact]
    public async Task the_placeholder_overload_leaves_a_literal_question_mark_alone()
    {
        await SeedTwoUsersAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        // The reason the char overload exists: this SQL contains a '?' of its own, inside a string
        // literal, so '?' cannot be the parameter placeholder.
        const string sql = "SELECT 'who?' FROM {0} WHERE JSON_VALUE(data, '$.firstName') = {1}";

        var rows = batch.Query<string>('^', string.Format(sql, UserTable, "^"), "Alice");

        await batch.Execute(TestContext.Current.CancellationToken);

        (await rows).Single().ShouldBe("who?");

        // And the other half of the reason: the default overload would substitute the literal '?'
        // first, corrupting the SQL. Pinned so the two overloads cannot quietly converge.
        await using var second = theStore.QuerySession();
        var broken = second.CreateBatchQuery();
        broken.Query<string>(string.Format(sql, UserTable, "?"), "Alice");
        await Should.ThrowAsync<Exception>(() => broken.Execute(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task raw_sql_materializes_a_document_type()
    {
        var (aliceId, _) = await SeedTwoUsersAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        // Same scalar / document / JSON rules IAdvancedSql applies, because it is the same reader:
        // a document type needs id and data selected in order.
        var users = batch.Query<User>(
            $"SELECT id, data FROM {UserTable} WHERE id = ?", aliceId);

        await batch.Execute(TestContext.Current.CancellationToken);

        (await users).Single().FirstName.ShouldBe("Alice");
    }

    [Fact]
    public async Task too_few_placeholders_is_reported_by_the_query_call()
    {
        await SeedTwoUsersAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        // Raised where the mistake was made, not one Execute() later where it would look like a
        // failure of whichever item happened to be reading at the time.
        Should.Throw<InvalidOperationException>(() =>
            batch.Query<string>($"SELECT 1 FROM {UserTable} WHERE id = ?", Guid.NewGuid(), "extra"));
    }

    [Fact]
    public async Task raw_sql_in_a_batch_is_not_tenant_scoped()
    {
        // The documented asymmetry: every other member of IBatchedQuery scopes to the session's
        // tenant, and raw SQL does not, because the caller wrote the SQL and owns its filter.
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "batch_raw_sql_tenanted";
            opts.Schema.For<User>().MultiTenanted();
        });
        await theStore.Advanced.CleanAllDocumentsAsync(TestContext.Current.CancellationToken);

        await using (var red = theStore.LightweightSession(new SessionOptions { TenantId = "red" }))
        {
            red.Store(new User { Id = Guid.NewGuid(), FirstName = "Red" });
            await red.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var blue = theStore.LightweightSession(new SessionOptions { TenantId = "blue" }))
        {
            blue.Store(new User { Id = Guid.NewGuid(), FirstName = "Blue" });
            await blue.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession(new SessionOptions { TenantId = "red" });
        var batch = query.CreateBatchQuery();

        var scoped = batch.Query<User>().ToList();
        var unscoped = batch.Query<int>(
            "SELECT COUNT(*) FROM [batch_raw_sql_tenanted].[pc_doc_user]");

        await batch.Execute(TestContext.Current.CancellationToken);

        (await scoped).Single().FirstName.ShouldBe("Red");
        (await unscoped).Single().ShouldBe(2);
    }
}
