using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Internal.Operations;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     polecat#436 (the twin of marten#5179): the three progression-delete paths that take a
///     projection or subscription NAME — rewind, delete-progress and rebuild teardown — used to run
///     <c>DELETE ... WHERE name LIKE @name</c> with <c>@name</c> bound to <c>name + "%"</c>. That is
///     wrong twice over, and each half silently destroys a *different* projection's progression state:
///     <list type="number">
///         <item>
///             <description>
///                 <c>_</c>, <c>%</c> and <c>[</c> are all legal in a projection name and all three
///                 are T-SQL <c>LIKE</c> metacharacters, so <c>day_summary</c> matched
///                 <c>dayXsummary</c> and a bracketed name did not even match itself.
///             </description>
///         </item>
///         <item>
///             <description>
///                 A plain prefix sweep on <c>day_summary</c> also took <c>day_summary_v2</c>'s rows,
///                 with no wildcard involved at all.
///             </description>
///         </item>
///     </list>
///     Every case below fails on the old predicate and passes on the exact-identity /
///     <c>':'</c>-anchored-and-escaped one.
/// </summary>
[Collection("integration")]
public class progression_delete_scoping_tests : IntegrationContext
{
    public progression_delete_scoping_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private IEventStore<IDocumentSession, IQuerySession> theEventStore => theStore;

    /// <summary>
    ///     A store with no projection registered under the names being deleted, which is the path that
    ///     falls back on the anchored pattern — and therefore the one where the escaping has to hold.
    /// </summary>
    private async Task WithBareStoreAsync(string schema)
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
        });

        await ClearProgressionAsync();
    }

    [Fact]
    public async Task delete_progress_leaves_a_sibling_sharing_the_name_prefix_alone()
    {
        await WithBareStoreAsync("progdel_sibling");

        var victim = new ShardName("day_summary");        // day_summary:All
        var sibling = new ShardName("day_summary_v2");    // day_summary_v2:All

        await SeedAsync(victim, 10);
        await SeedAsync(sibling, 20);

        await theEventStore.DeleteProjectionProgressAsync(theStore.Database, "day_summary",
            TestContext.Current.CancellationToken);

        await AssertProgressAsync(victim, 0);
        await AssertProgressAsync(sibling, 20);
    }

    [Fact]
    public async Task the_underscore_in_a_name_is_not_treated_as_a_wildcard()
    {
        await WithBareStoreAsync("progdel_underscore");

        var victim = new ShardName("day_summary");     // day_summary:All
        var bystander = new ShardName("dayXsummary");  // dayXsummary:All — matched by an unescaped '_'

        await SeedAsync(victim, 10);
        await SeedAsync(bystander, 20);

        await theEventStore.DeleteProjectionProgressAsync(theStore.Database, "day_summary",
            TestContext.Current.CancellationToken);

        await AssertProgressAsync(victim, 0);
        await AssertProgressAsync(bystander, 20);
    }

    [Fact]
    public async Task a_bracket_in_a_name_still_matches_itself()
    {
        await WithBareStoreAsync("progdel_bracket");

        // The other direction of the same defect: '[' opens a character class, so the unescaped
        // pattern "day[s]ummary%" matches "daysummary..." and NOT the rows it was aimed at. The
        // projection's own progression therefore survived its own teardown.
        var victim = new ShardName("day[s]ummary");
        var bystander = new ShardName("daysummary");

        await SeedAsync(victim, 10);
        await SeedAsync(bystander, 20);

        await theEventStore.DeleteProjectionProgressAsync(theStore.Database, "day[s]ummary",
            TestContext.Current.CancellationToken);

        await AssertProgressAsync(victim, 0);
        await AssertProgressAsync(bystander, 20);
    }

    [Fact]
    public async Task a_percent_in_a_name_is_not_treated_as_a_wildcard()
    {
        await WithBareStoreAsync("progdel_percent");

        var victim = new ShardName("day%summary");
        var bystander = new ShardName("dayANYTHINGsummary");

        await SeedAsync(victim, 10);
        await SeedAsync(bystander, 20);

        await theEventStore.DeleteProjectionProgressAsync(theStore.Database, "day%summary",
            TestContext.Current.CancellationToken);

        await AssertProgressAsync(victim, 0);
        await AssertProgressAsync(bystander, 20);
    }

    [Fact]
    public async Task per_tenant_rows_of_the_named_projection_are_still_deleted()
    {
        await WithBareStoreAsync("progdel_tenants");

        // The anchored pattern exists precisely for these: the tenant suffix is not enumerable up
        // front, so exact identities alone would leave every per-tenant row behind.
        var shard = new ShardName("day_summary");
        var acme = shard.ForTenant("acme");        // day_summary:All:acme
        var globex = shard.ForTenant("globex");    // day_summary:All:globex
        var sibling = new ShardName("day_summary_v2").ForTenant("acme");

        await SeedAsync(shard, 5);
        await SeedAsync(acme, 6);
        await SeedAsync(globex, 7);
        await SeedAsync(sibling, 8);

        await theEventStore.DeleteProjectionProgressAsync(theStore.Database, "day_summary",
            TestContext.Current.CancellationToken);

        await AssertProgressAsync(shard, 0);
        await AssertProgressAsync(acme, 0);
        await AssertProgressAsync(globex, 0);
        await AssertProgressAsync(sibling, 8);
    }

    [Fact]
    public async Task rewind_to_the_floor_leaves_a_prefix_sibling_alone()
    {
        await WithBareStoreAsync("progdel_rewind");

        var victim = new ShardName("day_summary");
        var sibling = new ShardName("day_summary_v2");

        await SeedAsync(victim, 10);
        await SeedAsync(sibling, 20);

        // A null/zero floor is the branch that DELETEs rather than re-stamping.
        await theEventStore.RewindSubscriptionProgressAsync(theStore.Database, "day_summary",
            TestContext.Current.CancellationToken, null);

        await AssertProgressAsync(victim, 0);
        await AssertProgressAsync(sibling, 20);
    }

    [Fact]
    public async Task rebuild_teardown_leaves_a_prefix_sibling_alone()
    {
        await WithBareStoreAsync("progdel_teardown");

        var victim = new ShardName("day_summary");
        var sibling = new ShardName("day_summary_v2");

        await SeedAsync(victim, 10);
        await SeedAsync(sibling, 20);

        await theEventStore.TeardownExistingProjectionStateAsync(theStore.Database, "day_summary",
            TestContext.Current.CancellationToken);

        await AssertProgressAsync(victim, 0);
        await AssertProgressAsync(sibling, 20);
    }

    [Fact]
    public async Task a_registered_projection_is_deleted_by_its_exact_shard_identities()
    {
        // Two independently registered async projections whose names share a prefix, which is the
        // shape the issue reports: tearing "day_summary" down must not touch "day_summary_v2".
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "progdel_registered";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Projections.Add(new DaySummaryProjection(), ProjectionLifecycle.Async);
            opts.Projections.Add(new DaySummaryV2Projection(), ProjectionLifecycle.Async);
        });

        await ClearProgressionAsync();

        var victim = theStore.Options.Projections.AllShards()
            .Single(x => x.Name.Name == "day_summary").Name;
        var sibling = theStore.Options.Projections.AllShards()
            .Single(x => x.Name.Name == "day_summary_v2").Name;

        await SeedAsync(victim, 10);
        await SeedAsync(victim.ForTenant("acme"), 11);
        await SeedAsync(sibling, 20);
        await SeedAsync(sibling.ForTenant("acme"), 21);

        await theEventStore.DeleteProjectionProgressAsync(theStore.Database, "day_summary",
            TestContext.Current.CancellationToken);

        await AssertProgressAsync(victim, 0);
        await AssertProgressAsync(victim.ForTenant("acme"), 0);
        await AssertProgressAsync(sibling, 20);
        await AssertProgressAsync(sibling.ForTenant("acme"), 21);
    }

    [Fact]
    public async Task an_unknown_name_deletes_nothing_and_does_not_throw()
    {
        await WithBareStoreAsync("progdel_unknown");

        var keep = new ShardName("day_summary");
        await SeedAsync(keep, 10);

        // Teardown and rewind are both run against projections that were removed from the
        // configuration since they last wrote progress, so an unknown name has to stay a no-op.
        await Should.NotThrowAsync(theEventStore.DeleteProjectionProgressAsync(theStore.Database,
            "never_registered", TestContext.Current.CancellationToken));

        await AssertProgressAsync(keep, 10);
    }

    private async Task ClearProgressionAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM {theStore.Events.ProgressionTableName};";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task AssertProgressAsync(ShardName shard, long expected)
    {
        var actual = await theStore.Database.ProjectionProgressFor(shard, TestContext.Current.CancellationToken);
        actual.ShouldBe(expected, $"progression for '{shard.Identity}'");
    }

    private async Task SeedAsync(ShardName shardName, long ceiling)
    {
        // Seed through the production write path (polecat#323), same as
        // delete_projection_progress_by_shard_name_tests.
        var events = theStore.Database.Events;
        var op = new RecordProgressionOperation(
            events.ProgressionTableName,
            shardName.Identity,
            ceiling,
            events.EnableExtendedProgressionTracking,
            upsert: true);

        await using var conn = await OpenConnectionAsync();
        await using var batch = new Microsoft.Data.SqlClient.SqlBatch(conn);
        var builder = new Weasel.SqlServer.BatchBuilder(batch);
        op.ConfigureCommand(builder);
        builder.Compile();
        await using var reader = await batch.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        await op.PostprocessAsync(reader, new List<Exception>(), TestContext.Current.CancellationToken);
    }
}

public class DaySummary
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public class DaySummaryV2
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class DaySummaryProjection : SingleStreamProjection<DaySummary, Guid>
{
    public DaySummaryProjection()
    {
        Name = "day_summary";
    }

    public void Apply(DaySummaryCounted e, DaySummary view) => view.Count++;
}

public partial class DaySummaryV2Projection : SingleStreamProjection<DaySummaryV2, Guid>
{
    public DaySummaryV2Projection()
    {
        Name = "day_summary_v2";
    }

    public void Apply(DaySummaryCounted e, DaySummaryV2 view) => view.Count++;
}

public record DaySummaryCounted(int Amount);
