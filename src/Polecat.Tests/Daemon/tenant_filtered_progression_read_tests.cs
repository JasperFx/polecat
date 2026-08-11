using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Internal.Operations;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     polecat#441: the tenant-filtered progression read. <c>IEventDatabase</c> has carried
///     <c>AllProjectionProgress(string? tenantId, ...)</c> since jasperfx#407 with a default that
///     <em>throws</em> for a non-null tenant, and Polecat had never overridden it — so every consumer
///     reading progression through the shared abstractions could not scope to a tenant on the SQL Server
///     flavour at all.
///     <para>
///         Rows are matched structurally, on the parsed <see cref="ShardName" />, not by testing whether
///         a name ends with the tenant id. That is the marten#5179 lesson, and the cases below are the
///         ones a suffix test gets wrong.
///     </para>
/// </summary>
[Collection("integration")]
public class tenant_filtered_progression_read_tests : IntegrationContext
{
    public tenant_filtered_progression_read_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task WithStoreAsync(string schema)
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = JasperFx.AutoCreate.All;
        });

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM {theStore.Events.ProgressionTableName};";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private IEventDatabase theEventDatabase => theStore.Database;

    [Fact]
    public async Task returns_only_the_named_tenants_rows()
    {
        await WithStoreAsync("tenantprog_basic");

        await SeedAsync(new ShardName("Trip").ForTenant("acme"), 10);
        await SeedAsync(new ShardName("Trip").ForTenant("globex"), 20);
        await SeedAsync(new ShardName("Other", "All", 9).ForTenant("acme"), 30);
        await SeedAsync(new ShardName("Trip"), 40);                       // store-global row
        await SeedAsync(new ShardName(ShardState.HighWaterMark), 50);

        var acme = await theEventDatabase.AllProjectionProgress("acme", TestContext.Current.CancellationToken);

        acme.Select(x => x.ShardName).OrderBy(x => x)
            .ShouldBe(["Other:V9:All:acme", "Trip:All:acme"]);
        acme.Single(x => x.ShardName == "Trip:All:acme").Sequence.ShouldBe(10);
    }

    [Fact]
    public async Task a_null_tenant_is_store_global_and_matches_the_tenant_less_overload()
    {
        await WithStoreAsync("tenantprog_global");

        await SeedAsync(new ShardName("Trip").ForTenant("acme"), 10);
        await SeedAsync(new ShardName("Trip"), 20);

        var scoped = await theEventDatabase.AllProjectionProgress(null, TestContext.Current.CancellationToken);
        var all = await theEventDatabase.AllProjectionProgress(TestContext.Current.CancellationToken);

        scoped.Select(x => x.ShardName).OrderBy(x => x)
            .ShouldBe(all.Select(x => x.ShardName).OrderBy(x => x));
        scoped.Count.ShouldBe(2);
    }

    [Fact]
    public async Task a_tenant_whose_id_is_a_suffix_of_another_is_not_confused_with_it()
    {
        // The marten#5179 case a string suffix test gets wrong: "acme" is a suffix of "megaacme", so
        // `name LIKE '%acme'` would hand back the wrong tenant's progress.
        await WithStoreAsync("tenantprog_suffix");

        await SeedAsync(new ShardName("Trip").ForTenant("acme"), 10);
        await SeedAsync(new ShardName("Trip").ForTenant("megaacme"), 20);

        var acme = await theEventDatabase.AllProjectionProgress("acme", TestContext.Current.CancellationToken);
        acme.Select(x => x.ShardName).ShouldBe(["Trip:All:acme"]);

        var mega = await theEventDatabase.AllProjectionProgress("megaacme", TestContext.Current.CancellationToken);
        mega.Select(x => x.ShardName).ShouldBe(["Trip:All:megaacme"]);
    }

    [Fact]
    public async Task a_tenant_id_carrying_a_like_wildcard_matches_only_itself()
    {
        await WithStoreAsync("tenantprog_wildcards");

        await SeedAsync(new ShardName("Trip").ForTenant("a_me"), 10);
        await SeedAsync(new ShardName("Trip").ForTenant("axme"), 20);

        var scoped = await theEventDatabase.AllProjectionProgress("a_me", TestContext.Current.CancellationToken);
        scoped.Select(x => x.ShardName).ShouldBe(["Trip:All:a_me"]);
    }

    [Fact]
    public async Task the_store_global_and_high_water_rows_are_never_attributed_to_a_tenant()
    {
        await WithStoreAsync("tenantprog_nontenant");

        await SeedAsync(new ShardName("Trip"), 10);
        await SeedAsync(new ShardName(ShardState.HighWaterMark), 20);

        (await theEventDatabase.AllProjectionProgress("acme", TestContext.Current.CancellationToken))
            .ShouldBeEmpty();
    }

    [Fact]
    public async Task an_unknown_tenant_returns_an_empty_list_rather_than_throwing()
    {
        await WithStoreAsync("tenantprog_unknown");

        await SeedAsync(new ShardName("Trip").ForTenant("acme"), 10);

        (await theEventDatabase.AllProjectionProgress("nobody", TestContext.Current.CancellationToken))
            .ShouldBeEmpty();
    }

    [Fact]
    public async Task extended_columns_are_carried_on_the_tenant_scoped_read()
    {
        // The tenant-scoped path selects the same column list as the store-global one; this pins that a
        // narrowed read is not quietly a reduced one.
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "tenantprog_extended";
            opts.AutoCreateSchemaObjects = JasperFx.AutoCreate.All;
            opts.Events.EnableExtendedProgressionTracking = true;
        });

        await using (var conn = await OpenConnectionAsync())
        {
            await using var clear = conn.CreateCommand();
            clear.CommandText = $"DELETE FROM {theStore.Events.ProgressionTableName};";
            await clear.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var shard = new ShardName("Trip").ForTenant("acme");
        await SeedAsync(shard, 10);

        await theStore.Database.WriteExtendedProgressionAsync(
            new ShardState(shard.Identity, 10) { AgentStatus = "Running", LastHeartbeat = DateTimeOffset.UtcNow },
            TestContext.Current.CancellationToken);

        var scoped = await theEventDatabase.AllProjectionProgress("acme", TestContext.Current.CancellationToken);
        var row = scoped.Single();
        row.AgentStatus.ShouldBe("Running");
        row.LastHeartbeat.ShouldNotBeNull();
    }

    private async Task SeedAsync(ShardName shardName, long ceiling)
    {
        // The production write path (polecat#323).
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
