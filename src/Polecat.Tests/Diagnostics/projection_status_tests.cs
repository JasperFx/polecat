using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.ExplorerApi;

/// <summary>
///     #589 — what <c>GetProjectionStatusesAsync</c> reports, beyond the projection names and
///     lifecycles <c>event_store_explorer_tests</c> already covers.
/// </summary>
/// <remarks>
///     Every fact here pins a field this method used to answer with something it did not know: a
///     hardcoded shard state, a lifecycle wedged into the state slot, a high-water row standing in for
///     the head of the store, and a tenant dimension that refused outright.
/// </remarks>
[Collection("integration")]
public class projection_status_tests : IntegrationContext
{
    public projection_status_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> StoreWithProjectionsAsync(string schema)
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = schema;
            opts.Projections.Add<SingleStreamProjection<QuestParty, Guid>>(ProjectionLifecycle.Inline);
            opts.Projections.Add<QuestLogProjection>(ProjectionLifecycle.Async);
        });

        return theStore;
    }

    [Fact]
    public async Task shard_state_is_unknown_rather_than_a_confident_stopped()
    {
        var store = await StoreWithProjectionsAsync("status_state");
        var statuses = await ((IEventStore)store).GetProjectionStatusesAsync(TestContext.Current.CancellationToken);

        var shards = statuses.SelectMany(x => x.Shards).ToList();
        shards.ShouldNotBeEmpty();

        // "Stopped" was hardcoded here for every shard. It is a fact about the running daemon, which
        // the progression table does not know, so it was indistinguishable from a daemon that really
        // had stopped — and that is the reading an operator acts on.
        shards.ShouldAllBe(x => x.State == "Unknown");
    }

    [Fact]
    public async Task an_inline_projections_shard_reports_a_state_not_its_lifecycle()
    {
        var store = await StoreWithProjectionsAsync("status_inline");
        var statuses = await ((IEventStore)store).GetProjectionStatusesAsync(TestContext.Current.CancellationToken);

        var inline = statuses.Where(x => x.ProjectionName == nameof(QuestParty)).ShouldHaveSingleItem();

        // The lifecycle belongs on the ProjectionStatus, and only there.
        inline.Lifecycle.ShouldBe(ProjectionLifecycle.Inline.ToString());
        inline.Shards.ShouldAllBe(x => x.State != ProjectionLifecycle.Inline.ToString());
    }

    [Fact]
    public async Task no_shard_state_anywhere_is_a_lifecycle_name()
    {
        var store = await StoreWithProjectionsAsync("status_no_lifecycle");
        var statuses = await ((IEventStore)store).GetProjectionStatusesAsync(TestContext.Current.CancellationToken);

        var lifecycles = Enum.GetNames<ProjectionLifecycle>();

        // The sharp version of the fact above: whatever State holds, it is never drawn from the
        // lifecycle vocabulary, so a consumer reading it cannot be handed the wrong kind of value.
        statuses.SelectMany(x => x.Shards)
            .ShouldAllBe(shard => !lifecycles.Contains(shard.State));
    }

    [Fact]
    public async Task event_store_sequence_is_the_head_of_the_store_not_the_daemon_high_water_row()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await StoreWithProjectionsAsync("status_head");

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<QuestParty>(Guid.NewGuid(), new QuestStarted("head"), new QuestStarted("more"));
            await session.SaveChangesAsync(ct);
        }

        var statuses = await ((IEventStore)store).GetProjectionStatusesAsync(ct);
        var shards = statuses.SelectMany(x => x.Shards).ToList();
        shards.ShouldNotBeEmpty();

        // No daemon has run, so the high-water progression row is absent or 0. Sourcing
        // EventStoreSequence from it reported every shard as 0-of-0 — caught up — over a store that
        // genuinely has events waiting. The head comes from MAX(seq_id) instead.
        shards.ShouldAllBe(x => x.EventStoreSequence >= 2);
    }

    [Fact]
    public async Task the_tenant_dimension_answers_instead_of_throwing()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await StoreWithProjectionsAsync("status_tenant");

        // Used to be a NotSupportedException from the jasperfx#407 default.
        var statuses = await ((IEventStore)store).GetProjectionStatusesAsync("blue", ct);

        statuses.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task tenant_scoped_shard_identities_carry_the_tenant()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await StoreWithProjectionsAsync("status_tenant_names");

        var untenanted = await ((IEventStore)store).GetProjectionStatusesAsync(ct);
        var tenanted = await ((IEventStore)store).GetProjectionStatusesAsync("blue", ct);

        var plain = untenanted.SelectMany(x => x.Shards).Select(x => x.ShardName).ToList();
        var scoped = tenanted.SelectMany(x => x.Shards).Select(x => x.ShardName).ToList();

        plain.ShouldNotBeEmpty();
        scoped.Count.ShouldBe(plain.Count);

        // On a single-database store the tenant lives inside it and its shards carry the trailing
        // :tenant suffix. Reporting the untenanted identity would describe a different shard.
        scoped.ShouldAllBe(name => name.EndsWith(":blue"));
        plain.ShouldAllBe(name => !name.EndsWith(":blue"));
    }

    [Fact]
    public async Task a_tenant_scoped_read_finds_the_tenant_bearing_progression_row()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await StoreWithProjectionsAsync("status_tenant_progress");

        // The identity the daemon writes under per-tenant partitioning. Seeded directly so the fact
        // does not depend on running a daemon.
        var tenanted = (await ((IEventStore)store).GetProjectionStatusesAsync("blue", ct))
            .SelectMany(x => x.Shards)
            .Select(x => x.ShardName)
            .First();

        await using (var conn = new SqlConnection(store.Options.ConnectionString))
        {
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            // Delete-then-insert rather than a bare INSERT: the test database keeps its schema between
            // runs, so a bare insert passes once and then trips the primary key forever after.
            cmd.CommandText = $"""
                DELETE FROM {store.Options.EventGraph.ProgressionTableName} WHERE name = @name;
                INSERT INTO {store.Options.EventGraph.ProgressionTableName} (name, last_seq_id) VALUES (@name, 42);
                """;
            cmd.Parameters.AddWithValue("@name", tenanted);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        var statuses = await ((IEventStore)store).GetProjectionStatusesAsync("blue", ct);
        var shard = statuses.SelectMany(x => x.Shards).Single(x => x.ShardName == tenanted);

        // This is the whole defect: the registrations are untenanted, the rows are suffixed, and
        // matching them by filtering rather than by COMPOSING the identity found nothing — every
        // shard reported 0 and looked healthy.
        shard.ProcessedSequence.ShouldBe(42);
    }
}
