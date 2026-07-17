using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Internal.Operations;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
/// #220 (jasperfx#473/#474, JasperFx.Events 2.16.0): exact-identity progression delete via the
/// store-agnostic <see cref="IEventDatabase.DeleteProjectionProgressByShardNameAsync"/> override.
///
/// Exercises every permutation of <see cref="ShardName.Identity"/>:
///   - default key "All", version 1            → "Trip:All"
///   - custom shard key, version 1             → "Trip:Lines"
///   - versioned (V&gt;1), key "All"           → "claim_lines:V9:All"
///   - versioned + custom key                  → "claim_lines:V9:Lines"
///   - per-tenant, version 1                   → "Trip:All:acme"
///   - per-tenant + versioned                  → "claim_lines:V9:All:acme"
///   - per-tenant + versioned + custom key     → "claim_lines:V9:Lines:acme"
///
/// plus the two semantics the issue calls out: exact-equality (no prefix/tenant collateral) and a
/// clean zero-row no-op for a non-existent identity.
/// </summary>
[Collection("integration")]
public class delete_projection_progress_by_shard_name_tests : IntegrationContext
{
    public delete_projection_progress_by_shard_name_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        // Clean progression table for each test
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM [dbo].[pc_event_progression];";
        await cmd.ExecuteNonQueryAsync();
    }

    public static IEnumerable<object[]> AllShardNamePermutations()
    {
        // name, shardKey, version, tenantId — expected Identity in the comment
        yield return [new ShardName("Trip")];                                  // "Trip:All"
        yield return [new ShardName("Trip", "Lines", 1)];                      // "Trip:Lines"
        yield return [new ShardName("claim_lines", "All", 9)];                 // "claim_lines:V9:All"
        yield return [new ShardName("claim_lines", "Lines", 9)];               // "claim_lines:V9:Lines"
        yield return [new ShardName("Trip").ForTenant("acme")];                // "Trip:All:acme"
        yield return [new ShardName("claim_lines", "All", 9).ForTenant("acme")];   // "claim_lines:V9:All:acme"
        yield return [new ShardName("claim_lines", "Lines", 9).ForTenant("acme")]; // "claim_lines:V9:Lines:acme"
    }

    [Theory]
    [MemberData(nameof(AllShardNamePermutations))]
    public async Task deletes_the_exact_identity_for_every_permutation(ShardName shard)
    {
        await InsertProgressAsync(shard, 12);

        // sanity: the row exists before the delete
        (await theStore.Database.ProjectionProgressFor(shard)).ShouldBe(12);

        // Reach it through the store-agnostic abstraction (issue requirement #2) — no reflection.
        await ((IEventDatabase)theStore.Database)
            .DeleteProjectionProgressByShardNameAsync(shard.Identity, CancellationToken.None);

        (await theStore.Database.ProjectionProgressFor(shard)).ShouldBe(0);
        var all = await theStore.Database.AllProjectionProgress();
        all.ShouldNotContain(s => s.ShardName == shard.Identity);
    }

    [Fact]
    public async Task exact_equality_does_not_drop_prefix_sibling()
    {
        // The issue's canonical hazard: deleting "claim_lines:V9:All" must NOT also drop
        // "claim_lines:V9:AllOther", which a prefix LIKE 'name%' delete would wrongly catch.
        var target = new ShardName("claim_lines", "All", 9);          // claim_lines:V9:All
        var sibling = new ShardName("claim_lines", "AllOther", 9);    // claim_lines:V9:AllOther

        await InsertProgressAsync(target, 10);
        await InsertProgressAsync(sibling, 20);

        await ((IEventDatabase)theStore.Database)
            .DeleteProjectionProgressByShardNameAsync(target.Identity, CancellationToken.None);

        (await theStore.Database.ProjectionProgressFor(target)).ShouldBe(0);
        (await theStore.Database.ProjectionProgressFor(sibling)).ShouldBe(20); // survives
    }

    [Fact]
    public async Task exact_equality_does_not_drop_tenant_scoped_sibling()
    {
        // A non-tenant identity is a strict prefix of the per-tenant one ("Trip:All" vs
        // "Trip:All:acme"). Exact equality must leave the tenant row alone.
        var shared = new ShardName("Trip");                  // Trip:All
        var perTenant = new ShardName("Trip").ForTenant("acme"); // Trip:All:acme

        await InsertProgressAsync(shared, 5);
        await InsertProgressAsync(perTenant, 7);

        await ((IEventDatabase)theStore.Database)
            .DeleteProjectionProgressByShardNameAsync(shared.Identity, CancellationToken.None);

        (await theStore.Database.ProjectionProgressFor(shared)).ShouldBe(0);
        (await theStore.Database.ProjectionProgressFor(perTenant)).ShouldBe(7); // survives
    }

    [Fact]
    public async Task nonexistent_identity_is_a_clean_no_op()
    {
        var keep = new ShardName("Trip");
        await InsertProgressAsync(keep, 3);

        // No matching row — must not throw and must not touch unrelated rows.
        await Should.NotThrowAsync(((IEventDatabase)theStore.Database)
            .DeleteProjectionProgressByShardNameAsync("does_not_exist:V3:All:nobody", CancellationToken.None));

        (await theStore.Database.ProjectionProgressFor(keep)).ShouldBe(3);
    }

    [Fact]
    public async Task deletes_only_the_targeted_row_among_all_permutations()
    {
        var shards = AllShardNamePermutations().Select(o => (ShardName)o[0]).ToArray();
        for (var i = 0; i < shards.Length; i++)
        {
            await InsertProgressAsync(shards[i], (i + 1) * 10);
        }

        var victim = shards[3]; // claim_lines:V9:Lines
        await ((IEventDatabase)theStore.Database)
            .DeleteProjectionProgressByShardNameAsync(victim.Identity, CancellationToken.None);

        for (var i = 0; i < shards.Length; i++)
        {
            var expected = shards[i].Identity == victim.Identity ? 0 : (i + 1) * 10;
            (await theStore.Database.ProjectionProgressFor(shards[i])).ShouldBe(expected);
        }
    }

    private async Task InsertProgressAsync(ShardName shardName, long ceiling)
    {
        // Seed through the production write path (polecat#323).
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
        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);
        await op.PostprocessAsync(reader, new List<Exception>(), CancellationToken.None);
    }
}
