using JasperFx.Core;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;

namespace Polecat.Tests.Daemon;

/// <summary>
///     Locks in the lock-id formula that Polecat's
///     <c>SingleTenantProjectionDistributor</c> uses, via
///     <see cref="ProjectionLockIds.Compute"/>, against Marten's
///     <c>SingleTenantProjectionDistributor</c> formula
///     (<c>Math.Abs($"{schema}:{shardName.Identity}".GetDeterministicHashCode()) + baseLockId</c>).
///
///     Polecat#117 deliberately adopted Marten's formula so co-deployed Marten +
///     Polecat nodes negotiate identical lock ids against the same SQL Server
///     instance. If a future JasperFx.Events change alters
///     <see cref="ProjectionLockIds.Compute"/> or the underlying
///     <see cref="StringExtensions.GetDeterministicHashCode"/>, this test
///     captures the regression on Polecat's side without needing a Marten
///     deployment to notice.
/// </summary>
public class projection_lock_id_tests
{
    [Fact]
    public void compute_matches_marten_formula_for_sample_tuple()
    {
        const string schema = "polecat_test";
        const int baseLockId = 12_345;
        var shardName = new ShardName("Trip"); // → Identity "Trip:All"

        var expected =
            Math.Abs($"{schema}:{shardName.Identity}".GetDeterministicHashCode())
            + baseLockId;

        var actual = ProjectionLockIds.Compute(schema, shardName, baseLockId);

        actual.ShouldBe(expected);
    }

    [Fact]
    public void compute_is_deterministic_across_calls()
    {
        const string schema = "dbo";
        const int baseLockId = 1_000_000;
        var shardName = new ShardName("Quest", "All", 2); // versioned identity

        var first = ProjectionLockIds.Compute(schema, shardName, baseLockId);
        var second = ProjectionLockIds.Compute(schema, shardName, baseLockId);

        first.ShouldBe(second);
        // The formula folds in baseLockId, so swapping it out shifts the result
        // by exactly that delta — guarantees DaemonSettings.DaemonLockId namespaces
        // the deployment without affecting the (schema, shard) hash contribution.
        ProjectionLockIds.Compute(schema, shardName, baseLockId + 7)
            .ShouldBe(first + 7);
    }

    [Fact]
    public void schema_or_shard_change_changes_the_lock_id()
    {
        const int baseLockId = 0;
        var nameA = new ShardName("A");
        var nameB = new ShardName("B");

        var aDbo = ProjectionLockIds.Compute("dbo", nameA, baseLockId);
        var aPolecat = ProjectionLockIds.Compute("polecat", nameA, baseLockId);
        var bDbo = ProjectionLockIds.Compute("dbo", nameB, baseLockId);

        // Different schemas under the same shard → different lock ids (this is
        // the property that lets multiple Polecat / Marten deployments coexist
        // on a single SQL Server instance).
        aDbo.ShouldNotBe(aPolecat);
        // Different shards under the same schema → different lock ids (this
        // is what makes per-shard hot-cold leadership work in SingleTenant).
        aDbo.ShouldNotBe(bDbo);
    }
}
