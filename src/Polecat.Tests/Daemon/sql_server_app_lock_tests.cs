using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon.Coordination;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     Coverage for <see cref="SqlServerAppLock"/> — the SQL Server
///     <c>sp_getapplock</c> primitive Polecat's projection coordinator
///     uses to negotiate hot-cold leadership across nodes
///     ([polecat#83](https://github.com/JasperFx/polecat/issues/83)).
/// </summary>
[Collection("integration")]
public class sql_server_app_lock_tests : IntegrationContext
{
    public sql_server_app_lock_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    // Use a randomized lock id per test run so concurrent test runs don't collide.
    private static int RandomLockId() => Random.Shared.Next(1_000_000, int.MaxValue);

    [Fact]
    public async Task only_one_instance_can_acquire_a_given_lock()
    {
        var lockId = RandomLockId();
        var connStr = ConnectionSource.ConnectionString;

        await using var first = new SqlServerAppLock(connStr, "db-a", NullLogger.Instance);
        await using var second = new SqlServerAppLock(connStr, "db-a", NullLogger.Instance);

        var firstAttempt = await first.TryAttainLockAsync(lockId, default);
        var secondAttempt = await second.TryAttainLockAsync(lockId, default);

        firstAttempt.ShouldBeTrue();
        secondAttempt.ShouldBeFalse();

        first.HasLock(lockId).ShouldBeTrue();
        second.HasLock(lockId).ShouldBeFalse();
    }

    [Fact]
    public async Task release_lets_a_waiting_instance_acquire()
    {
        var lockId = RandomLockId();
        var connStr = ConnectionSource.ConnectionString;

        await using var first = new SqlServerAppLock(connStr, "db-a", NullLogger.Instance);
        await using var second = new SqlServerAppLock(connStr, "db-a", NullLogger.Instance);

        (await first.TryAttainLockAsync(lockId, default)).ShouldBeTrue();
        (await second.TryAttainLockAsync(lockId, default)).ShouldBeFalse();

        await first.ReleaseLockAsync(lockId);
        first.HasLock(lockId).ShouldBeFalse();

        (await second.TryAttainLockAsync(lockId, default)).ShouldBeTrue();
        second.HasLock(lockId).ShouldBeTrue();
    }

    [Fact]
    public async Task disposing_releases_all_session_locks()
    {
        var lockId = RandomLockId();
        var connStr = ConnectionSource.ConnectionString;

        var first = new SqlServerAppLock(connStr, "db-a", NullLogger.Instance);
        await using var second = new SqlServerAppLock(connStr, "db-a", NullLogger.Instance);

        (await first.TryAttainLockAsync(lockId, default)).ShouldBeTrue();
        await first.DisposeAsync();

        // Closing first's connection auto-released the SQL Server session lock,
        // so second should now be able to take it.
        (await second.TryAttainLockAsync(lockId, default)).ShouldBeTrue();
    }

    [Fact]
    public async Task multiple_distinct_lock_ids_are_independent()
    {
        var lockA = RandomLockId();
        var lockB = RandomLockId();
        var connStr = ConnectionSource.ConnectionString;

        await using var owner = new SqlServerAppLock(connStr, "db-a", NullLogger.Instance);
        await using var contender = new SqlServerAppLock(connStr, "db-a", NullLogger.Instance);

        (await owner.TryAttainLockAsync(lockA, default)).ShouldBeTrue();

        // contender can take lockB even though owner holds lockA — different
        // resources are independent under sp_getapplock.
        (await contender.TryAttainLockAsync(lockB, default)).ShouldBeTrue();

        owner.HasLock(lockA).ShouldBeTrue();
        owner.HasLock(lockB).ShouldBeFalse();
        contender.HasLock(lockA).ShouldBeFalse();
        contender.HasLock(lockB).ShouldBeTrue();
    }

    [Fact]
    public async Task try_attain_is_idempotent_when_already_owned()
    {
        var lockId = RandomLockId();
        var connStr = ConnectionSource.ConnectionString;

        await using var owner = new SqlServerAppLock(connStr, "db-a", NullLogger.Instance);

        (await owner.TryAttainLockAsync(lockId, default)).ShouldBeTrue();
        (await owner.TryAttainLockAsync(lockId, default)).ShouldBeTrue(); // already held — short-circuit
        owner.HasLock(lockId).ShouldBeTrue();
    }

    [Fact]
    public async Task release_of_unknown_id_is_a_noop()
    {
        var connStr = ConnectionSource.ConnectionString;
        await using var owner = new SqlServerAppLock(connStr, "db-a", NullLogger.Instance);
        await owner.ReleaseLockAsync(RandomLockId()); // should not throw
    }
}
