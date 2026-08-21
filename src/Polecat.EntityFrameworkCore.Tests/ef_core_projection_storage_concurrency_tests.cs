using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.EntityFrameworkCore;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     #489 / jasperfx#683. <see cref="EfCoreProjectionStorage{TDoc,TId,TDbContext}" /> wraps one
///     <see cref="DbContext" />, which is not thread-safe, but AggregationRunner applies every slice
///     in a range through a fixed 10-wide block over a single storage instance per tenant group. A
///     multi-stream projection with fan-out grouping therefore put up to ten threads on one change
///     tracker. Reported against Marten's identically shaped storage as marten#5266
///     (InvalidOperationException from Dictionary.TryInsert, NullReferenceException from
///     ChangeDetector.DetectChanges).
/// </summary>
/// <remarks>
///     These tests assert the ROUTING rather than trying to trip the race, and that is deliberate.
///     The race was never reproduced in Polecat: the block's channel is created with
///     AllowSynchronousContinuations, so a waiting reader's continuation runs on the posting thread
///     and slice applications end up serialized anyway under this workload — measured max concurrency
///     was 1 with the fix reverted, over 19,000 slice applications. A test that "passes" through that
///     is not evidence of anything, and a probabilistic assertion on top of it would be worse: green
///     whether or not the storage opted out. What genuinely flips when IsThreadSafe changes is which
///     dispatch path the runner takes, so that is what is asserted here.
/// </remarks>
public class ef_core_projection_storage_concurrency_tests : IAsyncLifetime
{
    // Comfortably wider than the runner's 10-wide block, so a range's worth of slices would have
    // saturated it several times over.
    private const int PlayersPerEvent = 40;
    private const int GameCount = 10;
    private const int PointsPerGame = 3;

    private static readonly string[] Players =
        Enumerable.Range(0, PlayersPerEvent).Select(i => $"player_{i:D3}").ToArray();

    private DocumentStore _store = null!;

    public async ValueTask InitializeAsync()
    {
        _store = await EfCoreTestHelper.CreateStoreWithFanOutProjection(ProjectionLifecycle.Async);
    }

    public ValueTask DisposeAsync()
    {
        PlayerTallyProjection.ProbeEnabled = false;
        _store?.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    ///     The regression itself: the daemon must apply this storage's slices inline, never through
    ///     AggregationRunner's concurrent block. Revert IsThreadSafe to true and this fails — the
    ///     probe sees JasperFx.Blocks.Block on the stack.
    /// </summary>
    [Fact]
    public async Task daemon_applies_slices_inline_and_never_through_the_concurrent_block()
    {
        var token = TestContext.Current.CancellationToken;

        await using (var session = _store.LightweightSession())
        {
            for (var i = 0; i < GameCount; i++)
            {
                session.Events.StartStream(Guid.NewGuid(),
                    new TeamScored(Guid.NewGuid(), Players, PointsPerGame));
            }

            await session.SaveChangesAsync(token);
        }

        await _store.WaitForProjectionAsync();

        // A rebuild, because it LOADS each snapshot before mutating it — so the aggregation mutates
        // entities the change tracker is already tracking, which is the shape that made concurrent
        // dispatch dangerous in the first place.
        PlayerTallyProjection.ResetProbe();

        var daemon = await _store.BuildProjectionDaemonAsync();
        await daemon.RebuildProjectionAsync<PlayerTallyProjection>(token);

        PlayerTallyProjection.ProbedCalls.ShouldBeGreaterThan(0);
        PlayerTallyProjection.SawBlockDispatch.ShouldBeFalse(
            "EF Core-backed projection storage must be applied inline — it declares IsThreadSafe => false");

        // Deliberately NOT asserting an absolute total. A rebuild does not empty ef_player_tallies —
        // Polecat's teardown does not own an EF Core table — so tallies accumulate once per pass.
        //
        // What holds regardless: every player is named by every event, so all 40 tallies must be
        // IDENTICAL and each internally consistent. Losing or double-applying a slice breaks that.
        var rows = new List<(string Player, int Points, int Appearances)>();
        foreach (var player in Players)
        {
            var row = await EfCoreTestHelper.QueryRowAsync(
                "SELECT points, appearances FROM ef_player_tallies WHERE id = @id",
                ("@id", player));

            row.ShouldNotBeNull();
            rows.Add((player, (int)row["points"]!, (int)row["appearances"]!));
        }

        var expected = rows[0];
        expected.Appearances.ShouldBeGreaterThan(0);
        (expected.Appearances % GameCount).ShouldBe(0);
        expected.Points.ShouldBe(expected.Appearances * PointsPerGame);

        foreach (var row in rows)
        {
            row.Appearances.ShouldBe(expected.Appearances,
                $"{row.Player} diverged from {expected.Player} — a slice was lost or applied twice");
            row.Points.ShouldBe(expected.Points,
                $"{row.Player} diverged from {expected.Player} — a slice was lost or applied twice");
        }

        var rowCount = await EfCoreTestHelper.QueryScalarAsync<int>(
            "SELECT COUNT(*) FROM ef_player_tallies");
        rowCount.ShouldBe(PlayersPerEvent);
    }
}

/// <summary>
///     #489, the deterministic half — deliberately in its own class with NO
///     <see cref="IAsyncLifetime" />.
/// </summary>
/// <remarks>
///     Every other test class in this project gates ALL of its tests behind
///     <see cref="RequiresNativeJsonFactAttribute" />, so on a SQL Server without the native
///     <c>json</c> type xUnit skips them all and never constructs the class — which is what keeps
///     their store-building InitializeAsync from running. A single plain <c>[Fact]</c> alongside a
///     gated one breaks that: the class gets constructed for the ungated test, InitializeAsync runs,
///     and store creation dies with "Cannot find data type json" on the edge matrix leg. Hence the
///     split — this assertion needs no database at all (it opens no connection), so it belongs
///     nowhere near a fixture.
/// </remarks>
public class ef_core_projection_storage_thread_safety_tests
{
    [Fact]
    public void ef_core_storage_declares_itself_not_thread_safe()
    {
        var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();
        optionsBuilder.UseSqlServer(ConnectionSource.ConnectionString);

        using var dbContext = new TestDbContext(optionsBuilder.Options);
        var storage = new EfCoreProjectionStorage<PlayerTally, string, TestDbContext>(dbContext, "*DEFAULT*");

        ((IProjectionStorage<PlayerTally, string>)storage).IsThreadSafe.ShouldBeFalse();
    }
}
