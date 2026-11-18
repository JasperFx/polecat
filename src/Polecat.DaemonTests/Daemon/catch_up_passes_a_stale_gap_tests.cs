using System.Diagnostics;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #791 and #780, end to end. An inter-event seq_id gap (a rolled-back append) sits under the high
///     water mark. <c>CatchUpAsync</c> runs <c>HighWaterAgent.CheckNowAsync</c>, which loops
///     <c>DetectInSafeZone</c> toward the committed ceiling until it gets there or its five-minute
///     backstop expires. The safe zone aged the gap from the HighWaterMark row's <c>last_updated</c>,
///     which every one of those detections re-stamps (#705), so it never skipped it: catch-up sat out the
///     full five minutes. That is the 300s hang in both issues — they ran in the shared <c>dbo</c> schema
///     of a CI lane, where earlier tests' rolled-back appends had left exactly this kind of gap.
/// </summary>
public class catch_up_passes_a_stale_gap_tests : OneOffConfigurationsContext
{
    [Fact]
    public async Task catch_up_skips_an_inter_event_gap_once_it_is_stale()
    {
        var ct = TestContext.Current.CancellationToken;

        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "catch_up_stale_gap";
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var before = Guid.NewGuid();
        var rolledBack = Guid.NewGuid();
        var after = Guid.NewGuid();

        foreach (var (id, name) in new[] { (before, "Before"), (rolledBack, "Rolled back"), (after, "After") })
        {
            await using var session = theStore.LightweightSession();
            session.Events.StartStream(id, new QuestStarted(name));
            await session.SaveChangesAsync(ct);
        }

        // Turn the middle append into a hole between two committed ones, which is what a rolled-back
        // append leaves behind.
        await using (var conn = await OpenConnectionAsync())
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                DELETE FROM [catch_up_stale_gap].[pc_events] WHERE stream_id = @id;
                DELETE FROM [catch_up_stale_gap].[pc_streams] WHERE id = @id;
                """;
            cmd.Parameters.AddWithValue("@id", rolledBack);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Deliberately not StartAllAsync first. CatchUpAsync stops every agent before it runs
        // CheckNowAsync, so the only detection left is the DetectInSafeZone loop. A running agent's own
        // Detect() used to step over a gap it found just above the mark without waiting, which is what made
        // the hang intermittent: it only showed when the agent had not got there first. It holds there now
        // (#833), so a leading gap takes this same route; catch_up_passes_a_rolled_back_append_above_the_mark
        // pins that one.
        using var daemon = await theStore.BuildProjectionDaemonAsync();

        // The default StaleSequenceThreshold is 3s. Before #791 this took CheckNowAsync's full five
        // minutes, so bound it well short of that rather than waiting it out on a regression.
        var stopwatch = Stopwatch.StartNew();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), ct).WaitAsync(TimeSpan.FromSeconds(60), ct);
        stopwatch.Stop();

        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(30));

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<QuestParty>(before, ct)).ShouldNotBeNull();
        (await query.LoadAsync<QuestParty>(after, ct)).ShouldNotBeNull().Name.ShouldBe("After");
    }
}
