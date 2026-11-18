using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #500. On a cold start every async agent kicks high-water detection, and each detection ends
///     in <c>MarkHighWaterAsync</c> writing the one <c>HighWaterMark</c> row. Before the fix that
///     upsert was a bare <c>MERGE</c>: with no row yet, concurrent writers all probed, all missed,
///     and all took the INSERT branch, so every loser got
///     "Violation of PRIMARY KEY constraint 'pkey_pc_event_progression_name'".
///     Reported against a Wolverine-managed fleet where four agents started at once; the retry
///     succeeded, so the visible cost was a warn-with-stack on every boot rather than a failure.
/// </summary>
[Collection("integration")]
public class Bug_500_concurrent_high_water_mark : IntegrationContext
{
    public Bug_500_concurrent_high_water_mark(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        // The cold-start shape is the whole point: no HighWaterMark row for anyone to match on.
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM [dbo].[pc_event_progression];";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task concurrent_cold_start_marks_do_not_collide()
    {
        var token = TestContext.Current.CancellationToken;
        var detector = CreateDetector();

        // Well above the four agents in the report. The race window is one autocommit statement
        // wide, so the writers have to arrive genuinely together to land inside it.
        const int writers = 32;
        const int rounds = 8;

        for (var round = 0; round < rounds; round++)
        {
            // Warm the connection pool BEFORE the timed section. Without this the first few
            // OpenAsync calls pay pool creation and the writers trickle in one at a time, each
            // finding the row the previous one just committed — the test then passes against a
            // bare MERGE and guards nothing.
            await WarmPoolAsync(writers, token);

            // Back to the cold-start shape: no HighWaterMark row for anyone to match on.
            await DeleteHighWaterRowAsync(token);

            using var gate = new SemaphoreSlim(0, writers);
            var marks = Enumerable.Range(1, writers).Select(async i =>
            {
                await gate.WaitAsync(token);
                await detector.MarkHighWaterAsync(i, token);
            }).ToArray();

            gate.Release(writers);

            // The assertion is that nothing threw. A PK violation (2627) is the reported bug; a
            // deadlock (1205) is the bug that HOLDLOCK-without-UPDLOCK trades it for.
            await Task.WhenAll(marks);

            // And the upsert converges on one row rather than a pile.
            (await CountHighWaterRowsAsync(token)).ShouldBe(1);
        }
    }

    private async Task WarmPoolAsync(int connections, CancellationToken token)
    {
        // Open them all at once and hold them open together, so the pool actually grows to
        // `connections` rather than handing the same one back serially.
        var open = await Task.WhenAll(Enumerable.Range(0, connections).Select(async _ =>
        {
            var conn = new SqlConnection(theStore.Options.ConnectionString);
            await conn.OpenAsync(token);
            return conn;
        }));

        foreach (var conn in open)
        {
            await conn.DisposeAsync();
        }
    }

    private async Task DeleteHighWaterRowAsync(CancellationToken token)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "DELETE FROM [dbo].[pc_event_progression] WHERE name = 'HighWaterMark';";
        await cmd.ExecuteNonQueryAsync(token);
    }

    private async Task<int> CountHighWaterRowsAsync(CancellationToken token)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT COUNT(*) FROM [dbo].[pc_event_progression] WHERE name = 'HighWaterMark';";
        return (int)(await cmd.ExecuteScalarAsync(token))!;
    }

    private PolecatHighWaterDetector CreateDetector()
    {
        return new PolecatHighWaterDetector(
            theStore.Database.Events,
            theStore.Options.ConnectionString,
            theStore.Options.DaemonSettings,
            NullLogger<PolecatHighWaterDetector>.Instance,
            theStore.Options.ResiliencePipeline);
    }
}
