using System.Diagnostics;
using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat;

// -- Configuration ---------------------------------------------------------------
const int SEED_STREAMS = 1000;
const int SEED_EVENTS_PER_STREAM = 10;
const int BENCH_STREAMS = 200;
const int BENCH_EVENTS_PER_STREAM = 10;

var connectionString =
    Environment.GetEnvironmentVariable("POLECAT_TESTING_DATABASE")
    ?? "Server=localhost,11433;User Id=sa;Password=P@55w0rd;Timeout=5;MultipleActiveResultSets=True;Initial Catalog=master;Encrypt=False";    ;

Console.WriteLine("DCB Load Test - Append into large database (SQL Server)");
Console.WriteLine($"  Seed: {SEED_STREAMS} streams x {SEED_EVENTS_PER_STREAM} events = {SEED_STREAMS * SEED_EVENTS_PER_STREAM} events");
Console.WriteLine($"  Bench: {BENCH_STREAMS} streams x {BENCH_EVENTS_PER_STREAM} events = {BENCH_STREAMS * BENCH_EVENTS_PER_STREAM} events");
Console.WriteLine(new string('-', 90));

// -- Helpers ---------------------------------------------------------------------
async Task DropAllEventStoreTablesAsync()
{
    await using var conn = new SqlConnection(connectionString);
    await conn.OpenAsync();
    await using var cmd = conn.CreateCommand();
    cmd.CommandText = """
        DECLARE @sql NVARCHAR(MAX) = '';
        SELECT @sql = @sql + 'DROP TABLE [dbo].[' + TABLE_NAME + ']; '
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME LIKE 'pc_event_tag_%';
        EXEC sp_executesql @sql;
        IF OBJECT_ID('dbo.pc_events', 'U') IS NOT NULL DROP TABLE dbo.pc_events;
        IF OBJECT_ID('dbo.pc_streams', 'U') IS NOT NULL DROP TABLE dbo.pc_streams;
        IF OBJECT_ID('dbo.pc_event_progression', 'U') IS NOT NULL DROP TABLE dbo.pc_event_progression;
        """;
    await cmd.ExecuteNonQueryAsync();
}

DocumentStore CreateStore()
{
    return DocumentStore.For(opts =>
    {
        opts.ConnectionString = connectionString;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.Events.RegisterTagType<CustomerId>("customer");
        opts.Events.RegisterTagType<RegionId>("region");
    });
}

var results = new List<(string Scenario, int Iterations, TimeSpan Elapsed)>();

// -- Seed large DB ---------------------------------------------------------------
Console.WriteLine("\nPreparing large database...");
await DropAllEventStoreTablesAsync();

using (var store = CreateStore())
{
    await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

    Console.Write($"  Seeding {SEED_STREAMS * SEED_EVENTS_PER_STREAM} tagged events...");
    var sw = Stopwatch.StartNew();

    for (var s = 0; s < SEED_STREAMS; s++)
    {
        await using var session = store.LightweightSession();
        var streamId = Guid.NewGuid();
        var customerId = new CustomerId(Guid.NewGuid());
        var regionId = new RegionId($"region-{s % 10}");

        for (var e = 0; e < SEED_EVENTS_PER_STREAM; e++)
        {
            object data = e % 2 == 0
                ? new OrderPlaced($"ORD-{s}-{e}", 99.99m + e)
                : new OrderShipped($"TRACK-{s}-{e}");

            var evt = session.Events.BuildEvent(data);
            evt.WithTag(customerId, regionId);
            session.Events.Append(streamId, evt);
        }

        await session.SaveChangesAsync();
    }

    sw.Stop();
    Console.WriteLine($" done in {sw.Elapsed.TotalSeconds:N1}s");

    // -- Benchmark: No Tags ---
    Console.Write($"  No Tags...");
    {
        var bsw = Stopwatch.StartNew();
        var totalEvents = 0;

        for (var s = 0; s < BENCH_STREAMS; s++)
        {
            await using var session = store.LightweightSession();
            var streamId = Guid.NewGuid();

            for (var e = 0; e < BENCH_EVENTS_PER_STREAM; e++)
            {
                object data = e % 2 == 0
                    ? new OrderPlaced($"ORD-bench-{s}-{e}", 99.99m + e)
                    : new OrderShipped($"TRACK-bench-{s}-{e}");
                session.Events.Append(streamId, data);
            }

            await session.SaveChangesAsync();
            totalEvents += BENCH_EVENTS_PER_STREAM;
        }

        bsw.Stop();
        results.Add(("No Tags", totalEvents, bsw.Elapsed));
        Console.WriteLine($" {totalEvents} events in {bsw.Elapsed.TotalMilliseconds:N1}ms ({totalEvents / bsw.Elapsed.TotalSeconds:N1} events/sec)");
    }

    // -- Benchmark: With Tags ---
    Console.Write($"  With Tags...");
    {
        var bsw = Stopwatch.StartNew();
        var totalEvents = 0;

        for (var s = 0; s < BENCH_STREAMS; s++)
        {
            await using var session = store.LightweightSession();
            var streamId = Guid.NewGuid();
            var customerId = new CustomerId(Guid.NewGuid());
            var regionId = new RegionId($"region-{s % 10}");

            for (var e = 0; e < BENCH_EVENTS_PER_STREAM; e++)
            {
                object data = e % 2 == 0
                    ? new OrderPlaced($"ORD-bench-{s}-{e}", 99.99m + e)
                    : new OrderShipped($"TRACK-bench-{s}-{e}");

                var evt = session.Events.BuildEvent(data);
                evt.WithTag(customerId, regionId);
                session.Events.Append(streamId, evt);
            }

            await session.SaveChangesAsync();
            totalEvents += BENCH_EVENTS_PER_STREAM;
        }

        bsw.Stop();
        results.Add(("With Tags", totalEvents, bsw.Elapsed));
        Console.WriteLine($" {totalEvents} events in {bsw.Elapsed.TotalMilliseconds:N1}ms ({totalEvents / bsw.Elapsed.TotalSeconds:N1} events/sec)");
    }
}

// -- Results summary -------------------------------------------------------------
Console.WriteLine();
Console.WriteLine(new string('=', 90));
Console.WriteLine($"{"Scenario",-40} {"Events",10} {"Total (ms)",12} {"Events/sec",12}");
Console.WriteLine(new string('-', 90));
foreach (var (scenario, iterations, elapsed) in results)
{
    var opsPerSec = elapsed.TotalSeconds > 0
        ? (iterations / elapsed.TotalSeconds).ToString("N1")
        : "N/A";
    Console.WriteLine($"{scenario,-40} {iterations,10} {elapsed.TotalMilliseconds,12:N1} {opsPerSec,12}");
}
Console.WriteLine(new string('=', 90));

// -- Types -----------------------------------------------------------------------
record OrderPlaced(string OrderId, decimal Amount);
record OrderShipped(string TrackingNumber);
record CustomerId(Guid Value);
record RegionId(string Value);
