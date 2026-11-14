using DcbLoadTest;

// DCB load and behaviour harness, ported from Marten 10.0's src/DcbLoadTest (see its RESULTS-10.0.md).
// Run in Release against a catalog nothing else is using:
//
//   docker compose up -d        (SQL Server 2025 on localhost,11433, container polecat-sqlserver)
//   dotnet run -c Release --project src/DcbLoadTest -- <scenario> [--out results.md] [--key value]...
//
// The harness uses its own database, polecat_dcb_load by default (--database <name>), created through
// master if it does not exist. POLECAT_DCB_LOAD_DATABASE overrides the whole connection string. Max Pool
// Size is raised to at least --max-pool (400). Every scenario variant gets a fresh schema.
//
// --scopes TagValue,TagValueRecordingEventTypes,TagValueAndEventType narrows every scenario's matrix
// (default TagValue,TagValueAndEventType). There is no --modes: Polecat stores tags in tag tables only.

var settings = new Settings(args);
Report.Open(settings);

// Microsoft.Data.SqlClient has no asynchronous BeginTransaction / Commit (see Polecat's
// SqlTransactionRequests), so every save blocks a thread-pool thread for those round trips. With 50
// concurrent requests the pool starts below that (min = core count) and injects threads slowly, so
// the first variant of a run could pay for the ramp. --min-threads (default 256; 0 leaves the runtime
// default) pre-sizes the pool so the ramp is not inside any measurement. It is in the header line.
var minThreads = settings.Int("min-threads", 256);
if (minThreads > 0)
{
    ThreadPool.GetMinThreads(out _, out var io);
    ThreadPool.SetMinThreads(minThreads, Math.Max(io, minThreads));
}

if (settings.Scenario is "append" or "read" or "contend" or "crossed" or "growth" or "legacy")
{
    await Sql.EnsureDatabase(settings);
}

switch (settings.Scenario)
{
    case "append":
        // --repeats 3 --single-saves 2000 --batch-saves 400 --concurrent-saves 100 --writers 16
        await AppendScenario.Run(settings);
        break;

    case "read":
        // --seed-streams 100000 (x10 events) --iterations 1000 --seed-writers 8 --seed-streams-per-save 50 --reseed
        await ReadScenario.Run(settings);
        break;

    case "contend":
        // --concurrency 50 --rounds 5 --app-retries 2 --capacity 100 --book 0,0.2,1 --write-retries on,off
        await ContentionScenario.Run(settings, crossed: false);
        break;

    case "crossed":
        // --concurrency 50 --rounds 5 --classes 4 --book 0.5 --write-retries on,off
        await ContentionScenario.Run(settings, crossed: true);
        break;

    case "growth":
        // --tag-values 200000 --seconds 300 --writers 16 --fillfactors none,70 --sample-seconds 30
        // --sample-mode DETAILED|SAMPLED|LIMITED --update-stats
        await GrowthScenario.Run(settings);
        break;

    case "legacy":
        // The harness's original append benchmark: --seed-streams 1000 --bench-streams 200
        await LegacyScenario.Run(settings);
        break;

    default:
        Console.WriteLine("usage: DcbLoadTest <append|read|contend|crossed|growth|legacy> [--out file.md] [--key value]...");
        Console.WriteLine("See the comments in Program.cs for each scenario's options.");
        return 1;
}

return 0;
