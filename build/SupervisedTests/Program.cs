using System.Diagnostics;
using Bobcat.Resilience;
using Bobcat.Supervisor;
using Microsoft.Data.SqlClient;
using Polecat.Build.SupervisedTests;

// The supervised test runner: drives a Polecat test executable across several worker processes
// through Bobcat's supervisor (https://github.com/JasperFx/bobcat) instead of invoking it once.
//
// Why this exists: the `polecat` workflow's Polecat.Tests job is the critical path of every PR —
// roughly 40 minutes for `default` storage and 26 for `edge`, against ~2 minutes for every other
// job in the matrix. Nothing else in the workflow is close, so the suite's wall clock IS the
// feedback loop.
//
// What makes it safe here rather than merely faster:
//
//   - Partitioning is by test CLASS, never by test. xUnit's isolation contract is per class, so a
//     class's fixtures and static state assume one process. Bobcat guarantees this; the suite does
//     not have to.
//   - Each worker gets its OWN CATALOG via POLECAT_TESTING_DATABASE. Polecat.Tests isolates by
//     DatabaseSchemaName inside one catalog, which separates classes from each other but not two
//     processes from each other — so isolation has to be the database.
//   - The suite was made parallel-ready before this runner existed: ConnectionSource.Scoped() for
//     anything at SERVER scope (a database a test creates is a sibling of the worker's, not a
//     child, so per-worker catalogs do not isolate it), and no connection strings rewritten by
//     text. See the "Writing tests that survive being run in parallel processes" section of
//     CLAUDE.md, which is the contract this runner depends on.
//
// Usage:
//   dotnet run --project build/SupervisedTests -- \
//       --executable src/Polecat.Tests/bin/Release/net9.0/Polecat.Tests [--workers N]

var executable = ArgValue("--executable")
                 ?? throw new ArgumentException("--executable <path to the MTP test host> is required.");

if (!File.Exists(executable))
{
    throw new FileNotFoundException(
        $"Test host '{executable}' does not exist. Build the test project first.", executable);
}

// The committed count is tuned on a developer machine; the machine actually running decides what it
// can carry. A GitHub-hosted runner is 4 vCPU / 16 GB and is also hosting the SQL Server container,
// and an oversubscribed fleet does not fail politely — Wolverine's rollout killed hosted runners
// outright ("The runner has received a shutdown signal", which is the runner dying, not a test).
// An explicit --workers bypasses the clamp: measuring past the ceiling is a valid thing to ask.
var requested = int.TryParse(ArgValue("--workers"), out var w) ? w : 4;
var explicitlyAsked = ArgValue("--workers") is not null;
var ceiling = Math.Max(1, Environment.ProcessorCount / 2);
var workers = !explicitlyAsked && requested > ceiling ? ceiling : requested;

if (workers != requested)
{
    Console.WriteLine(
        $"Clamping {requested} workers to {workers} for this machine's {Environment.ProcessorCount} core(s).");
}

var retriesOff = Environment.GetCommandLineArgs().Contains("--disable-test-retry");

Console.WriteLine(
    $"=== Supervised run: {Path.GetFileName(executable)}, {workers} worker(s){(retriesOff ? ", retries OFF" : "")} ===");

EnsureLaneDatabases(workers);

var factory = new MtpWorkerFactory(executable)
{
    // Discovery and any isolated/recycled worker report lane 0, so the catalogs provisioned equal
    // the workers asked for rather than the processes launched.
    EnvironmentFor = context => new Dictionary<string, string>
    {
        ["POLECAT_TESTING_DATABASE"] = LaneConnectionString(context.Lane)
    }
};

// A substring match on the test's display name, ANDed onto nothing else. Exists so a supervised
// run can be exercised against one class without paying for the whole suite — the TRX round trip
// in particular. A filter that matches NOTHING fails the run rather than reporting a green empty
// one: an emptied filter after a rename must not read as a pass.
var filter = ArgValue("--filter");

var supervisor = new Supervisor(factory)
{
    MaxParallelWorkers = workers,
    TestFilter = filter is null
        ? null
        : test => test.DisplayName.Contains(filter, StringComparison.OrdinalIgnoreCase),

    // Off by default when measuring. A retry budget masks exactly the instability parallelism is
    // suspected of introducing, so the first green run at a new fleet size should be earned without
    // one — and a pass-on-retry is reported as flaky, never folded into a clean pass.
    RetryBudget = retriesOff
        ? RetryBudget.None
        : new RetryBudget { MaxAttemptsPerTest = 2, MaxRetriesPerRun = 25 },

    // Fresh-process retries mean workers+1 hosts resident at once unless idle lanes are released
    // first. That is what OOM-killed 16GB runners twice during Wolverine's rollout, both times
    // during a retry.
    ReleaseIdleLanes = true,

    // Report-only, all three. None of them fails or kills anything; they exist so a wedged job's
    // log says where it stopped, which is the thing a capped job never reaches.
    StallThreshold = TimeSpan.FromMinutes(5),
    HeartbeatInterval = TimeSpan.FromSeconds(30),

    Log = Console.WriteLine
};

if (!retriesOff) supervisor.AddFailurePolicy(new RetryFailuresInFreshProcess());

var startedAt = DateTime.UtcNow;
var stopwatch = Stopwatch.StartNew();
var results = await supervisor.Run();
stopwatch.Stop();

if (filter is not null && results.Tests.Count == 0)
{
    Console.Error.WriteLine($"No test matched --filter '{filter}'. Refusing to report an empty run as a pass.");
    return 1;
}

Console.WriteLine(RunReport.ToText(results));
Console.WriteLine($"Wall clock: {stopwatch.Elapsed:mm\\:ss}");

// The TRX the CI reporter reads. A supervised run produces none on its own — each worker executes
// a slice and none of them knows the whole run — so the runner writes it, in the same place and
// with the same name the single-process path uses, so dorny/test-reporter's existing
// **/TestResults/*.trx glob picks it up unchanged.
var trxPath = ArgValue("--report-trx-path")
              ?? Path.Combine(Path.GetDirectoryName(Path.GetFullPath(executable))!,
                  "TestResults", "test-results.trx");

TrxWriter.Write(results, trxPath, Path.GetFullPath(executable), startedAt);

// Verified rather than assumed: an unreadable or short TRX makes the reporter say nothing at all,
// so the check belongs here, where it can fail the run that caused it.
TrxWriter.VerifyRoundTrip(trxPath, results.Tests.Count);

Console.WriteLine($"TRX written: {trxPath} ({results.Tests.Count} result(s))");

// The supervisor's own verdict, not a recount: it already knows what an indeterminate, a
// stall-managed outcome and a spent retry budget each mean for the run.
return results.ExitCode;

string? ArgValue(string name)
{
    var all = Environment.GetCommandLineArgs();
    var index = Array.IndexOf(all, name);
    return index >= 0 && index + 1 < all.Length ? all[index + 1] : null;
}

static string MasterConnectionString() =>
    Environment.GetEnvironmentVariable("POLECAT_TESTING_DATABASE")
    ?? "Server=localhost,11433;User Id=sa;Password=P@55w0rd;Timeout=5;MultipleActiveResultSets=True;Initial Catalog=master;Encrypt=False";

static string LaneConnectionString(int lane) =>
    new SqlConnectionStringBuilder(MasterConnectionString()) { InitialCatalog = $"polecat_w{lane}" }
        .ConnectionString;

// One catalog per lane, created up front against master. CREATE DATABASE cannot run inside the
// per-lane connection it is creating the target of, and a worker must not race another worker to
// create its own — so this happens once, here, before any worker launches.
static void EnsureLaneDatabases(int workers)
{
    var master = new SqlConnectionStringBuilder(MasterConnectionString()) { InitialCatalog = "master" }
        .ConnectionString;

    using var connection = new SqlConnection(master);
    connection.Open();

    for (var lane = 0; lane < workers; lane++)
    {
        var name = $"polecat_w{lane}";

        // Dropped and recreated rather than reused: a catalog carrying the previous run's tables is
        // the FALSE-GREEN TRAP in CLAUDE.md — a schema-shape change passes locally against stale
        // tables and fails on CI's fresh database. A per-lane catalog is throwaway by construction,
        // so there is no reason to inherit that hazard here.
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            IF DB_ID('{name}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{name}];
            END
            CREATE DATABASE [{name}];
            """;
        command.CommandTimeout = 120;
        command.ExecuteNonQuery();

        Console.WriteLine($"  lane {lane} -> {name}");
    }
}

/// <summary>
///     A failure gets one retry in a FRESH process, within the budget. Fresh rather than warm
///     because a test that fails after polluting its process cannot be re-run in it — every attempt
///     needs the full reset bracket, and out here the process IS the bracket. A pass-on-retry is
///     never folded into a clean pass; it lands in the flaky ledger the report prints.
/// </summary>
file sealed class RetryFailuresInFreshProcess : IFailurePolicy
{
    public Disposition? Decide(AttemptContext attempt)
    {
        if (attempt.Succeeded || !attempt.RetriesAvailable) return null;

        return Disposition.RetryInFreshProcess(
            "a failure is retried in a fresh process, within the budget, to separate flaky from broken");
    }
}
