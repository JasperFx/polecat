using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Bobcat.Ledger;
using Bobcat.Resilience;
using Bobcat.Supervisor;
using Microsoft.Data.SqlClient;
using Nuke.Common;
using Nuke.Common.IO;
using Nuke.Common.Tools.DotNet;
using Serilog;
using static Nuke.Common.Tools.DotNet.DotNetTasks;

// The supervised test runner: every CI test job runs its test project through Bobcat's Supervisor
// (https://github.com/JasperFx/bobcat) instead of invoking the test host once. JasperFx/bobcat#565,
// following Wolverine's rollout (bobcat's design/wolverine-ci-rollout.md) — every lesson there was
// paid for, and each one is applied below where it bites.
//
//   - Worker processes, partitioned by test CLASS. xUnit's isolation contract is per class, so a
//     class's fixtures and static state assume one process; the supervisor never splits one.
//   - A catalog per lane, and the lanes are the SUITE's. Polecat.Tests isolates by DatabaseSchemaName
//     inside ONE catalog, which separates classes from each other but not two processes from each
//     other — so each worker is pointed at its own catalog through POLECAT_TESTING_DATABASE
//     (Polecat.TestUtils/ConnectionSource). Anything a test creates at SERVER scope goes through
//     ConnectionSource.Scoped(), which carries the lane's catalog name, so sibling databases do not
//     collide either. Each suite's catalogs carry its own prefix (polecat_core_w0, polecat_aspnet_w0,
//     polecat_efcore_w0) and a suite drops only its own, so the three suites can run at the same time
//     against one SQL Server — which is what the LocalGate target (build/LocalGate.cs) does.
//   - Retries are opt-in, per test. A failure is reported as a failure unless that test says, in the
//     code, [Trait("Retry", "2")] — and then it gets one more attempt in a FRESH process, because the
//     process is the only reset bracket an xUnit test has. That is Bobcat's DefaultFailurePolicy
//     since 1.0 (bobcat#661), so no policy of our own is registered. A pass on retry is never folded
//     into a clean pass: it is the flaky ledger.
//   - Stalls and the heartbeat are reported, never acted on.
//   - The ledger balances lanes by measured duration. It is a CI artifact, not a committed file:
//     each run downloads the last green main run's, balances on it, folds its own results in and
//     uploads it again (test-template.yml). Bobcat 1.0 keys it by display name (bobcat#730).
//
// The workflow runs `./build.sh CIPolecat --framework net9.0 --storage default` (or edge) and owns
// nothing but the services: SQL Server comes from docker compose (docker-compose.yml for default,
// the standalone docker-compose.edge.yml for edge).
partial class Build
{
    /// <summary>
    ///     Overrides a target's committed worker count (<c>--test-workers 4</c>) AND bypasses the
    ///     <c>ProcessorCount / 2</c> clamp. Measuring past the ceiling is a legitimate thing to ask
    ///     for; the clamp exists for the run nobody configured.
    /// </summary>
    [Parameter("Worker processes for a supervised run; overrides the committed count and the ProcessorCount/2 clamp")]
    readonly int? TestWorkers;

    /// <summary>
    ///     Turns off even opted-in retries (<c>--disable-test-retry</c>). Measure with this first: a
    ///     green run at a new fleet size is a stronger claim without a retry budget.
    /// </summary>
    [Parameter("Disable the opt-in retries entirely")]
    readonly bool DisableTestRetry;

    /// <summary>
    ///     A substring of a test's display name, for running one class or test through the supervisor.
    ///     A filter that matches NOTHING fails the run: an emptied filter after a rename must never
    ///     read as a green job.
    /// </summary>
    [Parameter("Run only tests whose display name contains this text; matching nothing fails")]
    readonly string TestFilter;

    /// <summary>
    ///     Which SQL Server the run is against — the workflow's <c>storage_type</c> matrix axis
    ///     (<c>default</c> = SQL Server 2025 on 11433 from docker-compose.yml, <c>edge</c> = SQL Server
    ///     2022 on 11435 from docker-compose.edge.yml). It picks the server (<c>--connection-string</c>
    ///     or <c>--edge-connection-string</c>, unless POLECAT_TESTING_DATABASE names one) and changes
    ///     nothing about how the tests run. It also keys the ledger, the run report and the TRX, because
    ///     the two servers cost very differently per test and a balancer fed one's durations would plan
    ///     the other's lanes badly, and because a default and an edge run must not overwrite each other.
    /// </summary>
    [Parameter("The SQL Server the run is against (default | edge); picks the server and keys the ledger, report and TRX")]
    readonly string Storage = "default";

    [Parameter("SQL Server connection string for --storage edge (docker-compose.edge.yml).")]
    readonly string EdgeConnectionString =
        "Server=localhost,11435;User Id=sa;Password=P@55w0rd;Timeout=5;MultipleActiveResultSets=True;Initial Catalog=master;Encrypt=False";

    /// <summary>
    ///     The caller's promise that the test hosts are already built for this configuration and
    ///     framework (<c>--skip-build</c>), so a shared Release build is reused instead of rebuilt per
    ///     target. A missing host is still reported, with the path it was looked for at.
    /// </summary>
    [Parameter("Reuse the test hosts already built for this configuration and framework; build nothing")]
    readonly bool SkipBuild;

    /// <summary>
    ///     Two attempts, and only for tests that opted in. That is the previous supervised harness's
    ///     REAL ceiling (MaxAttemptsPerTest = 2, wolverine-ci-rollout lesson 8) — the difference is
    ///     that it used to spend it on every failure, and now spends it only where a test says it is
    ///     known to be flaky.
    /// </summary>
    const int MaxAttemptsPerTest = 2;

    const int MaxRetriesPerRun = 25;

    static AbsolutePath LedgerOutput => RootDirectory / "artifacts" / "test-ledger";
    static AbsolutePath ReportOutput => RootDirectory / "artifacts" / "test-reports";
    static AbsolutePath PreviousLedgers => RootDirectory / "previous-ledger";

    // ─── CI targets: one per workflow ──────────────────────────────────

    /// <summary>
    ///     One supervised test project: where it is, its committed worker count, and the prefix of its
    ///     lane catalogs (<c>polecat_{Lanes}_w{lane}</c>). The prefix is what lets the three suites run
    ///     at the same time on one SQL Server: each one drops and recreates only its own catalogs.
    /// </summary>
    sealed record SupervisedSuite(string Lanes, string ProjectPath, int Workers)
    {
        public string Name => Path.GetFileNameWithoutExtension(ProjectPath);
    }

    /// <summary>
    ///     Polecat.Tests — ~3,400 tests and the critical path of every PR by an order of magnitude.
    ///     Four workers is the committed count; the clamp takes it to two on a 4-vCPU runner unless
    ///     the workflow says otherwise (it does — see polecat.yml for the evidence it says it on).
    /// </summary>
    static readonly SupervisedSuite PolecatTests = new("core", "src/Polecat.Tests/Polecat.Tests.csproj", Workers: 4);

    /// <summary>Two minutes end to end; one worker is parity, and more would buy nothing.</summary>
    static readonly SupervisedSuite AspNetCoreTests =
        new("aspnet", "src/Polecat.AspNetCore.Testing/Polecat.AspNetCore.Testing.csproj", Workers: 1);

    /// <summary>Two minutes end to end; one worker is parity, and more would buy nothing.</summary>
    static readonly SupervisedSuite EfCoreTests =
        new("efcore", "src/Polecat.EntityFrameworkCore.Tests/Polecat.EntityFrameworkCore.Tests.csproj", Workers: 1);

    /// <summary>Every supervised suite, longest first.</summary>
    static readonly SupervisedSuite[] Suites = [PolecatTests, AspNetCoreTests, EfCoreTests];

    Target CIPolecat => _ => _
        .Executes(() => RunSupervised(PolecatTests));

    Target CIAspNetCore => _ => _
        .Executes(() => RunSupervised(AspNetCoreTests));

    Target CIEfCore => _ => _
        .Executes(() => RunSupervised(EfCoreTests));

    /// <summary>
    ///     Every supervised suite, one after another — what the three workflows run between them.
    ///     LocalGate runs the same three at once.
    /// </summary>
    Target CI => _ => _
        .DependsOn(CIPolecat, CIAspNetCore, CIEfCore);

    // ─── The run ───────────────────────────────────────────────────────

    void RunSupervised(SupervisedSuite suite)
    {
        var workers = suite.Workers;

        // A committed count is tuned on a developer machine; the machine running decides what it can
        // carry (wolverine-ci-rollout lesson 3). Several test hosts beside a database can kill a
        // 4-vCPU hosted runner outright — "The runner has received a shutdown signal" is the runner
        // dying, not a test — and SQL Server is memory-hungry. An explicit --test-workers bypasses it.
        var ceiling = Math.Max(1, Environment.ProcessorCount / 2);
        if (TestWorkers is { } asked)
        {
            workers = asked;
        }
        else if (workers > ceiling)
        {
            Log.Information("Clamping {Asked} workers to {Ceiling} for this machine's {Cores} core(s)",
                workers, ceiling, Environment.ProcessorCount);
            workers = ceiling;
        }

        var framework = string.IsNullOrEmpty(Framework) ? "net9.0" : Framework;
        var projectName = suite.Name;

        if (SkipBuild)
        {
            Log.Information("--skip-build: reusing the {Configuration} {Framework} host of {Project}", Configuration, framework, projectName);
        }
        else
        {
            DotNetBuild(s => s
                .SetProjectFile(suite.ProjectPath)
                .SetConfiguration(Configuration)
                .SetFramework(framework));
        }

        var executable = testHostFor(suite.ProjectPath, framework);

        Log.Information("=== {Project} ({Framework}, {Storage}): supervised run, {Workers} worker(s){Retry} ===",
            projectName, framework, Storage, workers, DisableTestRetry ? ", retries OFF" : "");

        ensureLaneDatabases(suite, workers);

        var ledger = loadLedger(ledgerNameFor(suite));
        var supervisor = new Supervisor(workerFactoryFor(suite, executable))
        {
            MaxParallelWorkers = workers,
            Log = message => Log.Information("  {Message}", message)
        };
        configureSupervisor(supervisor, ledger);

        Log.Information("  ledger: {Count} measured duration(s) for {Name}", supervisor.KnownTestDurations?.Count ?? 0, ledgerNameFor(suite));

        var startedAt = DateTime.UtcNow;
        var stopwatch = Stopwatch.StartNew();
        var results = supervisor.Run().GetAwaiter().GetResult();
        stopwatch.Stop();

        // A filter that matched nothing is a renamed class, not a pass.
        if (!string.IsNullOrEmpty(TestFilter) && results.Tests.Count == 0)
        {
            throw new InvalidOperationException(
                $"--test-filter '{TestFilter}' matched no test in {projectName}. Refusing to report an empty run as a pass.");
        }

        var recorded = recordRun(suite, framework, executable, results, startedAt, ledger);

        report(projectName, framework, workers, stopwatch.Elapsed, results, recorded);

        if (results.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"{projectName} ({framework}, {Storage}): {results.Summarize()} — exit code {results.ExitCode}");
        }
    }

    /// <summary>
    ///     Everything a suite's supervisor is told beyond its workers and its log — the same for a CI
    ///     target and for the suite's unit in LocalGate, so a gate run is the CI run, only overlapped.
    /// </summary>
    void configureSupervisor(Supervisor supervisor, TestLedger ledger)
    {
        var known = ledger.KnownDurations();
        supervisor.KnownTestDurations = known.Count > 0 ? known : null;

        supervisor.TestFilter = string.IsNullOrEmpty(TestFilter)
            ? null
            : test => test.DisplayName.Contains(TestFilter, StringComparison.OrdinalIgnoreCase);

        // Opt-in retries only: Bobcat's DefaultFailurePolicy retries a test that says
        // [Trait("Retry", "2")], once, in a fresh process. The budget is the build's.
        supervisor.RetryBudget = DisableTestRetry
            ? RetryBudget.None
            : new RetryBudget { MaxAttemptsPerTest = MaxAttemptsPerTest, MaxRetriesPerRun = MaxRetriesPerRun };

        // A fresh-process retry with every lane still resident is workers + 1 hosts at once; that
        // OOM-killed 16GB runners twice in Wolverine's rollout, both times during a retry.
        supervisor.ReleaseIdleLanes = true;

        // Report-only, all of them: a test in flight past five minutes is named as it happens, a
        // progress line every 30s says where a wedged run stopped, and worker RSS is sampled so the
        // worker count is a measured decision rather than a hope.
        supervisor.StallThreshold = TimeSpan.FromMinutes(5);
        supervisor.StallAction = StallAction.Report;
        supervisor.HeartbeatInterval = TimeSpan.FromSeconds(30);
        supervisor.ResourceSampleInterval = TimeSpan.FromSeconds(15);
    }

    /// <summary>
    ///     The suite's MTP host, each worker pointed at its own lane catalog. Discovery and any
    ///     isolated or fresh-process retry report lane 0, so the catalogs provisioned equal the workers
    ///     asked for rather than the processes launched.
    /// </summary>
    MtpWorkerFactory workerFactoryFor(SupervisedSuite suite, string executable) => new(executable)
    {
        EnvironmentFor = context => new Dictionary<string, string>
        {
            ["POLECAT_TESTING_DATABASE"] = laneConnectionString(suite, context.Lane)
        }
    };

    string ledgerNameFor(SupervisedSuite suite) => $"{suite.Name}.{Storage}.json";

    /// <summary>
    ///     What a finished run leaves behind: the ledger with this run folded in, the TRX the CI
    ///     reporter reads, and the supervisor's JSON report. Written even for a red run — a failing test
    ///     still measured something, and the ledger's flakiness evidence is exactly the runs that did
    ///     not go cleanly.
    /// </summary>
    TestLedger recordRun(SupervisedSuite suite, string framework, string executable, SupervisorResults results,
        DateTime startedAt, TestLedger ledger)
    {
        var runId = Environment.GetEnvironmentVariable("GITHUB_RUN_ID") is { Length: > 0 } ghRun
            ? $"{ghRun}.{Environment.GetEnvironmentVariable("GITHUB_RUN_ATTEMPT") ?? "1"}"
            : Guid.NewGuid().ToString("N");

        var recorded = ledger.Record(SupervisorLedger.From(results, runId, DateTimeOffset.UtcNow));
        recorded.Save(LedgerOutput / ledgerNameFor(suite));

        // The TRX dorny/test-reporter reads, where its **/TestResults/*.trx glob already looks. A
        // supervised run produces none on its own — no single worker knows the whole run — so it is
        // Bobcat's RunReport.WriteTrx (bobcat#699), which replaced build/TrxWriter.cs. The file name
        // carries the storage leg, so a default and an edge run of one build do not overwrite each
        // other; the framework is already in the directory.
        var trxPath = Path.Combine(Path.GetDirectoryName(executable)!, "TestResults", $"test-results.{Storage}.trx");
        RunReport.WriteTrx(results, trxPath, new TrxOptions { StartedAt = startedAt, Storage = executable });
        verifyTrx(trxPath, results.Tests.Count);

        // The supervisor's own JSON report, uploaded beside the ledger: the primary record of the run,
        // where the ledger is a compaction of it.
        Directory.CreateDirectory(ReportOutput);
        File.WriteAllText(ReportOutput / $"{suite.Name}.{framework}.{Storage}.report.json", RunReport.ToJson(results));

        return recorded;
    }

    /// <summary>
    ///     The last green main run's ledger, when the workflow fetched it (test-template.yml). Advisory:
    ///     an absent or unreadable ledger degrades lane balancing to test counts and never fails a run.
    /// </summary>
    /// <remarks>
    ///     Bobcat keeps the newest <see cref="TestLedger.DefaultMaxRunsPerTest" /> runs per test. A
    ///     ledger written by Bobcat 0.60 (version 1, keyed by uid) is re-keyed by display name on load.
    /// </remarks>
    TestLedger loadLedger(string name)
    {
        var ledger = TestLedger.Empty();

        var candidates = Directory.Exists(PreviousLedgers)
            ? Directory.EnumerateFiles(PreviousLedgers, name, SearchOption.AllDirectories)
            : [];

        foreach (var candidate in candidates)
        {
            try
            {
                ledger = ledger.Merge(TestLedger.Load(candidate));
            }
            catch (Exception e)
            {
                Log.Warning("Could not read ledger {File} ({Message}); balancing without it", candidate, e.Message);
            }
        }

        return ledger;
    }

    // ─── Reporting ─────────────────────────────────────────────────────

    void report(string projectName, string framework, int workers, TimeSpan wallClock, SupervisorResults results,
        TestLedger ledger)
    {
        Console.WriteLine(RunReport.ToText(results));

        var memory = RunResources.For(results);
        var peak = memory.IsMeasured && memory.PeakBytes is { } bytes ? RunResources.Humanize(bytes) : "not measured";

        Log.Information("=== {Project} ({Storage}): {Summary} — {Workers} worker(s), wall clock {Wall:mm\\:ss}, peak worker RSS {Peak} ===",
            projectName, Storage, results.Summarize(), workers, wallClock, peak);

        foreach (var flaky in results.PassedOnRetry)
            Log.Warning("  [FLAKY] {Test} — passed on attempt {Attempts}", flaky.DisplayName, flaky.AttemptCount);
        foreach (var fault in results.WorkerFaults)
            Log.Error("  [WORKER FAULT] {Fault}", fault);
        foreach (var stalled in results.StalledTests)
            Log.Warning("  [STALLED] {Test} — {Seconds}s in flight on lane {Lane}",
                stalled.DisplayName, (int)stalled.InFlight.TotalSeconds, stalled.Worker.Lane);
        foreach (var test in results.Indeterminate)
            Log.Error("  [INDETERMINATE] {Test} — {Error}", test.DisplayName, test.Final.Outcome.ErrorMessage);
        foreach (var test in results.Failed)
            Log.Error("  [FAILED] {Test} — {Error}", test.DisplayName, test.Final.Outcome.ErrorMessage);

        // The ledger's own reading of its history: hints it would propose and tests getting slower.
        // Proposals, never policy — a human accepts one by writing it into the test.
        var proposals = ledger.ProposeHints();
        foreach (var proposal in proposals)
            Log.Information("  [HINT PROPOSAL] {Suggestion} — {Tests}", proposal.Suggestion, string.Join(", ", proposal.Tests));

        if (Environment.GetEnvironmentVariable("GITHUB_ACTIONS") != "true") return;

        foreach (var flaky in results.PassedOnRetry)
            Console.WriteLine($"::warning title={projectName} ({Storage}) flaky::{flaky.DisplayName} passed on attempt {flaky.AttemptCount}");
        foreach (var stalled in results.StalledTests.DistinctBy(x => x.DisplayName))
            Console.WriteLine($"::warning title={projectName} ({Storage}) stalled::{stalled.DisplayName} was in flight {(int)stalled.InFlight.TotalSeconds}s");

        var summaryPath = Environment.GetEnvironmentVariable("GITHUB_STEP_SUMMARY");
        if (string.IsNullOrEmpty(summaryPath)) return;

        try
        {
            var md = new StringBuilder();
            md.AppendLine($"## {projectName} ({framework}, {Storage}) — supervised");
            md.AppendLine();
            md.AppendLine("| Tests | Clean | Skipped | Passed on retry | Failed | Indeterminate | Worker faults | Stalled | Workers | Wall clock | Peak worker RSS |");
            md.AppendLine("|---|---|---|---|---|---|---|---|---|---|---|");
            md.AppendLine($"| {results.Tests.Count} | {results.CleanPasses.Count} | {results.Skipped.Count} | {results.PassedOnRetry.Count} | {results.Failed.Count} | " +
                          $"{results.Indeterminate.Count} | {results.WorkerFaults.Count} | {results.StalledTests.Select(x => x.DisplayName).Distinct().Count()} | " +
                          $"{workers} | {wallClock:mm\\:ss} | {peak} |");
            md.AppendLine();

            void list(string heading, IEnumerable<string> lines)
            {
                var items = lines.ToList();
                if (items.Count == 0) return;
                md.AppendLine($"**{heading}**");
                md.AppendLine();
                foreach (var line in items) md.AppendLine($"- {line}");
                md.AppendLine();
            }

            list("Flaky (passed on retry)", results.PassedOnRetry.Select(x => $"`{x.DisplayName}` — attempt {x.AttemptCount}"));
            list("Failed", results.Failed.Select(x => $"`{x.DisplayName}`"));
            list("Indeterminate", results.Indeterminate.Select(x => $"`{x.DisplayName}`"));
            list("Worker faults", results.WorkerFaults.Select(x => x.ToString()));
            list("Stalled (in flight past 5 minutes; reported, not acted on)",
                results.StalledTests.DistinctBy(x => x.DisplayName).Select(x => $"`{x.DisplayName}` — {(int)x.InFlight.TotalSeconds}s, lane {x.Worker.Lane}"));
            list("Ledger hint proposals", proposals.Select(x => $"{x.Suggestion} — {string.Join(", ", x.Tests)}"));

            md.AppendLine("<details><summary>Run report</summary>");
            md.AppendLine();
            md.AppendLine("```");
            md.AppendLine(RunReport.ToText(results));
            md.AppendLine("```");
            md.AppendLine("</details>");
            md.AppendLine();

            File.AppendAllText(summaryPath, md.ToString());
        }
        catch (Exception e)
        {
            // Reporting about the tests must never fail the tests.
            Log.Warning("Could not write the step summary: {Message}", e.Message);
        }
    }

    // ─── Test host and per-lane catalogs ───────────────────────────────

    string testHostFor(string projectPath, string framework)
    {
        var projectName = Path.GetFileNameWithoutExtension(projectPath);
        var host = (AbsolutePath)Path.GetDirectoryName(Path.GetFullPath(projectPath)) / "bin" / Configuration / framework / projectName;
        string executable = OperatingSystem.IsWindows() ? (string)host + ".exe" : (string)host;

        if (!File.Exists(executable))
        {
            throw new InvalidOperationException(
                $"{executable} does not exist " +
                (SkipBuild ? "and --skip-build was given, so nothing built it. " : $"after building {projectPath} for {framework}. ") +
                "The test host is the " +
                "xUnit v3 Microsoft.Testing.Platform executable (OutputType=Exe + UseMicrosoftTestingPlatformRunner).");
        }

        return executable;
    }

    /// <summary>
    ///     The server-level connection the lane catalogs are created through: POLECAT_TESTING_DATABASE
    ///     when CI (or a developer) set one, else <c>--connection-string</c> for the default storage and
    ///     <c>--edge-connection-string</c> for <c>--storage edge</c>.
    /// </summary>
    string serverConnectionString()
    {
        var configured = Environment.GetEnvironmentVariable("POLECAT_TESTING_DATABASE") is { Length: > 0 } fromEnvironment
            ? fromEnvironment
            : Storage switch
            {
                "default" => ConnectionString,
                "edge" => EdgeConnectionString,
                _ => throw new ArgumentException($"--storage '{Storage}' is neither 'default' nor 'edge'.")
            };

        return new SqlConnectionStringBuilder(configured) { InitialCatalog = "master" }.ConnectionString;
    }

    string laneConnectionString(SupervisedSuite suite, int lane) =>
        new SqlConnectionStringBuilder(serverConnectionString()) { InitialCatalog = laneDatabase(suite, lane) }.ConnectionString;

    static string laneDatabase(SupervisedSuite suite, int lane) => $"polecat_{suite.Lanes}_w{lane}";

    /// <summary>
    ///     One catalog per lane, created before any worker launches — a worker must not race another to
    ///     create its own, and CREATE DATABASE cannot run inside the connection it is the target of.
    /// </summary>
    /// <remarks>
    ///     Dropped and recreated rather than reused: a catalog holding the previous run's tables is the
    ///     false-green trap (a schema-shape change passes against stale tables locally and fails on CI's
    ///     fresh server). The lane's sibling databases — what <c>ConnectionSource.Scoped()</c> names,
    ///     <c>polecat_core_w0_tenant_a</c> — are dropped with it for the same reason.
    ///     <para>
    ///     Only THIS suite's catalogs: every name here starts <c>polecat_{suite}_w</c>, so another
    ///     suite running at the same time on the same server (LocalGate) never loses its lanes.
    ///     </para>
    /// </remarks>
    void ensureLaneDatabases(SupervisedSuite suite, int workers)
    {
        // LocalGate starts units concurrently; CREATE DATABASE copies `model` under an exclusive
        // lock, so the catalogs are provisioned one suite at a time.
        lock (LaneProvisioning) provisionLanes(suite, workers);
    }

    static readonly object LaneProvisioning = new();

    void provisionLanes(SupervisedSuite suite, int workers)
    {
        using var connection = new SqlConnection(serverConnectionString());
        connection.Open();

        for (var lane = 0; lane < workers; lane++)
        {
            var name = laneDatabase(suite, lane);
            recreateDatabase(connection, name);
            Log.Information("  {Suite} lane {Lane} -> {Database}", suite.Name, lane, name);
        }
    }

    /// <summary>
    ///     Drops <paramref name="name"/> and every <c>{name}_*</c> sibling, then creates it empty. The
    ///     sibling match escapes <c>_</c>, which LIKE otherwise reads as "any one character".
    /// </summary>
    static void recreateDatabase(SqlConnection connection, string name)
    {
        var siblings = name.Replace("_", "[_]") + "[_]%";

        // Identifiers, not values — they cannot be parameterized. Generated in this build, never input.
        using var command = connection.CreateCommand();
        command.CommandTimeout = 300;
        command.CommandText = $"""
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql += N'ALTER DATABASE ' + QUOTENAME(name) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ' + QUOTENAME(name) + N';'
            FROM sys.databases
            WHERE name = N'{name}' OR name LIKE N'{siblings}';
            EXEC sp_executesql @sql;
            CREATE DATABASE [{name}];
            """;
        command.ExecuteNonQuery();
    }

    /// <summary>
    ///     Re-reads the TRX just written and checks it holds one result per test.
    /// </summary>
    /// <remarks>
    ///     A short or malformed TRX is silent at dorny/test-reporter (it reports nothing and the job
    ///     looks fine), so the run that wrote it fails here instead. Bobcat writes one top-level
    ///     <c>UnitTestResult</c> per test and nests a retried test's attempts as <c>InnerResults</c>,
    ///     which are not counted.
    /// </remarks>
    static void verifyTrx(string path, int expectedTests)
    {
        XNamespace ns = "http://microsoft.com/schemas/VisualStudio/TeamTest/2010";
        var written = XDocument.Load(path).Root!.Element(ns + "Results")!.Elements(ns + "UnitTestResult").Count();

        if (written != expectedTests)
        {
            throw new InvalidOperationException(
                $"TRX check failed: {written} result(s) for {expectedTests} test(s) at '{path}'. "
                + "The CI reporter reads this file; a short or malformed one reports nothing rather than failing.");
        }
    }
}
