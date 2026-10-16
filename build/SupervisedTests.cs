using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Bobcat.Ledger;
using Bobcat.Resilience;
using Bobcat.Supervisor;
using Microsoft.Data.SqlClient;
using Nuke.Common;
using Nuke.Common.IO;
using Nuke.Common.Tools.DotNet;
using Polecat.Build;
using Serilog;
using static Nuke.Common.Tools.DotNet.DotNetTasks;

// The supervised test runner: every CI test job runs its test project through Bobcat's Supervisor
// (https://github.com/JasperFx/bobcat) instead of invoking the test host once. JasperFx/bobcat#565,
// following Wolverine's rollout (bobcat's design/wolverine-ci-rollout.md) — every lesson there was
// paid for, and each one is applied below where it bites.
//
//   - Worker processes, partitioned by test CLASS. xUnit's isolation contract is per class, so a
//     class's fixtures and static state assume one process; the supervisor never splits one.
//   - A catalog per lane. Polecat.Tests isolates by DatabaseSchemaName inside ONE catalog, which
//     separates classes from each other but not two processes from each other — so each worker is
//     pointed at its own catalog through POLECAT_TESTING_DATABASE (Polecat.TestUtils/ConnectionSource).
//     Anything a test creates at SERVER scope goes through ConnectionSource.Scoped(), which carries
//     the lane's catalog name, so sibling databases do not collide either.
//   - Retries are opt-in, per test. A failure is reported as a failure unless that test says, in the
//     code, [Trait("Retry", "2")] — and then it gets one more attempt in a FRESH process, because the
//     process is the only reset bracket an xUnit test has. A pass on retry is never folded into a
//     clean pass: it is the flaky ledger.
//   - Stalls and the heartbeat are reported, never acted on.
//   - The committed ledger (build/test-ledger/) balances lanes by measured duration from the first
//     pass; each run's observations are uploaded so `UpdateTestLedgers` can fold them back in.
//
// The workflow runs `./build.sh CIPolecat --framework net9.0 --storage default` (or edge) and owns
// nothing but the services: SQL Server comes from docker compose exactly as before.
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
    ///     (<c>default</c> = SQL Server 2025, <c>edge</c> = 2022). It changes nothing about how the
    ///     tests run; it keys the ledger, because the two servers cost very differently per test and a
    ///     balancer fed one's durations would plan the other's lanes badly.
    /// </summary>
    [Parameter("The SQL Server the run is against (default | edge); keys the test ledger")]
    readonly string Storage = "default";

    /// <summary>
    ///     Two attempts, and only for tests that opted in. That is the previous supervised harness's
    ///     REAL ceiling (MaxAttemptsPerTest = 2, wolverine-ci-rollout lesson 8) — the difference is
    ///     that it used to spend it on every failure, and now spends it only where a test says it is
    ///     known to be flaky.
    /// </summary>
    const int MaxAttemptsPerTest = 2;

    const int MaxRetriesPerRun = 25;

    /// <summary>
    ///     Observations kept per test. Small because the file is committed: a ledger observation is
    ///     ~560 bytes of JSON, so Polecat.Tests' ~3,400 tests cost ~1.9 MB per retained run, per server.
    ///     Three is enough for a median.
    /// </summary>
    const int LedgerRunsPerTest = 3;

    static AbsolutePath CommittedLedgers => RootDirectory / "build" / "test-ledger";
    static AbsolutePath LedgerOutput => RootDirectory / "artifacts" / "test-ledger";
    static AbsolutePath ReportOutput => RootDirectory / "artifacts" / "test-reports";
    static AbsolutePath PreviousLedgers => RootDirectory / "previous-ledger";

    // ─── CI targets: one per workflow ──────────────────────────────────

    /// <summary>
    ///     Polecat.Tests — ~3,400 tests and the critical path of every PR by an order of magnitude.
    ///     Four workers is the committed count; the clamp takes it to two on a 4-vCPU runner unless
    ///     the workflow says otherwise (it does — see polecat.yml for the evidence it says it on).
    /// </summary>
    Target CIPolecat => _ => _
        .Executes(() => RunSupervised("src/Polecat.Tests/Polecat.Tests.csproj", workers: 4));

    /// <summary>Two minutes end to end; one worker is parity, and more would buy nothing.</summary>
    Target CIAspNetCore => _ => _
        .Executes(() => RunSupervised("src/Polecat.AspNetCore.Testing/Polecat.AspNetCore.Testing.csproj", workers: 1));

    /// <summary>Two minutes end to end; one worker is parity, and more would buy nothing.</summary>
    Target CIEfCore => _ => _
        .Executes(() => RunSupervised("src/Polecat.EntityFrameworkCore.Tests/Polecat.EntityFrameworkCore.Tests.csproj", workers: 1));

    /// <summary>Every supervised suite, one after another — what the three workflows run between them.</summary>
    Target CI => _ => _
        .DependsOn(CIPolecat, CIAspNetCore, CIEfCore);

    /// <summary>
    ///     Folds run ledgers into the committed ones under build/test-ledger/. The source is
    ///     <c>--ledger-source &lt;dir&gt;</c> (default: artifacts/test-ledger, what a local run writes);
    ///     for CI's, download the <c>test-ledger-*</c> artifacts of a green main run into a directory
    ///     first (<c>gh run download &lt;id&gt; --pattern 'test-ledger-*' --dir ledgers</c>).
    /// </summary>
    /// <remarks>
    ///     A merge, never an overwrite: <see cref="TestLedger.Merge" /> is commutative and idempotent, so
    ///     folding the same run twice changes nothing and a git conflict on the file is resolved by
    ///     re-running this against both sides. Tests unseen for 60 days are pruned — renamed or deleted.
    /// </remarks>
    Target UpdateTestLedgers => _ => _
        .Executes(() =>
        {
            var source = string.IsNullOrEmpty(LedgerSource) ? LedgerOutput : (AbsolutePath)Path.GetFullPath(LedgerSource);
            var files = Directory.Exists(source)
                ? Directory.EnumerateFiles(source, "*.json", SearchOption.AllDirectories)
                    .Where(x => !x.EndsWith(".report.json", StringComparison.Ordinal))
                    .ToList()
                : [];

            if (files.Count == 0) throw new InvalidOperationException($"No ledger files under {source}.");

            var cutoff = DateTimeOffset.UtcNow.AddDays(-60);
            foreach (var file in files)
            {
                var committed = CommittedLedgers / Path.GetFileName(file);
                var merged = TestLedger.Load(committed, LedgerRunsPerTest)
                    .Merge(TestLedger.Load(file, LedgerRunsPerTest))
                    .PruneTestsNotSeenSince(cutoff);

                merged.Save(committed);
                Log.Information("Ledger {File}: {Tests} test(s)", committed, merged.Tests.Count);
            }
        });

    [Parameter("Directory of ledger files UpdateTestLedgers folds in (default: artifacts/test-ledger)")]
    readonly string LedgerSource;

    // ─── The run ───────────────────────────────────────────────────────

    void RunSupervised(string projectPath, int workers)
    {
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
        var projectName = Path.GetFileNameWithoutExtension(projectPath);

        DotNetBuild(s => s
            .SetProjectFile(projectPath)
            .SetConfiguration(Configuration)
            .SetFramework(framework));

        var executable = testHostFor(projectPath, framework);

        Log.Information("=== {Project} ({Framework}, {Storage}): supervised run, {Workers} worker(s){Retry} ===",
            projectName, framework, Storage, workers, DisableTestRetry ? ", retries OFF" : "");

        ensureLaneDatabases(workers);

        var factory = new MtpWorkerFactory(executable)
        {
            // Discovery and any isolated or fresh-process retry report lane 0, so the catalogs
            // provisioned equal the workers asked for rather than the processes launched.
            EnvironmentFor = context => new Dictionary<string, string>
            {
                ["POLECAT_TESTING_DATABASE"] = laneConnectionString(context.Lane)
            }
        };

        var ledgerName = $"{projectName}.{Storage}.json";
        var ledger = loadLedger(ledgerName);
        var known = ledger.KnownDurations();

        var supervisor = new Supervisor(factory)
        {
            MaxParallelWorkers = workers,
            KnownTestDurations = known.Count > 0 ? known : null,

            TestFilter = string.IsNullOrEmpty(TestFilter)
                ? null
                : test => test.DisplayName.Contains(TestFilter, StringComparison.OrdinalIgnoreCase),

            RetryBudget = DisableTestRetry
                ? RetryBudget.None
                : new RetryBudget { MaxAttemptsPerTest = MaxAttemptsPerTest, MaxRetriesPerRun = MaxRetriesPerRun },

            // A fresh-process retry with every lane still resident is workers + 1 hosts at once; that
            // OOM-killed 16GB runners twice in Wolverine's rollout, both times during a retry.
            ReleaseIdleLanes = true,

            // Report-only, all of them: a test in flight past five minutes is named as it happens, a
            // progress line every 30s says where a wedged run stopped, and worker RSS is sampled so the
            // worker count is a measured decision rather than a hope.
            StallThreshold = TimeSpan.FromMinutes(5),
            StallAction = StallAction.Report,
            HeartbeatInterval = TimeSpan.FromSeconds(30),
            ResourceSampleInterval = TimeSpan.FromSeconds(15),

            Log = message => Log.Information("  {Message}", message)
        };

        if (!DisableTestRetry) supervisor.AddFailurePolicy(new RetryOptedInTestsInFreshProcess());

        Log.Information("  ledger: {Count} measured duration(s) for {Name}", known.Count, ledgerName);

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

        var runId = Environment.GetEnvironmentVariable("GITHUB_RUN_ID") is { Length: > 0 } ghRun
            ? $"{ghRun}.{Environment.GetEnvironmentVariable("GITHUB_RUN_ATTEMPT") ?? "1"}"
            : Guid.NewGuid().ToString("N");

        // Written even for a red run: a failing test still measured something, and the ledger's
        // flakiness evidence is exactly the runs that did not go cleanly.
        var recorded = ledger.Record(SupervisorLedger.From(results, runId, DateTimeOffset.UtcNow));
        recorded.Save(LedgerOutput / ledgerName);

        // The TRX dorny/test-reporter reads, where its **/TestResults/*.trx glob already looks. A
        // supervised run produces none on its own — no single worker knows the whole run.
        var trxPath = Path.Combine(Path.GetDirectoryName(executable)!, "TestResults", "test-results.trx");
        TrxWriter.Write(results, trxPath, executable, startedAt);
        TrxWriter.VerifyRoundTrip(trxPath, results.Tests.Count);

        // The supervisor's own JSON report, uploaded beside the ledger: the primary record of the run,
        // where the ledger is a compaction of it.
        Directory.CreateDirectory(ReportOutput);
        File.WriteAllText(ReportOutput / $"{projectName}.{Storage}.report.json", RunReport.ToJson(results));

        report(projectName, framework, workers, stopwatch.Elapsed, results, recorded);

        if (results.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"{projectName} ({framework}, {Storage}): {results.Summarize()} — exit code {results.ExitCode}");
        }
    }

    /// <summary>
    ///     The committed ledger, merged with the last green main run's when the workflow fetched it.
    ///     Both are advisory: an absent or unreadable ledger degrades lane balancing to test counts
    ///     and never fails a run.
    /// </summary>
    TestLedger loadLedger(string name)
    {
        var ledger = TestLedger.Empty(LedgerRunsPerTest);

        foreach (var candidate in new[] { (string)(CommittedLedgers / name) }
                     .Concat(Directory.Exists(PreviousLedgers)
                         ? Directory.EnumerateFiles(PreviousLedgers, name, SearchOption.AllDirectories)
                         : []))
        {
            try
            {
                ledger = ledger.Merge(TestLedger.Load(candidate, LedgerRunsPerTest));
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
            md.AppendLine("| Tests | Clean | Passed on retry | Failed | Indeterminate | Worker faults | Stalled | Workers | Wall clock | Peak worker RSS |");
            md.AppendLine("|---|---|---|---|---|---|---|---|---|---|");
            md.AppendLine($"| {results.Tests.Count} | {results.CleanPasses.Count} | {results.PassedOnRetry.Count} | {results.Failed.Count} | " +
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
                $"{executable} does not exist after building {projectPath} for {framework}. The test host is the " +
                "xUnit v3 Microsoft.Testing.Platform executable (OutputType=Exe + UseMicrosoftTestingPlatformRunner).");
        }

        return executable;
    }

    /// <summary>
    ///     The server-level connection the lane catalogs are created through: POLECAT_TESTING_DATABASE
    ///     when CI (or a developer) set one, else the <c>--connection-string</c> parameter.
    /// </summary>
    string serverConnectionString() =>
        new SqlConnectionStringBuilder(Environment.GetEnvironmentVariable("POLECAT_TESTING_DATABASE") ?? ConnectionString)
        {
            InitialCatalog = "master"
        }.ConnectionString;

    string laneConnectionString(int lane) =>
        new SqlConnectionStringBuilder(serverConnectionString()) { InitialCatalog = laneDatabase(lane) }.ConnectionString;

    static string laneDatabase(int lane) => $"polecat_w{lane}";

    /// <summary>
    ///     One catalog per lane, created before any worker launches — a worker must not race another to
    ///     create its own, and CREATE DATABASE cannot run inside the connection it is the target of.
    /// </summary>
    /// <remarks>
    ///     Dropped and recreated rather than reused: a catalog holding the previous run's tables is the
    ///     false-green trap (a schema-shape change passes against stale tables locally and fails on CI's
    ///     fresh server). The lane's sibling databases — what <c>ConnectionSource.Scoped()</c> names,
    ///     <c>polecat_w0_tenant_a</c> — are dropped with it for the same reason.
    /// </remarks>
    void ensureLaneDatabases(int workers)
    {
        using var connection = new SqlConnection(serverConnectionString());
        connection.Open();

        for (var lane = 0; lane < workers; lane++)
        {
            var name = laneDatabase(lane);

            // Identifiers, not values — they cannot be parameterized. Generated above, never input.
            using var command = connection.CreateCommand();
            command.CommandTimeout = 300;
            command.CommandText = $"""
                DECLARE @sql nvarchar(max) = N'';
                SELECT @sql += N'ALTER DATABASE ' + QUOTENAME(name) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ' + QUOTENAME(name) + N';'
                FROM sys.databases
                WHERE name = N'{name}' OR name LIKE N'{name}[_]%';
                EXEC sp_executesql @sql;
                CREATE DATABASE [{name}];
                """;
            command.ExecuteNonQuery();

            Log.Information("  lane {Lane} -> {Database}", lane, name);
        }
    }

    /// <summary>
    ///     The retry policy: a failure is retried — once, alone, in a FRESH process — only when the
    ///     test opted in with <c>[Trait("Retry", "2")]</c>. Every other failure abstains here and falls
    ///     through to Bobcat's DefaultFailurePolicy, which fails it.
    /// </summary>
    /// <remarks>
    ///     Why not Bobcat's own <c>Retry</c> handling: DefaultFailurePolicy answers a Retry trait with
    ///     RetryInProcess, a warm retry in the process the failure just polluted. That cannot work for a
    ///     test whose failure leaves state behind (wolverine-ci-rollout lesson 5), and with
    ///     ReleaseIdleLanes the lane it would return to may be gone. Out here the process is the only
    ///     reset bracket an xUnit test has.
    /// </remarks>
    class RetryOptedInTestsInFreshProcess : IFailurePolicy
    {
        public Disposition Decide(AttemptContext attempt)
        {
            if (attempt.Succeeded || !attempt.RetriesAvailable) return null;
            if (!attempt.HasTrait(ResilienceTags.Retry)) return null;

            return Disposition.RetryInFreshProcess(
                "opted in with [Trait(\"Retry\", ...)] — retried alone in a fresh process; a pass is reported as flaky");
        }
    }
}
