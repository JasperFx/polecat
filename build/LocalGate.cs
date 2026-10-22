using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Bobcat.Ledger;
using Bobcat.Supervisor;
using Microsoft.Data.SqlClient;
using Nuke.Common;
using Nuke.Common.IO;
using Nuke.Common.Tooling;
using Nuke.Common.Tools.DotNet;
using Serilog;
using static Nuke.Common.Tools.DotNet.DotNetTasks;

// The local commit-and-push gate (JasperFx/polecat#797, on Bobcat's Gate — bobcat#729,
// https://github.com/JasperFx/bobcat/blob/main/docs/local-gates.md): one command that builds once and
// runs every supervised suite this repository's CI runs, at the same time, against the one SQL Server
// in docker-compose.yml.
//
//   ./build.sh LocalGate                        newest framework (net10.0), default storage
//   ./build.sh LocalGate --framework all        net10.0 and net9.0
//   ./build.sh LocalGate --aot                  plus aot-smoke and aot-runtime-smoke afterwards
//   ./build.sh LocalGate --storage edge         against SQL Server 2022 (docker-compose.edge.yml)
//
// Why there is no shared `sqlserver` track: a track is exclusive, so one held by all three suites
// would run them one after the other — exactly what `./build.sh CI` already does. What made them
// collide was NAMES, not the server: all three used to drop and recreate `polecat_w{lane}`. Each
// suite's catalogs now carry its own prefix (`polecat_core_w*`, `polecat_aspnet_w*`,
// `polecat_efcore_w*`, SupervisedTests.cs) and each suite drops only its own, and everything a test
// creates at server scope goes through ConnectionSource.Scoped(), which carries the lane's catalog.
// So the tracks declared below are per SUITE: a track is the suite's lane catalogs, which is the
// resource a unit really holds. The three suites overlap; two frameworks of ONE suite (with
// --framework all) share its catalogs and so run one after the other. Each track's preflight is that
// the SQL Server answers a query, not just that a port is open — under x64 emulation the port opens
// well before the server accepts logins.
partial class Build
{
    /// <summary>
    ///     Worker processes running at once across every unit of the gate. Bobcat's default is half
    ///     the machine's cores — 9 on an 18-core box, which fits all three suites (4 + 1 + 1) at once.
    /// </summary>
    [Parameter("LocalGate: worker processes across every unit at once (default ProcessorCount / 2)")]
    readonly int? WorkerBudget;

    /// <summary>
    ///     Also run <see cref="AotSmoke" /> and <see cref="AotRuntimeSmoke" /> as units of the gate,
    ///     after the test suites. Off by default: they build and natively publish rather than test, so
    ///     they are worth their ~minutes before a push that touches annotations or the AOT surface.
    /// </summary>
    [Parameter("LocalGate: also run the aot-smoke and aot-runtime-smoke units after the test suites")]
    readonly bool Aot;

    /// <summary>
    ///     The runtime identifier <see cref="AotRuntimeSmoke" /> publishes natively for. The host's own
    ///     by default (osx-arm64 on Apple silicon); CI passes linux-x64.
    /// </summary>
    [Parameter("AotRuntimeSmoke: runtime identifier to publish natively for (default: the host's, e.g. osx-arm64)")]
    readonly string AotRuntime;

    /// <summary>
    ///     Build once, then run Polecat.Tests (4 workers), Polecat.AspNetCore.Testing and
    ///     Polecat.EntityFrameworkCore.Tests as units of one Bobcat gate — longest first, concurrently,
    ///     on the newest framework unless <c>--framework all</c> (or a pinned one) — and report every
    ///     unit. Each unit is configured exactly as its CI target is (the opt-in retry policy, stall
    ///     reporting, ledger-balanced lanes) and leaves the same ledger, TRX and report behind.
    /// </summary>
    Target LocalGate => _ => _
        .Executes(async () =>
        {
            var clock = Stopwatch.StartNew();

            // The test projects declare no framework of their own: Directory.Build.props sets
            // net9.0;net10.0 for the whole repository, which the gate's project-file reader cannot see.
            var frameworks = TargetFrameworks.Declared(RootDirectory / "Directory.Build.props").ToArray();
            if (frameworks.Length == 0)
            {
                throw new InvalidOperationException("Could not read <TargetFrameworks> from Directory.Build.props.");
            }

            var server = new SqlConnectionStringBuilder(serverConnectionString()).DataSource;
            var ledgers = Suites.ToDictionary(suite => suite.Name, suite => loadLedger(ledgerNameFor(suite)));

            var gate = new GateDeclaration
            {
                Frameworks = FrameworkSelection.Parse(Framework),
                Configuration = Configuration,
                SkipBuild = SkipBuild,
                // Per storage leg: SQL Server 2022 and 2025 cost differently, and the record orders
                // the next run's units. A filtered run keeps none — a slice's seconds recorded as a
                // suite's duration would order the next full run wrongly.
                DurationsPath = string.IsNullOrEmpty(TestFilter)
                    ? RootDirectory / ".bobcat" / $"gate-durations.{Storage}.json"
                    : null
            };

            if (WorkerBudget is { } budget) gate.WorkerBudget = budget;

            foreach (var suite in Suites)
            {
                var track = $"polecat_{suite.Lanes}_w*";
                gate.Tracks.Add(new Track(track)
                    .Check($"SQL Server ({Storage}) answers at {server}", pingServer));

                gate.Project(RootDirectory / suite.ProjectPath, suite.Workers)
                    .Targeting(frameworks)
                    .On(track)
                    .Supervise((_, supervisor) => configureSupervisor(supervisor, ledgers[suite.Name]))
                    .LaunchingWith(unit =>
                    {
                        // Provisioned when the unit starts, not before the gate: the catalogs are
                        // this unit's alone, and dropping them is only safe once the previous unit on
                        // this track (the suite's other framework) has finished with them.
                        ensureLaneDatabases(suite, unit.Workers);
                        return workerFactoryFor(suite, unit.Executable);
                    });
            }

            Log.Information("=== LocalGate ({Storage} at {Server}, {Configuration}{Aot}) ===",
                Storage, server, Configuration, Aot ? ", with the AOT units" : "");

            var results = await gate.Run();

            // The same artifacts a CI run leaves: the ledger with this run folded in, a TRX per host,
            // the supervisor's JSON report. Two frameworks of one suite fold into one ledger.
            foreach (var unit in results.Units.Where(u => u.Results is not null))
            {
                var suite = Suites.Single(s => s.Name == unit.Unit.Project.Name);
                ledgers[suite.Name] = recordRun(suite, unit.Unit.Framework, unit.Unit.Executable, unit.Results!,
                    DateTime.UtcNow - unit.Duration, ledgers[suite.Name]);
            }

            if (!string.IsNullOrEmpty(TestFilter) && results.AbortReason is null &&
                results.Units.Sum(u => u.Results?.Tests.Count ?? 0) == 0)
            {
                throw new InvalidOperationException(
                    $"--test-filter '{TestFilter}' matched no test in any unit. Refusing to report an empty run as a pass.");
            }

            var optional = new List<(string Name, TimeSpan Duration, Exception Error)>();
            if (Aot)
            {
                optional.Add(runOptionalUnit("aot-smoke", runAotSmoke));
                optional.Add(runOptionalUnit("aot-runtime-smoke", runAotRuntimeSmoke));

                Console.WriteLine();
                Console.WriteLine($"AOT units ({optional.Count}), after the test suites:");
                foreach (var (name, duration, error) in optional)
                {
                    Console.WriteLine($"  {(error is null ? "✓" : "✗")} {name,-18} {humanize(duration),8}" +
                                      (error is null ? "" : $"  {error.Message}"));
                }
            }

            clock.Stop();
            Console.WriteLine();
            Console.WriteLine($"LocalGate: {humanize(clock.Elapsed)} wall clock in all; test units {humanize(results.Duration)} " +
                              $"({humanize(results.SequentialDuration)} one after the other), exit code {results.ExitCode}" +
                              (Aot ? $", AOT units {humanize(TimeSpan.FromTicks(optional.Sum(x => x.Duration.Ticks)))}" : ""));

            var failedOptional = optional.Where(x => x.Error is not null).Select(x => x.Name).ToArray();
            if (results.ExitCode != 0 || failedOptional.Length > 0)
            {
                throw new InvalidOperationException(
                    $"LocalGate is red: test units exit code {results.ExitCode}" +
                    (failedOptional.Length > 0 ? $"; failed: {string.Join(", ", failedOptional)}" : "") +
                    ". The gate's report above names every unit.");
            }
        });

    /// <summary>
    ///     A Release build of Polecat.AotSmoke, whose project file promotes the AOT and trim analyzer
    ///     warnings (IL2026, IL3050, …) to errors: a static gate on Polecat's annotations. No database.
    ///     The aot-smoke workflow runs this target.
    /// </summary>
    Target AotSmoke => _ => _
        .Executes(runAotSmoke);

    /// <summary>
    ///     Native publish of Polecat.AotRuntimeSmoke for <c>--aot-runtime</c> (the host's RID by
    ///     default; the aot-runtime-smoke workflow passes linux-x64), then runs the binary against SQL
    ///     Server in a catalog of its own, <c>polecat_aot</c>. Red when any shape disagrees with its
    ///     declared expectation (src/Polecat.AotRuntimeSmoke/Program.cs).
    /// </summary>
    Target AotRuntimeSmoke => _ => _
        .Executes(runAotRuntimeSmoke);

    void runAotSmoke()
    {
        DotNetBuild(s => s
            .SetProjectFile(RootDirectory / "src" / "Polecat.AotSmoke" / "Polecat.AotSmoke.csproj")
            .SetConfiguration(Configuration.Release)
            .SetProcessEnvironmentVariable("MSBUILDDISABLENODEREUSE", "1"));
    }

    void runAotRuntimeSmoke()
    {
        var rid = string.IsNullOrEmpty(AotRuntime) ? RuntimeInformation.RuntimeIdentifier : AotRuntime;
        var output = RootDirectory / "artifacts" / "aot-runtime-smoke" / rid;

        // Trim and AOT warnings are NOT promoted to errors here, deliberately: this unit's question is
        // what HAPPENS, and failing the publish on a warning would stop it ever being asked.
        DotNetPublish(s => s
            .SetProject(RootDirectory / "src" / "Polecat.AotRuntimeSmoke" / "Polecat.AotRuntimeSmoke.csproj")
            .SetConfiguration(Configuration.Release)
            .SetRuntime(rid)
            .SetOutput(output)
            .SetProcessEnvironmentVariable("MSBUILDDISABLENODEREUSE", "1"));

        // A catalog of its own, recreated, so it neither sees a previous run's tables nor touches
        // anything a test suite running beside it owns.
        const string catalog = "polecat_aot";
        lock (LaneProvisioning)
        {
            using var connection = new SqlConnection(serverConnectionString());
            connection.Open();
            recreateDatabase(connection, catalog);
        }

        var connectionString = new SqlConnectionStringBuilder(serverConnectionString())
        {
            InitialCatalog = catalog,
            ConnectTimeout = 30
        }.ConnectionString;

        var environment = new Dictionary<string, string>(EnvironmentInfo.Variables)
        {
            ["POLECAT_TESTING_DATABASE"] = connectionString
        };

        var binary = output / (OperatingSystem.IsWindows() ? "Polecat.AotRuntimeSmoke.exe" : "Polecat.AotRuntimeSmoke");
        Log.Information("Running {Binary} ({Rid}) against {Server}/{Catalog}", binary, rid,
            new SqlConnectionStringBuilder(connectionString).DataSource, catalog);

        ProcessTasks.StartProcess(binary, environmentVariables: environment, workingDirectory: output)
            .AssertZeroExitCode();
    }

    static (string Name, TimeSpan Duration, Exception Error) runOptionalUnit(string name, Action run)
    {
        Console.WriteLine($"=== {name}: starting ===");
        var clock = Stopwatch.StartNew();
        try
        {
            run();
            return (name, clock.Elapsed, null);
        }
        catch (Exception e)
        {
            Log.Error(e, "{Unit} failed", name);
            return (name, clock.Elapsed, e);
        }
    }

    async Task pingServer(CancellationToken ct)
    {
        var builder = new SqlConnectionStringBuilder(serverConnectionString()) { ConnectTimeout = 15 };
        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        await command.ExecuteScalarAsync(ct);
    }

    static string humanize(TimeSpan duration)
        => duration.TotalHours >= 1 ? $"{(int)duration.TotalHours}h{duration.Minutes:00}m"
            : duration.TotalMinutes >= 1 ? $"{(int)duration.TotalMinutes}m{duration.Seconds:00}s"
            : $"{duration.TotalSeconds:0}s";
}
