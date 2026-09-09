using System.Globalization;
using System.Xml.Linq;
using Bobcat.Supervisor;

namespace Polecat.Build.SupervisedTests;

/// <summary>
///     Writes a supervised run's results as a Visual Studio TRX file.
/// </summary>
/// <remarks>
///     <para>
///         The supervisor drives its workers over Microsoft.Testing.Platform's server mode and never
///         asks any of them for a report, so a supervised run produces no TRX of its own — each
///         worker is executing a slice and none of them knows the whole run. This closes that gap so
///         the CI job keeps reporting through <c>dorny/test-reporter</c> exactly as the single-process
///         path does: same artifact, same reporter, same annotations on the PR.
///     </para>
///     <para>
///         Deliberately the minimum shape the <c>dotnet-trx</c> reporter reads — <c>TestDefinitions</c>
///         to name the class, <c>Results</c> for outcomes and failure text, <c>ResultSummary</c> for
///         the counters. It is not a full VSTest emulation and should not grow into one.
///     </para>
///     <para>
///         <b>The failure mode to respect here is silence.</b> A TRX that is malformed, or absent, does
///         not fail the reporter loudly — it reports nothing and the job looks fine, which is the same
///         trap the repo already documents for <c>dotnet test --logger trx</c> under MTP. That is why
///         the runner asserts the file it just wrote parses and holds one result per test before the
///         process exits: a reporting regression must break the run that caused it, not the next
///         person's investigation.
///     </para>
/// </remarks>
internal static class TrxWriter
{
    private static readonly XNamespace Ns = "http://microsoft.com/schemas/VisualStudio/TeamTest/2010";

    // The well-known VSTest "unit test" type id. The reporter does not validate it, but a TRX
    // without it is not a TRX any other tool would accept either.
    private const string UnitTestType = "13cdc9d9-ddb5-4fa4-a97d-d965ccfc6d4b";
    private const string TestListId = "8c84fa94-04c1-424b-9868-57a2d4851a1d";

    public static void Write(SupervisorResults results, string path, string assemblyPath, DateTime startedAt)
    {
        var finishedAt = startedAt + results.Duration;
        var computer = Environment.MachineName;

        var definitions = new XElement(Ns + "TestDefinitions");
        var unitResults = new XElement(Ns + "Results");

        var passed = 0;
        var failed = 0;
        var skipped = 0;

        foreach (var test in results.Tests)
        {
            var final = test.Final.Outcome;

            // A stable id per test rather than a fresh Guid, so re-runs of the same suite produce
            // comparable files and a diff between two TRX files is about outcomes, not identity.
            var testId = DeterministicGuid(test.Uid);
            var executionId = DeterministicGuid("execution:" + test.Uid);

            var (className, _) = SplitDisplayName(test.DisplayName);

            definitions.Add(new XElement(Ns + "UnitTest",
                new XAttribute("name", test.DisplayName),
                new XAttribute("storage", assemblyPath),
                new XAttribute("id", testId),
                new XElement(Ns + "Execution", new XAttribute("id", executionId)),
                new XElement(Ns + "TestMethod",
                    new XAttribute("codeBase", assemblyPath),
                    new XAttribute("adapterTypeName", "executor://bobcat/supervisor"),
                    new XAttribute("className", className),
                    // The full display name, matching what the single-process MTP runner writes
                    // here — not the bare method name. Compared against a reference TRX rather
                    // than guessed, because the reporter groups on className and labels on this.
                    new XAttribute("name", test.DisplayName))));

            var outcome = final.State switch
            {
                WorkerTestState.Passed => "Passed",
                WorkerTestState.Skipped => "NotExecuted",
                _ => "Failed"
            };

            if (outcome == "Passed") passed++;
            else if (outcome == "NotExecuted") skipped++;
            else failed++;

            var result = new XElement(Ns + "UnitTestResult",
                new XAttribute("executionId", executionId),
                new XAttribute("testId", testId),
                new XAttribute("testName", test.DisplayName),
                new XAttribute("computerName", computer),
                new XAttribute("duration", FormatDuration(final.Duration)),
                new XAttribute("startTime", startedAt.ToString("O", CultureInfo.InvariantCulture)),
                new XAttribute("endTime", finishedAt.ToString("O", CultureInfo.InvariantCulture)),
                new XAttribute("testType", UnitTestType),
                new XAttribute("outcome", outcome),
                new XAttribute("testListId", TestListId),
                new XAttribute("relativeResultsDirectory", string.Empty));

            if (outcome == "Failed")
            {
                // An indeterminate is not an ordinary failure and must not read as one: the run never
                // established what the test does, because the worker died under it. TRX has no such
                // outcome, so it is reported as failed — with a message that says which it was, and
                // the worker's own faults are in the run's report beside it.
                var message = final.State == WorkerTestState.Indeterminate
                    ? "INDETERMINATE: the worker finished without reporting a result for this test. "
                      + "See the run report for the worker's exit code and stderr."
                    : Join(final.ErrorType, final.ErrorMessage);

                var errorInfo = new XElement(Ns + "ErrorInfo",
                    new XElement(Ns + "Message", message));

                if (!string.IsNullOrWhiteSpace(final.StackTrace))
                {
                    errorInfo.Add(new XElement(Ns + "StackTrace", final.StackTrace));
                }

                result.Add(new XElement(Ns + "Output", errorInfo));
            }
            else if (test.WasRetriedForFailure)
            {
                // A pass-on-retry is a pass here, because it is one — but the TRX says so out loud
                // rather than laundering it, and the flaky ledger in the run report is the fuller
                // account.
                result.Add(new XElement(Ns + "Output",
                    new XElement(Ns + "StdOut",
                        $"FLAKY: passed on attempt {test.AttemptCount} of {test.AttemptCount}.")));
            }

            unitResults.Add(result);
        }

        var total = results.Tests.Count;

        var run = new XElement(Ns + "TestRun",
            new XAttribute("id", Guid.NewGuid()),
            new XAttribute("name", $"Supervised run ({results.WorkersLaunched} worker(s))"),
            new XElement(Ns + "Times",
                new XAttribute("creation", startedAt.ToString("O", CultureInfo.InvariantCulture)),
                new XAttribute("queuing", startedAt.ToString("O", CultureInfo.InvariantCulture)),
                new XAttribute("start", startedAt.ToString("O", CultureInfo.InvariantCulture)),
                new XAttribute("finish", finishedAt.ToString("O", CultureInfo.InvariantCulture))),
            definitions,
            unitResults,
            new XElement(Ns + "ResultSummary",
                new XAttribute("outcome", results.AbortReason is null && failed == 0 ? "Completed" : "Failed"),
                new XElement(Ns + "Counters",
                    new XAttribute("total", total),
                    new XAttribute("executed", total - skipped),
                    new XAttribute("passed", passed),
                    new XAttribute("failed", failed),
                    new XAttribute("error", 0),
                    new XAttribute("timeout", 0),
                    new XAttribute("aborted", 0),
                    new XAttribute("inconclusive", 0),
                    new XAttribute("passedButRunAborted", 0),
                    new XAttribute("notRunnable", 0),
                    new XAttribute("notExecuted", skipped),
                    new XAttribute("disconnected", 0),
                    new XAttribute("warning", 0),
                    new XAttribute("completed", 0),
                    new XAttribute("inProgress", 0),
                    new XAttribute("pending", 0))));

        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        new XDocument(new XDeclaration("1.0", "utf-8", null), run).Save(path);
    }

    /// <summary>
    ///     Re-reads the file just written and checks it says what the run said.
    /// </summary>
    /// <remarks>
    ///     A reporting regression that produces an unreadable or short TRX is silent at the reporter,
    ///     so it is caught here instead — where it can fail the run that caused it.
    /// </remarks>
    public static void VerifyRoundTrip(string path, int expectedTests)
    {
        var document = XDocument.Load(path);
        var written = document.Descendants(Ns + "UnitTestResult").Count();

        if (written != expectedTests)
        {
            throw new InvalidOperationException(
                $"TRX round-trip check failed: wrote {written} result(s) for {expectedTests} test(s) at '{path}'. "
                + "The CI reporter reads this file; a short or malformed one reports nothing rather than failing.");
        }
    }

    /// <summary>
    ///     Splits an xUnit display name into the class and method the reporter groups by.
    /// </summary>
    /// <remarks>
    ///     A theory's display name carries its arguments — <c>Namespace.Class.method(x: 1)</c> — so the
    ///     split is on the last dot BEFORE any argument list, not the last dot in the string.
    /// </remarks>
    private static (string ClassName, string MethodName) SplitDisplayName(string displayName)
    {
        var argumentStart = displayName.IndexOf('(');
        var searchIn = argumentStart >= 0 ? displayName[..argumentStart] : displayName;

        var lastDot = searchIn.LastIndexOf('.');
        return lastDot < 0
            ? (string.Empty, displayName)
            : (searchIn[..lastDot], displayName[(lastDot + 1)..]);
    }

    private static string Join(string? errorType, string? message)
        => string.IsNullOrWhiteSpace(errorType) ? message ?? "Test failed." : $"{errorType}: {message}";

    private static string FormatDuration(TimeSpan? duration)
        => (duration ?? TimeSpan.Zero).ToString(@"hh\:mm\:ss\.fffffff", CultureInfo.InvariantCulture);

    private static Guid DeterministicGuid(string value)
    {
        var hash = System.Security.Cryptography.MD5.HashData(System.Text.Encoding.UTF8.GetBytes(value));
        return new Guid(hash);
    }
}
