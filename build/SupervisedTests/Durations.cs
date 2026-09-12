using System.Text.Json;
using Bobcat.Supervisor;

namespace Polecat.Build.SupervisedTests;

/// <summary>
///     Per-test durations, carried from one run to the next so lanes are balanced by measured cost
///     instead of by test count.
/// </summary>
/// <remarks>
///     <para>
///         Without history the supervisor balances lanes by counting tests, which assumes every test
///         costs the same. On this suite they do not: the main run that motivated this had four lanes
///         of 642 tests each finish a wall clock of 18m48s against 65m58s of measured test time —
///         perfect balance would be 16m30s, so roughly <b>2m18s</b> was lanes waiting on whichever one
///         drew the slow classes.
///     </para>
///     <para>
///         That number is also the ceiling. Duration balancing cannot beat
///         <c>sum(durations) / lanes</c>, and it cannot split a class — the partitioner works in whole
///         classes because xUnit's isolation contract is per class — so the largest single class is a
///         hard floor underneath that. Worth knowing before expecting more from this than it can give.
///     </para>
/// </remarks>
internal static class Durations
{
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = true };

    /// <summary>
    ///     Writes this run's per-test durations.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>First-attempt</b> durations, not totals. Lane balancing wants what running a test
    ///         once typically costs; a retry-amplified total would overweight exactly the flaky tests,
    ///         which are the ones whose cost is least representative.
    ///     </para>
    ///     <para>
    ///         A test with no reported duration is <b>omitted, never zero-filled</b>. Bobcat charges an
    ///         absent test the median of what is known, which is the right guess; a zero would instead
    ///         claim the test is free and pull whichever lane received it into taking more work.
    ///     </para>
    /// </remarks>
    public static void Write(SupervisorResults results, string path)
    {
        try
        {
            var durations = new SortedDictionary<string, long>(StringComparer.Ordinal);

            foreach (var test in results.Tests)
            {
                var measured = test.Attempts
                    .OrderBy(a => a.AttemptNumber)
                    .Select(a => a.Outcome.Duration)
                    .FirstOrDefault(d => d is not null);

                if (measured is { } duration)
                {
                    durations[test.Uid] = (long)duration.TotalMilliseconds;
                }
            }

            if (durations.Count == 0) return;

            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
            File.WriteAllText(path, JsonSerializer.Serialize(durations, Json));

            Console.WriteLine($"Durations written: {path} ({durations.Count} test(s))");
        }
        catch (Exception e)
        {
            // Reporting ABOUT the tests must never fail the tests. A run that produced a verdict has
            // done its job; losing next run's balancing hint is not worth turning green into red.
            Console.WriteLine($"WARNING: could not write durations to '{path}': {e.Message}");
        }
    }

    /// <summary>
    ///     The previous run's durations, or null when there are none.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Null is not an error and must not read as one: it is the first run, a new branch whose
    ///         artifact has not been produced yet, or a developer running locally. The supervisor
    ///         degrades to count balancing, which is exactly today's behaviour.
    ///     </para>
    ///     <para>
    ///         Accepts a file or a directory, and searches a directory recursively, because an
    ///         artifact download may or may not preserve the directory it was uploaded from.
    ///     </para>
    /// </remarks>
    public static IReadOnlyDictionary<string, TimeSpan>? Read(string? path)
    {
        if (string.IsNullOrWhiteSpace(path)) return null;

        try
        {
            var file = File.Exists(path)
                ? path
                : Directory.Exists(path)
                    ? Directory.EnumerateFiles(path, "*.json", SearchOption.AllDirectories).FirstOrDefault()
                    : null;

            if (file is null)
            {
                Console.WriteLine($"No previous durations at '{path}' — balancing lanes by test count.");
                return null;
            }

            var raw = JsonSerializer.Deserialize<Dictionary<string, long>>(File.ReadAllText(file));
            if (raw is null || raw.Count == 0)
            {
                Console.WriteLine($"'{file}' held no durations — balancing lanes by test count.");
                return null;
            }

            Console.WriteLine($"Balancing lanes with {raw.Count} duration(s) from '{file}'.");

            return raw.ToDictionary(
                pair => pair.Key,
                pair => TimeSpan.FromMilliseconds(pair.Value),
                StringComparer.Ordinal);
        }
        catch (Exception e)
        {
            Console.WriteLine($"WARNING: could not read durations from '{path}' ({e.Message}) — balancing by count.");
            return null;
        }
    }
}
