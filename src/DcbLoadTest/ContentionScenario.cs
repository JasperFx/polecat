using System.Diagnostics;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat;

namespace DcbLoadTest;

/// <summary>
/// 3. Contended decisions on one tag value: marten#5887's "join waitlist" shape. N decisions read
/// the same boundary concurrently (a class: ClassScheduled + SpotBooked) and each appends either
/// WaitlistJoined (not selected by anyone's query) or, with probability <c>--book</c>, SpotBooked (selected by
/// everyone's). Each request retries a DcbConcurrencyException <c>--app-retries</c> times with no wait, like
/// the reporter's Wolverine RetryTimes(2).
///
/// <c>crossed</c> is the deadlock probe: each decision reads one class and appends an event tagged with a
/// different one, so concurrent saves check row X and bump row Y while others check Y and bump X.
/// </summary>
public static class ContentionScenario
{
    private sealed class Outcome
    {
        public int Requests;
        public int Succeeded;
        public int FailedAfterRetries;
        public int Conflicts;
        public int ClientDeadlocks;
        public int OtherErrors;
        public int FirstAttemptConflicts;
        public string? FirstOtherError;
        public readonly Latencies Request = new();
        public readonly Latencies Save = new();
        public TimeSpan Elapsed;
        public long ServerDeadlocks;
        public long SystemHealthDeadlocks;
        public OpStats VersionTable = OpStats.Zero;
        public string Invariant = "";
    }

    public static async Task Run(Settings settings, bool crossed)
    {
        var concurrency = settings.Int("concurrency", 50);
        var rounds = settings.Int("rounds", 5);
        var appRetries = settings.Int("app-retries", 2);
        var capacity = settings.Int("capacity", 100);
        var classes = settings.Int("classes", 4);
        var books = crossed
            ? new[] { settings.Double("book", 0.5) }
            : settings.Get("book", "0,0.2,1").Split(',')
                .Select(x => double.Parse(x, System.Globalization.CultureInfo.InvariantCulture)).ToArray();
        var retryModes = settings.Get("write-retries", "on,off").Split(',').Select(x => x == "on").ToArray();
        var scenarioStart = await Sql.ServerUtcNow(settings);

        Report.Line(crossed
            ? $"## 3b. Crossed boundaries: {concurrency} concurrent decisions over {classes} classes, each reads one class and appends to another"
            : $"## 3. Contended decisions: {concurrency} concurrent decisions on one class");
        Report.Line();
        Report.Header(settings);
        Report.Line($"{rounds} rounds of {concurrency} requests released together; each request makes up to {appRetries + 1} attempts " +
                    $"(fetch boundary, append, save), retrying DcbConcurrencyException with no wait. " +
                    (crossed
                        ? "Half the appends also carry the class read, so a save checks one class's version rows and bumps another's (and sometimes both)."
                        : $"Query: class AND types (ClassScheduled, SpotBooked); 'book' = share of requests that append SpotBooked while seats remain (capacity {capacity}), the rest append WaitlistJoined."));
        Report.Line();
        Report.Line("Deadlocks are three counts: the server-wide `Number of Deadlocks/sec` counter delta (sys.dm_os_performance_counters, cumulative " +
                    "despite the name, and counting every database on the instance), client SqlException 1205, and system_health " +
                    "`xml_deadlock_report` events naming this database (dispatched lazily, so a deadlock in the last ~2 minutes can be missing). " +
                    "Write retries 'on' is Polecat's default ResiliencePipeline, which has no retry strategy; 'off' is ConfigurePolly(_ => { }).");
        Report.Line();

        var rows = new List<string[]>();
        foreach (var book in books)
        foreach (var scope in settings.Scopes)
        foreach (var writeRetries in retryModes)
        {
            var schema = $"dcb_cont_{Stores.Short(scope)}";
            using var store = await Stores.Fresh(settings, schema, scope, writeRetries);
            var outcome = crossed
                ? await RunCrossed(settings, store, schema, concurrency, rounds, appRetries, classes, book)
                : await RunOneClass(settings, store, schema, concurrency, rounds, appRetries, capacity, book);

            var commitsPerSec = outcome.Succeeded / outcome.Elapsed.TotalSeconds;
            var attempts = outcome.Succeeded + outcome.Conflicts + outcome.ClientDeadlocks + outcome.OtherErrors;
            var v = outcome.VersionTable;
            rows.Add([
                book.ToString("0.##"), scope.ToString(), writeRetries ? "on" : "off",
                $"{outcome.Succeeded}/{outcome.Requests}", outcome.FailedAfterRetries.ToString(),
                outcome.Conflicts.ToString(), Report.Pct((double)outcome.FirstAttemptConflicts / outcome.Requests),
                commitsPerSec.ToString("N0"),
                Report.Ms(outcome.Request.Percentile(50)), Report.Ms(outcome.Request.Percentile(99)), Report.Ms(outcome.Request.Max),
                Report.Ms(outcome.Save.Percentile(50)), Report.Ms(outcome.Save.Percentile(99)),
                $"{outcome.ServerDeadlocks} / {outcome.ClientDeadlocks} / {(outcome.SystemHealthDeadlocks < 0 ? "n/a" : outcome.SystemHealthDeadlocks)}",
                $"{v.RowLockWaits:N0} / {v.RowLockWaitMs:N0}", $"{v.PageLockWaits:N0} / {v.PageLockWaitMs:N0}", v.LockPromotions.ToString(),
                outcome.OtherErrors == 0 ? "0" : $"{outcome.OtherErrors} ({outcome.FirstOtherError})",
                outcome.Invariant
            ]);
            Report.Progress($"book {book} {scope} retries {(writeRetries ? "on" : "off")}: " +
                            $"{outcome.Succeeded}/{outcome.Requests} ok, {outcome.Conflicts} conflicts, " +
                            $"{outcome.ServerDeadlocks} server deadlocks, {outcome.ClientDeadlocks} client 1205, " +
                            $"{attempts} attempts, {outcome.Invariant}");
        }

        Report.Table(
            ["book", "Scope", "write retries", "succeeded", "failed after retries", "DcbConcurrencyException",
                "1st-attempt conflict rate", "commits/s", "request p50 ms", "request p99 ms", "request max ms",
                "save p50 ms", "save p99 ms", "deadlocks: perf counter / client 1205 / system_health",
                "version row lock waits / ms", "version page lock waits / ms", "lock escalations", "other errors", "check"],
            rows);

        var total = await Sql.SystemHealthDeadlocks(settings, scenarioStart);
        Report.Line($"system_health deadlock reports naming `{settings.DatabaseName}` since this scenario began: " +
                    $"{(total < 0 ? "n/a" : total.ToString())} (re-read at the end, so it includes reports dispatched late).");
        Report.Line();
    }

    private static EventTagQuery DecisionQuery(Guid classId)
        => new EventTagQuery().Or(new ClassId(classId)).AndEventsOfType<ClassScheduled, SpotBooked>();

    private static async Task<Outcome> RunOneClass(Settings settings, IDocumentStore store, string schema,
        int concurrency, int rounds, int appRetries, int capacity, double book)
    {
        var classId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            var scheduled = session.Events.BuildEvent(new ClassScheduled(classId, capacity));
            scheduled.WithTag(new ClassId(classId));
            session.Events.StartStream(Guid.NewGuid(), scheduled);
            await session.SaveChangesAsync();
        }

        var outcome = await Drive(settings, store, schema, concurrency, rounds, appRetries, (random, session) =>
        {
            var wantsSpot = random.NextDouble() < book;
            return (DecisionQuery(classId), boundary =>
            {
                var state = boundary.Aggregate;
                var member = Guid.NewGuid();
                object data = wantsSpot && state != null && state.Booked < state.Capacity
                    ? new SpotBooked(classId, member)
                    : new WaitlistJoined(classId, member);
                var wrapped = session.Events.BuildEvent(data);
                wrapped.WithTag(new ClassId(classId));
                boundary.AppendOne(wrapped);
            });
        });

        // A lost conflict shows up as overbooking; a replayed commit as more events than successes.
        await using var query = store.LightweightSession(); // IQueryEventStore has no QueryByTagsAsync
        var booked = (await query.Events.QueryByTagsAsync(new EventTagQuery().Or(new ClassId(classId)).AndEventsOfType<SpotBooked>())).Count;
        var all = (await query.Events.QueryByTagsAsync(new EventTagQuery().Or(new ClassId(classId)))).Count;
        var ok = booked <= capacity && all == outcome.Succeeded + 1;
        outcome.Invariant = $"{(ok ? "ok" : "VIOLATED")}: {booked} booked, {all - 1} appended";
        return outcome;
    }

    private static async Task<Outcome> RunCrossed(Settings settings, IDocumentStore store, string schema,
        int concurrency, int rounds, int appRetries, int classes, double book)
    {
        var classIds = Enumerable.Range(0, classes).Select(_ => Guid.NewGuid()).ToArray();
        await using (var session = store.LightweightSession())
        {
            foreach (var id in classIds)
            {
                var scheduled = session.Events.BuildEvent(new ClassScheduled(id, int.MaxValue));
                scheduled.WithTag(new ClassId(id));
                session.Events.StartStream(Guid.NewGuid(), scheduled);
            }

            await session.SaveChangesAsync();
        }

        var outcome = await Drive(settings, store, schema, concurrency, rounds, appRetries, (random, session) =>
        {
            var read = random.Next(classes);
            var write = (read + 1 + random.Next(classes - 1)) % classes;
            // Polecat's tag tables hold any number of values per tag type, so half the appends carry both
            // classes (Marten's HStore mode could not; TagTables is the only shape here).
            var alsoRead = random.Next(2) == 0;
            var wantsSpot = random.NextDouble() < book;
            return (DecisionQuery(classIds[read]), boundary =>
            {
                var member = Guid.NewGuid();
                object data = wantsSpot ? new SpotBooked(classIds[write], member) : new WaitlistJoined(classIds[write], member);
                var wrapped = session.Events.BuildEvent(data);
                if (alsoRead) wrapped.WithTag(new ClassId(classIds[write]), new ClassId(classIds[read]));
                else wrapped.WithTag(new ClassId(classIds[write]));
                boundary.AppendOne(wrapped);
            });
        });

        // Events tagged with two classes are found by both queries, so count distinct ids
        var appended = await CountDecisionEvents(store, classIds);
        outcome.Invariant = appended == outcome.Succeeded ? $"ok: {appended} appended" : $"VIOLATED: {appended} appended, {outcome.Succeeded} succeeded";
        return outcome;
    }

    private static async Task<int> CountDecisionEvents(IDocumentStore store, Guid[] classIds)
    {
        await using var query = store.LightweightSession(); // IQueryEventStore has no QueryByTagsAsync
        var ids = new HashSet<Guid>();
        foreach (var id in classIds)
        {
            foreach (var e in await query.Events.QueryByTagsAsync(new EventTagQuery().Or(new ClassId(id)).AndEventsOfType<SpotBooked, WaitlistJoined>()))
            {
                ids.Add(e.Id);
            }
        }

        return ids.Count;
    }

    /// <summary>
    /// Run the rounds. <paramref name="plan"/> picks, for one attempt, the boundary query and what to append
    /// once the boundary is read.
    /// </summary>
    private static async Task<Outcome> Drive(Settings settings, IDocumentStore store, string schema, int concurrency,
        int rounds, int appRetries, Func<Random, IDocumentSession, (EventTagQuery Query, Action<IEventBoundary<ClassState>> Append)> plan)
    {
        var outcome = new Outcome();

        // Warm-up: the fetch AND save plans, and one pooled connection per concurrent request, so the first
        // variant does not measure plan compilation or SQL Server logins. Each warm-up decision is on a
        // throwaway class of its own, so it conflicts with nothing and the checks never see it.
        await Task.WhenAll(Enumerable.Range(0, concurrency).Select(w => Task.Run(async () =>
        {
            var throwaway = Guid.NewGuid();
            await using var warm = store.LightweightSession();
            var boundary = await warm.Events.FetchForWritingByTags<ClassState>(DecisionQuery(throwaway));
            var joined = warm.Events.BuildEvent(new WaitlistJoined(throwaway, Guid.NewGuid()));
            joined.WithTag(new ClassId(throwaway));
            boundary.AppendOne(joined);
            await warm.SaveChangesAsync();
        })));

        var serverBefore = await Sql.ServerDeadlocks(settings);
        var since = await Sql.ServerUtcNow(settings);
        var versionBefore = await Sql.Operational(settings, schema);

        var sw = Stopwatch.StartNew();
        for (var round = 0; round < rounds; round++)
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var currentRound = round;
            var requests = Enumerable.Range(0, concurrency).Select(i => Task.Run(async () =>
            {
                await gate.Task;
                var random = new Random(currentRound * 1000 + i);
                var started = Stopwatch.GetTimestamp();
                for (var attempt = 0; attempt <= appRetries; attempt++)
                {
                    try
                    {
                        await using var session = store.LightweightSession();
                        var (query, append) = plan(random, session);
                        var boundary = await session.Events.FetchForWritingByTags<ClassState>(query);
                        append(boundary);

                        var saveStarted = Stopwatch.GetTimestamp();
                        await session.SaveChangesAsync();
                        outcome.Save.Add(Stopwatch.GetElapsedTime(saveStarted));

                        Interlocked.Increment(ref outcome.Succeeded);
                        outcome.Request.Add(Stopwatch.GetElapsedTime(started));
                        return;
                    }
                    catch (Exception e) when (Errors.Is<DcbConcurrencyException>(e))
                    {
                        Interlocked.Increment(ref outcome.Conflicts);
                        if (attempt == 0) Interlocked.Increment(ref outcome.FirstAttemptConflicts);
                    }
                    catch (Exception e) when (Errors.IsDeadlock(e))
                    {
                        Interlocked.Increment(ref outcome.ClientDeadlocks);
                    }
                    catch (Exception e)
                    {
                        Interlocked.Increment(ref outcome.OtherErrors);
                        outcome.FirstOtherError ??= Errors.Describe(e);
                    }
                }

                Interlocked.Increment(ref outcome.FailedAfterRetries);
                outcome.Request.Add(Stopwatch.GetElapsedTime(started));
            })).ToArray();

            gate.SetResult();
            await Task.WhenAll(requests);
            outcome.Requests += concurrency;
        }

        outcome.Elapsed = sw.Elapsed;
        outcome.ServerDeadlocks = await Sql.ServerDeadlocks(settings) - serverBefore;
        outcome.SystemHealthDeadlocks = await Sql.SystemHealthDeadlocks(settings, since);
        outcome.VersionTable = (await Sql.Operational(settings, schema)).Minus(versionBefore);
        return outcome;
    }
}
