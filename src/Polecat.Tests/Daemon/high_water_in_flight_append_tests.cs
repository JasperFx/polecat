using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon;
using Polecat.Projections;
using Polecat.Subscriptions;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     How a database decides what a reader sees. Each one is a real answer to "what does the detector
///     see while an append is in flight", which is why every fact below runs against all three.
/// </summary>
public enum DatabaseLocking
{
    /// <summary>The SQL Server default. A plain read blocks on an uncommitted row.</summary>
    LockingReadCommitted,

    /// <summary>The Azure SQL Database default. A plain read skips an uncommitted row silently.</summary>
    ReadCommittedSnapshot,

    /// <summary>Also on by default in Azure SQL Database. The writer holds no row lock, only its transaction.</summary>
    OptimizedLocking
}

/// <summary>
///     One database per <see cref="DatabaseLocking" /> mode, because every one of those settings is per
///     database and <c>master</c> cannot have any of them. Created on first use and shared by a class's
///     facts, since creating them is by far the slowest thing these tests do.
/// </summary>
public abstract class HighWaterDatabases : IAsyncDisposable
{
    private readonly Dictionary<DatabaseLocking, string> _created = new();

    public async Task<string> ConnectionStringForAsync(DatabaseLocking locking)
    {
        if (!_created.TryGetValue(locking, out var database))
        {
            database = ConnectionSource.Scoped($"{GetType().Name}_{locking}");
            await CreateAsync(database, locking);
            _created[locking] = database;
        }

        return ConnectionSource.ConnectionStringFor(database);
    }

    private static async Task CreateAsync(string database, DatabaseLocking locking)
    {
        var snapshot = locking == DatabaseLocking.LockingReadCommitted ? "OFF" : "ON";
        var optimized = locking != DatabaseLocking.OptimizedLocking
            ? ""
            : $"""
               ALTER DATABASE [{database}] SET ACCELERATED_DATABASE_RECOVERY = ON WITH ROLLBACK IMMEDIATE;
               ALTER DATABASE [{database}] SET OPTIMIZED_LOCKING = ON WITH ROLLBACK IMMEDIATE;
               """;

        await ExecuteAsync(ConnectionSource.MasterConnectionString, $"""
            {DropSql(database)}
            CREATE DATABASE [{database}];
            ALTER DATABASE [{database}] SET READ_COMMITTED_SNAPSHOT {snapshot} WITH ROLLBACK IMMEDIATE;
            {optimized}
            """);

        // A fact named for a mode is worthless if it quietly ran in another, so read the mode back.
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = locking == DatabaseLocking.OptimizedLocking
            ? "SELECT is_read_committed_snapshot_on & is_optimized_locking_on FROM sys.databases WHERE name = @name;"
            : "SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = @name;";
        cmd.Parameters.AddWithValue("@name", database);

        var enabled = (bool)(await cmd.ExecuteScalarAsync())!;
        enabled.ShouldBe(locking != DatabaseLocking.LockingReadCommitted, $"{database} is not in {locking} mode");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var database in _created.Values)
        {
            await using (var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(database)))
            {
                SqlConnection.ClearPool(conn);
            }

            await ExecuteAsync(ConnectionSource.MasterConnectionString, DropSql(database));
        }
    }

    private static string DropSql(string database) => $"""
        IF DB_ID('{database}') IS NOT NULL
        BEGIN
            ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE [{database}];
        END
        """;

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}

public sealed class StoreGlobalHighWaterDatabases : HighWaterDatabases;

public sealed class PerTenantHighWaterDatabases : HighWaterDatabases;

/// <summary>
///     Every fact starts from an empty event store with its IDENTITY and sequences back at the start, so the
///     literal seq_ids in these tests are real.
/// </summary>
public abstract class HighWaterLockingContext : IAsyncLifetime
{
    private readonly HighWaterDatabases _databases;
    private readonly List<DocumentStore> _stores = [];

    protected HighWaterLockingContext(HighWaterDatabases databases)
    {
        _databases = databases;
    }

    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    protected string ConnectionString { get; private set; } = "";

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        foreach (var store in _stores) store.Dispose();
        return ValueTask.CompletedTask;
    }

    protected async Task<DocumentStore> CreateStoreAsync(DatabaseLocking locking, Action<StoreOptions>? configure = null)
    {
        if (locking == DatabaseLocking.OptimizedLocking)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "Optimized locking needs SQL Server 2025");
        }

        ConnectionString = await _databases.ConnectionStringForAsync(locking);

        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionString;
            opts.DatabaseSchemaName = "dbo";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.AddEventType<Tick>();
            configure?.Invoke(opts);
        });
        _stores.Add(store);

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await ExecuteAsync(ConnectionString, """
            DELETE FROM dbo.pc_events;
            DELETE FROM dbo.pc_streams;
            DELETE FROM dbo.pc_event_progression;
            IF OBJECT_ID('dbo.pc_doc_ticktotal') IS NOT NULL DELETE FROM dbo.pc_doc_ticktotal;

            -- RESEED on a table that has never had a row would hand out 0 next, not 1.
            IF EXISTS (SELECT 1 FROM sys.identity_columns
                       WHERE object_id = OBJECT_ID('dbo.pc_events') AND last_value IS NOT NULL)
                DBCC CHECKIDENT ('dbo.pc_events', RESEED, 0) WITH NO_INFOMSGS;

            DECLARE @restart nvarchar(max) = N'';
            SELECT @restart = @restart + 'ALTER SEQUENCE dbo.' + name + ' RESTART WITH 1;'
            FROM sys.sequences WHERE name LIKE 'pc[_]events[_]sequence[_]%';
            EXEC sp_executesql @restart;
            """);

        return store;
    }

    protected static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private protected static PolecatHighWaterDetector DetectorFor(DocumentStore store,
        TimeSpan? staleSequenceThreshold = null)
    {
        var settings = store.Options.DaemonSettings;
        if (staleSequenceThreshold.HasValue)
        {
            settings = new DaemonSettings { StaleSequenceThreshold = staleSequenceThreshold.Value };
        }

        return new PolecatHighWaterDetector(store.Options.EventGraph, store.Options.ConnectionString, settings,
            NullLogger<PolecatHighWaterDetector>.Instance, store.Options.ResiliencePipeline);
    }

    /// <summary>One event each on new streams, which leaves the streams for later appends to hit.</summary>
    protected static async Task<Guid[]> StartStreamsAsync(DocumentStore store, int count, string? tenant = null)
    {
        var streams = Enumerable.Range(0, count).Select(_ => Guid.NewGuid()).ToArray();
        foreach (var stream in streams)
        {
            await AppendAsync(store, stream, tenant, start: true);
        }

        return streams;
    }

    private static IDocumentSession OpenSession(DocumentStore store, string? tenant)
        => tenant == null ? store.LightweightSession() : store.LightweightSession(new SessionOptions { TenantId = tenant });

    protected static async Task AppendAsync(DocumentStore store, Guid stream, string? tenant = null, bool start = false)
    {
        await using var session = OpenSession(store, tenant);
        if (start) session.Events.StartStream(stream, new Tick());
        else session.Events.Append(stream, new Tick());
        await session.SaveChangesAsync(Ct);
    }

    public record Tick;

    public class TickTotal
    {
        public Guid Id { get; set; }
        public int Count { get; set; }
        public void Apply(Tick _) => Count++;
    }

    protected sealed class DeliberateRollback : Exception;

    /// <summary>
    ///     A real <c>SaveChangesAsync</c>, stopped after its rows are written and before they commit, for
    ///     as long as the test wants. Disposing an append still in flight rolls it back.
    /// </summary>
    /// <remarks>
    ///     Append only to a stream that already exists. The append planner reads the stream row under
    ///     <c>UPDLOCK, HOLDLOCK</c>, so a NEW stream takes a key-range lock on <c>pc_streams</c> that can
    ///     make the next new stream wait for this one, which would stop the test before it starts.
    /// </remarks>
    protected sealed class InFlightAppend : IAsyncDisposable
    {
        private readonly HeldBeforeCommit _hold = new();
        private Task _save = Task.CompletedTask;

        public static async Task<InFlightAppend> StartAsync(DocumentStore store, Guid stream, string? tenant = null)
        {
            var append = new InFlightAppend();
            append._save = Task.Run(async () =>
            {
                await using var session = OpenSession(store, tenant);
                session.AddTransactionParticipant(append._hold);
                session.Events.Append(stream, new Tick());
                await session.SaveChangesAsync();
            });

            await Task.WhenAny(append._hold.Written, append._save).WaitAsync(TimeSpan.FromSeconds(10), Ct);
            if (append._save.IsCompleted) await append._save;

            return append;
        }

        public async Task CommitAsync()
        {
            _hold.Release(commit: true);
            await _save.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        }

        public async Task RollBackAsync()
        {
            _hold.Release(commit: false);
            await Should.ThrowAsync<DeliberateRollback>(() => _save.WaitAsync(TimeSpan.FromSeconds(10), Ct));
        }

        public async ValueTask DisposeAsync()
        {
            _hold.Release(commit: false);
            try { await _save; }
            catch (DeliberateRollback) { }
        }
    }

    // Separate from InFlightAppend because a session disposes any participant that is IAsyncDisposable, and
    // InFlightAppend's disposal waits for that same session's save.
    private sealed class HeldBeforeCommit : ITransactionParticipant
    {
        private readonly TaskCompletionSource _written = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _commit = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Written => _written.Task;

        public void Release(bool commit) => _commit.TrySetResult(commit);

        public async Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction,
            CancellationToken token)
        {
            _written.TrySetResult();
            if (!await _commit.Task.WaitAsync(token)) throw new DeliberateRollback();
        }
    }
}

/// <summary>
///     #833. With READ_COMMITTED_SNAPSHOT on, an in-flight append is not locked against the detector, it is
///     simply a hole; and a hole directly above the mark was invisible, because the gap walk has no row below
///     it to compare with. The mark moved past it, the append committed BELOW the mark, and no async
///     projection or subscription ever read that event. Nothing logged and nothing was reported as skipped.
///     With READ_COMMITTED_SNAPSHOT off the reader blocked on the row, which is what hid this.
/// </summary>
public class high_water_in_flight_append_tests(StoreGlobalHighWaterDatabases databases)
    : HighWaterLockingContext(databases), IClassFixture<StoreGlobalHighWaterDatabases>
{
    // Long enough that a detector blocked on a row lock fails here rather than passing slowly.
    private static readonly TimeSpan DetectionBound = TimeSpan.FromSeconds(3);

    private static Task<HighWaterStatistics> DetectAsync(PolecatHighWaterDetector detector)
        => detector.Detect(Ct).WaitAsync(DetectionBound, Ct);

    private static Task<HighWaterStatistics> DetectInSafeZoneAsync(PolecatHighWaterDetector detector)
        => detector.DetectInSafeZone(Ct).WaitAsync(DetectionBound, Ct);

    /// <summary>The reported defect, with the detector also required to answer rather than block.</summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_in_flight_append_holds_the_mark_below_it(DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 2);

        var detector = DetectorFor(store);
        (await DetectAsync(detector)).CurrentMark.ShouldBe(2);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0]); // seq 3
        await AppendAsync(store, streams[1]); // seq 4 commits behind it

        var stats = await DetectAsync(detector);

        stats.CurrentMark.ShouldBe(2, "seq 3 has not committed, so nothing above it is safe to read yet");
        stats.HighestSequence.ShouldBe(4);
        stats.IncludesSkipping.ShouldBeFalse();

        await inFlight.CommitAsync();

        (await DetectAsync(detector)).CurrentMark.ShouldBe(4);
    }

    /// <summary>
    ///     The half that holding alone does not fix. The default threshold is three seconds, so a mark that
    ///     merely held would hand the same event loss back once a large append had been open for that long.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_in_flight_append_is_not_skipped_however_long_it_stays_open(DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 2);

        var detector = DetectorFor(store, staleSequenceThreshold: TimeSpan.FromMilliseconds(100));
        await DetectAsync(detector);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0]); // seq 3
        await AppendAsync(store, streams[1]); // seq 4

        // What the agent does once it decides the mark is stale: safe zone, again and again.
        for (var i = 0; i < 4; i++)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(250), Ct);

            var stats = await DetectInSafeZoneAsync(detector);
            stats.CurrentMark.ShouldBe(2, "seq 3 is in flight, not dead");
            stats.IncludesSkipping.ShouldBeFalse();
        }

        await inFlight.CommitAsync();

        var after = await DetectInSafeZoneAsync(detector);
        after.CurrentMark.ShouldBe(4);
        after.IncludesSkipping.ShouldBeFalse("nothing was skipped: seq 3 landed");
    }

    /// <summary>
    ///     The other direction. IDENTITY values are burned by a rollback and Polecat writes no tombstones, so
    ///     this hole is permanent and holding at it cannot be the end of the story. Only the safe zone may
    ///     cross it, only after the threshold, and it has to say that it did.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task a_rolled_back_append_above_the_mark_is_skipped_only_by_the_safe_zone_once_stale(
        DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 2);

        var detector = DetectorFor(store, staleSequenceThreshold: TimeSpan.FromMilliseconds(500));
        await DetectAsync(detector);

        var rolledBack = await InFlightAppend.StartAsync(store, streams[0]); // seq 3
        await rolledBack.RollBackAsync();
        await AppendAsync(store, streams[1]); // seq 4

        (await DetectAsync(detector)).CurrentMark.ShouldBe(2);

        var young = await DetectInSafeZoneAsync(detector);
        young.CurrentMark.ShouldBe(2, "the gap is younger than the threshold");
        young.IncludesSkipping.ShouldBeFalse();

        await Task.Delay(TimeSpan.FromMilliseconds(750), Ct);

        (await DetectAsync(detector)).CurrentMark.ShouldBe(2, "Detect never crosses a gap, however old");

        var stale = await DetectInSafeZoneAsync(detector);
        stale.CurrentMark.ShouldBe(4);
        stale.IncludesSkipping.ShouldBeTrue();

        (await DetectAsync(detector)).CurrentMark.ShouldBe(4, "and the skip is what was persisted");
    }

    /// <summary>
    ///     A skip is not all or nothing. A dead hole under the mark must not stall on an unrelated long append
    ///     higher up, and clearing it must not run on over that append to the height.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task a_skip_stops_below_an_append_in_flight_higher_up(DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 3);

        var detector = DetectorFor(store, staleSequenceThreshold: TimeSpan.FromMilliseconds(100));
        await DetectAsync(detector);

        var rolledBack = await InFlightAppend.StartAsync(store, streams[0]); // seq 4
        await rolledBack.RollBackAsync();
        await AppendAsync(store, streams[1]); // seq 5
        await using var inFlight = await InFlightAppend.StartAsync(store, streams[2]); // seq 6
        await AppendAsync(store, streams[1]); // seq 7

        await DetectAsync(detector);
        await Task.Delay(TimeSpan.FromMilliseconds(250), Ct);

        var skipped = await DetectInSafeZoneAsync(detector);
        skipped.CurrentMark.ShouldBe(5, "4 is dead, 5 is committed, and 6 is still in flight");
        skipped.IncludesSkipping.ShouldBeTrue();

        await Task.Delay(TimeSpan.FromMilliseconds(250), Ct);
        (await DetectInSafeZoneAsync(detector)).CurrentMark.ShouldBe(5);

        await inFlight.CommitAsync();

        (await DetectAsync(detector)).CurrentMark.ShouldBe(7);
    }

    /// <summary>
    ///     A skip reaches only as far as what existed when the gap was first seen. A value allocated moments
    ///     ago can have no row yet, so a hole that young is no evidence of anything and gets its own threshold.
    /// </summary>
    [Fact]
    public async Task a_skip_does_not_cross_a_hole_younger_than_the_threshold()
    {
        var store = await CreateStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 2);

        var detector = DetectorFor(store, staleSequenceThreshold: TimeSpan.FromSeconds(1));
        await DetectAsync(detector);

        var oldHole = await InFlightAppend.StartAsync(store, streams[0]); // seq 3
        await oldHole.RollBackAsync();
        await AppendAsync(store, streams[1]); // seq 4

        (await DetectAsync(detector)).CurrentMark.ShouldBe(2); // the clock starts here, with 4 the ceiling

        await Task.Delay(TimeSpan.FromMilliseconds(1250), Ct);

        var youngHole = await InFlightAppend.StartAsync(store, streams[0]); // seq 5
        await youngHole.RollBackAsync();
        await AppendAsync(store, streams[1]); // seq 6

        var first = await DetectInSafeZoneAsync(detector);
        first.CurrentMark.ShouldBe(4, "seq 3 is old enough to skip. seq 5 is not");
        first.IncludesSkipping.ShouldBeTrue();

        var again = await DetectInSafeZoneAsync(detector);
        again.CurrentMark.ShouldBe(4);
        again.IncludesSkipping.ShouldBeFalse();

        await Task.Delay(TimeSpan.FromMilliseconds(1250), Ct);

        (await DetectInSafeZoneAsync(detector)).CurrentMark.ShouldBe(6);
    }

    /// <summary>
    ///     A hole above the mark and a second one higher up. Finding the higher one must not let the mark
    ///     advance to it: that steps over the lower one, on both paths.
    /// </summary>
    [Fact]
    public async Task a_gap_higher_up_does_not_hide_a_leading_gap()
    {
        var store = await CreateStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 2);

        var detector = DetectorFor(store);
        await DetectAsync(detector);

        await using var lower = await InFlightAppend.StartAsync(store, streams[0]); // seq 3
        await AppendAsync(store, streams[1]); // seq 4
        var higher = await InFlightAppend.StartAsync(store, streams[1]); // seq 5
        await higher.RollBackAsync();
        await AppendAsync(store, streams[1]); // seq 6

        (await DetectAsync(detector)).CurrentMark.ShouldBe(2);
        (await DetectInSafeZoneAsync(detector)).CurrentMark.ShouldBe(2);
    }

    /// <summary>
    ///     A store with no mark has no floor, so events that simply start above 1 are not a hole to wait on.
    ///     An append IN that range still is. Telling those apart is the only way to keep both this and
    ///     <c>detect_all_contiguous</c>, which sits on a table IDENTITY has long since moved on from.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_in_flight_append_holds_a_store_that_has_no_mark_yet(DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 2);
        await ExecuteAsync(ConnectionString, "DELETE FROM dbo.pc_events;"); // leaves IDENTITY at 2, and no mark

        var detector = DetectorFor(store);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0]); // seq 3
        await AppendAsync(store, streams[1]); // seq 4

        (await DetectAsync(detector)).CurrentMark.ShouldBe(0);

        await inFlight.CommitAsync();

        (await DetectAsync(detector)).CurrentMark.ShouldBe(4);
    }

    [Fact]
    public async Task a_store_with_no_mark_whose_events_start_above_one_is_not_held()
    {
        var store = await CreateStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 2);
        await ExecuteAsync(ConnectionString, "DELETE FROM dbo.pc_events;");
        await AppendAsync(store, streams[0]); // seq 3

        (await DetectAsync(DetectorFor(store))).CurrentMark.ShouldBe(3);
    }

    /// <summary>
    ///     The bug as a user meets it: an event that committed after the mark passed it never reaches an async
    ///     projection, and here it stays open past the threshold on a running daemon, so the agent's own
    ///     stale handling has run several times by the time it lands.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_append_that_commits_late_still_reaches_an_async_projection(DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking, opts =>
        {
            opts.Projections.Snapshot<TickTotal>(SnapshotLifecycle.Async);
            opts.DaemonSettings.StaleSequenceThreshold = TimeSpan.FromMilliseconds(250);
        });
        var streams = await StartStreamsAsync(store, 2);

        using var daemon = await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        await WaitForTotalsAsync(store, streams[0], 1);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0]);
        await AppendAsync(store, streams[1]);

        await Task.Delay(TimeSpan.FromSeconds(2), Ct);

        await inFlight.CommitAsync();

        await WaitForTotalsAsync(store, streams[0], 2);
        await WaitForTotalsAsync(store, streams[1], 2);

        await daemon.StopAllAsync();
    }

    /// <summary>
    ///     #791's hang, with the gap directly above the mark rather than between two events. Detect now holds
    ///     at that gap where it used to jump it, so CatchUpAsync depends on the safe zone to get past: a
    ///     detector that held and never skipped would sit out CheckNowAsync's five minute backstop.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task catch_up_passes_a_rolled_back_append_above_the_mark(DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking, opts =>
        {
            opts.Projections.Snapshot<TickTotal>(SnapshotLifecycle.Async);
            opts.DaemonSettings.StaleSequenceThreshold = TimeSpan.FromMilliseconds(500);
        });
        var streams = await StartStreamsAsync(store, 2);
        await DetectorFor(store).Detect(Ct);

        var rolledBack = await InFlightAppend.StartAsync(store, streams[0]);
        await rolledBack.RollBackAsync();
        await AppendAsync(store, streams[1]);

        using var daemon = await store.BuildProjectionDaemonAsync();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), Ct).WaitAsync(TimeSpan.FromSeconds(60), Ct);
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(30));

        await using var query = store.QuerySession();
        (await query.LoadAsync<TickTotal>(streams[1], Ct)).ShouldNotBeNull().Count.ShouldBe(2);
    }

    /// <summary>
    ///     Everything at once and no staging: many short appends committing past the occasional long one, some
    ///     rolling back, on a running daemon. No scripted interleaving above can reach a read that disagrees
    ///     with itself; this can.
    /// </summary>
    /// <remarks>
    ///     The shape of the load matters, because the old code only lost an event with the mark caught up
    ///     directly beneath an in-flight row. Evenly long transactions leave an older hole under the lowest one
    ///     almost all the time, the gap walk stops at that, and this passed on code that lost events. Asserted
    ///     on the seq_ids a subscription was handed rather than on a projected count for the same reason: an
    ///     aggregate here is repaired by the stream's next event.
    /// </remarks>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task concurrent_appends_and_rollbacks_lose_no_committed_event(DatabaseLocking locking)
    {
        const int writers = 4;
        const int appends = 25;

        var delivered = new SequenceCollector();
        var store = await CreateStoreAsync(locking, opts =>
        {
            opts.Projections.Subscribe(delivered);
            opts.DaemonSettings.StaleSequenceThreshold = TimeSpan.FromMilliseconds(300);
            opts.DaemonSettings.FastPollingTime = TimeSpan.FromMilliseconds(25);
            opts.DaemonSettings.SlowPollingTime = TimeSpan.FromMilliseconds(50);
        });
        var streams = await StartStreamsAsync(store, writers);

        using var daemon = await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        await Task.WhenAll(streams.Select((stream, writer) => Task.Run(async () =>
        {
            var random = new Random(writer);

            for (var i = 0; i < appends; i++)
            {
                await using var append = await InFlightAppend.StartAsync(store, stream);

                // Long enough to outlast the stale threshold and several detection cycles.
                await Task.Delay(random.Next(0, 8) == 0 ? 1500 : random.Next(0, 20), Ct);

                if (random.Next(0, 10) == 0) await append.RollBackAsync();
                else await append.CommitAsync();
            }
        }, Ct)));

        var committed = await CommittedSequencesAsync();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        while (committed.Except(delivered.Sequences).Any() && stopwatch.Elapsed < TimeSpan.FromSeconds(20))
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), Ct);
        }

        committed.Except(delivered.Sequences).ShouldBeEmpty("every committed event has to reach a subscription");

        await daemon.StopAllAsync();
    }

    private async Task<List<long>> CommittedSequencesAsync()
    {
        var sequences = new List<long>();

        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT seq_id FROM dbo.pc_events;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct)) sequences.Add(reader.GetInt64(0));

        return sequences;
    }

    private sealed class SequenceCollector : SubscriptionBase
    {
        private readonly ConcurrentDictionary<long, bool> _sequences = new();

        public IEnumerable<long> Sequences => _sequences.Keys;

        public override Task<IChangeListener> ProcessEventsAsync(EventRange page, ISubscriptionController controller,
            IDocumentOperations operations, CancellationToken cancellationToken)
        {
            foreach (var @event in page.Events) _sequences[@event.Sequence] = true;

            return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
        }
    }

    private static async Task WaitForTotalsAsync(DocumentStore store, Guid stream, int expected)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var count = 0;

        while (stopwatch.Elapsed < TimeSpan.FromSeconds(30))
        {
            await using var query = store.QuerySession();
            count = (await query.LoadAsync<TickTotal>(stream, Ct))?.Count ?? 0;
            if (count == expected) return;

            await Task.Delay(TimeSpan.FromMilliseconds(100), Ct);
        }

        count.ShouldBe(expected, $"stream {stream} should have been projected in full");
    }
}

/// <summary>
///     #833 on the path <c>UseTenantPartitionedEvents</c> uses. A tenant's seq_ids come from its own sequence,
///     which rollbacks burn exactly as they burn IDENTITY values. This path had a leading-gap rule already, so
///     Detect's hole was not here; its flaw was the skip. Its age comes from <c>last_updated</c>, stamped only
///     when a tenant's mark advances, so a tenant idle for longer than the threshold stepped over a brand new
///     in-flight append the first time it saw one.
/// </summary>
public class per_tenant_high_water_in_flight_append_tests(PerTenantHighWaterDatabases databases)
    : HighWaterLockingContext(databases), IClassFixture<PerTenantHighWaterDatabases>
{
    private const string Tenant = "Red";

    private Task<DocumentStore> CreateTenantedStoreAsync(DatabaseLocking locking) => CreateStoreAsync(locking, opts =>
    {
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.EventGraph.UseTenantPartitionedEvents = true;
    });

    /// <summary>
    ///     A row committed behind an in-flight append, from the tenant's own sequence.
    /// </summary>
    /// <remarks>
    ///     Raw rather than through a session because a real append here would wait for the one in flight,
    ///     which is a different problem from the one under test. pc_streams' key is
    ///     <c>(tenant_id, id, tenant_ordinal)</c> under partitioning, so the planner's <c>HOLDLOCK</c> read on
    ///     <c>(tenant_id, id)</c> is a range seek that also locks the next stream in key order. A real append
    ///     only gets ahead of another one if it has already passed that read.
    /// </remarks>
    private async Task CommitBehindAsync()
    {
        var ordinal = await OrdinalAsync();
        await ExecuteAsync(ConnectionString, $$"""
            INSERT INTO dbo.pc_events (seq_id, id, stream_id, version, data, type, tenant_id, tenant_ordinal)
            VALUES (NEXT VALUE FOR dbo.pc_events_sequence_{{ordinal}}, NEWID(), NEWID(), 1, '{}', 'tick', '{{Tenant}}', {{ordinal}});
            """);
    }

    /// <summary>What a rollback leaves behind: a sequence value gone, and no row.</summary>
    private async Task BurnSequenceValueAsync()
        => await ExecuteAsync(ConnectionString, $"SELECT NEXT VALUE FOR dbo.pc_events_sequence_{await OrdinalAsync()};");

    private async Task<int> OrdinalAsync()
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT ordinal FROM dbo.pc_tenant_partitions WHERE tenant_id = '{Tenant}';";
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private static Task PersistMarkAsync(DocumentStore store, long mark, DateTimeOffset stamp)
        => ((IHighWaterDetector)DetectorFor(store)).MarkHighWaterForTenantAsync(Tenant, mark, stamp, Ct);

    private static async Task<HighWaterStatistics> DetectAsync(DocumentStore store)
    {
        var vector = await DetectorFor(store).DetectForTenantsAsync([Tenant], Ct)
            .WaitAsync(TimeSpan.FromSeconds(3), Ct);

        vector.TryGetStatistics(Tenant, out var stats).ShouldBeTrue();
        return stats;
    }

    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_in_flight_append_holds_a_tenants_mark(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 2, Tenant);
        await PersistMarkAsync(store, 2, DateTimeOffset.UtcNow);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 3
        await CommitBehindAsync(); // seq 4

        var stats = await DetectAsync(store);
        stats.CurrentMark.ShouldBe(2);
        stats.HighestSequence.ShouldBe(4);

        await inFlight.CommitAsync();

        (await DetectAsync(store)).CurrentMark.ShouldBe(4);
    }

    /// <summary>
    ///     The flaw read out of the code in #833, reproduced. Nothing has touched this tenant for five minutes,
    ///     which is all it took to read as stale, on the first poll that ever saw this gap.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_idle_tenant_does_not_skip_an_in_flight_append_on_first_sight(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 2, Tenant);
        await PersistMarkAsync(store, 2, DateTimeOffset.UtcNow.AddMinutes(-5));

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 3
        await CommitBehindAsync(); // seq 4

        var stats = await DetectAsync(store);
        stats.CurrentMark.ShouldBe(2);
        stats.IncludesSkipping.ShouldBeFalse();

        await inFlight.CommitAsync();

        var after = await DetectAsync(store);
        after.CurrentMark.ShouldBe(4);
        after.IncludesSkipping.ShouldBeFalse("nothing was skipped: seq 3 landed");
    }

    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task a_rolled_back_append_is_skipped_once_the_tenant_goes_stale(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 2, Tenant);
        await PersistMarkAsync(store, 2, DateTimeOffset.UtcNow);

        var rolledBack = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 3
        await rolledBack.RollBackAsync();
        await CommitBehindAsync(); // seq 4

        (await DetectAsync(store)).CurrentMark.ShouldBe(2);

        await PersistMarkAsync(store, 2, DateTimeOffset.UtcNow.AddMinutes(-5));

        var stats = await DetectAsync(store);
        stats.CurrentMark.ShouldBe(4);
        stats.IncludesSkipping.ShouldBeTrue();
    }

    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task a_tenants_skip_stops_below_an_append_in_flight_higher_up(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 3, Tenant);
        await PersistMarkAsync(store, 3, DateTimeOffset.UtcNow.AddMinutes(-5));

        var rolledBack = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 4
        await rolledBack.RollBackAsync();
        await CommitBehindAsync(); // seq 5
        await using var inFlight = await InFlightAppend.StartAsync(store, streams[2], Tenant); // seq 6
        await CommitBehindAsync(); // seq 7

        var stats = await DetectAsync(store);
        stats.CurrentMark.ShouldBe(5, "4 is dead, 5 is committed, and 6 is still in flight");
        stats.IncludesSkipping.ShouldBeTrue();
    }

    /// <summary>
    ///     The per-tenant rule used to look for a leading gap only when it had found no gap higher up, so
    ///     the higher one moved the mark straight over the lower.
    /// </summary>
    [Fact]
    public async Task a_gap_higher_up_does_not_hide_a_tenants_leading_gap()
    {
        var store = await CreateTenantedStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 2, Tenant);
        await PersistMarkAsync(store, 2, DateTimeOffset.UtcNow);

        await using var lower = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 3
        await CommitBehindAsync(); // seq 4
        await BurnSequenceValueAsync(); // seq 5
        await CommitBehindAsync(); // seq 6

        (await DetectAsync(store)).CurrentMark.ShouldBe(2);
    }

    /// <summary>
    ///     The walk only looks above the mark, so it finds nothing here. A tenant removed and added back restarts
    ///     its sequence below the row it left behind, and that mark has to come back down rather than strand it.
    /// </summary>
    [Fact]
    public async Task a_mark_above_a_tenants_height_follows_it_down()
    {
        var store = await CreateTenantedStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        await StartStreamsAsync(store, 2, Tenant);
        await PersistMarkAsync(store, 50, DateTimeOffset.UtcNow);

        (await DetectAsync(store)).CurrentMark.ShouldBe(2);
    }

    /// <summary>
    ///     <c>a_tenant_whose_events_start_above_one_is_not_held_at_zero</c> holds, because that hole is
    ///     nothing. This one has an append in it.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_in_flight_append_holds_a_tenant_that_has_no_mark_yet(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 2, Tenant);
        await ExecuteAsync(ConnectionString, "DELETE FROM dbo.pc_events;");

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 3
        await CommitBehindAsync(); // seq 4

        (await DetectAsync(store)).CurrentMark.ShouldBe(0);

        await inFlight.CommitAsync();

        (await DetectAsync(store)).CurrentMark.ShouldBe(4);
    }
}
