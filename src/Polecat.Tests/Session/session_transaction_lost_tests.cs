using System.Data;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Internal;
using Polecat.Resilience;
using Polecat.Services;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polly;
using Polly.Retry;

namespace Polecat.Tests.Session;

/// <summary>
///     #832, the SQL Server shape of marten#5577 / marten#5578: what a session does once SQL Server has
///     rolled its transaction back underneath it.
/// </summary>
/// <remarks>
///     <para>
///         1205 and 3960 end the transaction on the server and leave the connection open. SqlClient
///         marks the <see cref="SqlTransaction" /> completed, and <c>SqlCommand</c> silently drops a
///         completed transaction rather than refusing it. So a session that kept using the dead object
///         ran every statement in autocommit and only found out at the commit: reported failed, and
///         durable.
///     </para>
///     <para>
///         The rule these facts pin: a session never runs a command outside a transaction it believes
///         it is inside. If the caller was relying on that transaction (an exclusive lock, a
///         <c>Snapshot</c> view) the session is finished. If it was only the failed save's own, the
///         save failed atomically and the next one starts clean.
///     </para>
/// </remarks>
public class session_transaction_lost_tests : OneOffConfigurationsContext
{
    private const int DeadlockVictim = 1205;
    private const int SnapshotUpdateConflict = 3960;
    private const int LockRequestTimeout = 1222;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private string StreamsTable => $"[{theStore.Options.DatabaseSchemaName}].[pc_streams]";
    private string RowsTable => $"[{theStore.Options.DatabaseSchemaName}].[lost_tx_rows]";
    private string ProbeTable => $"[{theStore.Options.DatabaseSchemaName}].[lost_tx_probe]";

    public class Note
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private async Task<T> ScalarAsync<T>(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (T)Convert.ChangeType((await cmd.ExecuteScalarAsync(Ct))!, typeof(T));
    }

    private async Task CreateScratchTablesAsync()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await ExecuteAsync($"""
            CREATE TABLE {RowsTable} (id int PRIMARY KEY, n int NOT NULL);
            INSERT INTO {RowsTable} VALUES (1, 0), (2, 0);
            CREATE TABLE {ProbeTable} (id int IDENTITY PRIMARY KEY, who varchar(20) NOT NULL);
            """);
    }

    private async Task<(Guid A, Guid B)> SeedTwoStreamsAsync()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var a = Guid.NewGuid();
        var b = Guid.NewGuid();

        await using var seed = theStore.LightweightSession();
        seed.Events.StartStream(a, new QuestStarted("A"));
        seed.Events.StartStream(b, new QuestStarted("B"));
        seed.Store(new Note { Id = Guid.NewGuid(), Name = "seed" });
        await seed.SaveChangesAsync(Ct);

        return (a, b);
    }

    private sealed record DeadlockOutcome(
        IDocumentSession Victim, IDocumentSession Survivor, Exception Failure, Guid StreamA, Guid StreamB);

    /// <summary>
    ///     Two sessions take stream locks in opposite order, as in
    ///     <c>exclusive_append_lock_failure_tests</c>, and the victim is handed back still open so the
    ///     test can go on using it. The caller owns both sessions.
    /// </summary>
    private async Task<DeadlockOutcome> LoseADeadlockAsync(Func<IDocumentSession, Guid, Task> takeLock)
    {
        var (a, b) = await SeedTwoStreamsAsync();

        var one = theStore.LightweightSession();
        var two = theStore.LightweightSession();

        await takeLock(one, a);
        await takeLock(two, b);

        async Task<(IDocumentSession Session, Exception? Failure)> Cross(IDocumentSession session, Guid id)
        {
            try
            {
                await takeLock(session, id);
                return (session, null);
            }
            catch (Exception e)
            {
                return (session, e);
            }
        }

        var results = await Task.WhenAll(Task.Run(() => Cross(one, b), Ct), Task.Run(() => Cross(two, a), Ct));

        var victim = results.Single(x => x.Failure != null);
        var survivor = results.Single(x => x.Failure == null);

        return new DeadlockOutcome(victim.Session, survivor.Session, victim.Failure!, a, b);
    }

    private static Task AppendExclusive(IDocumentSession session, Guid id)
        => session.Events.AppendExclusive(id, Ct, new MonsterSlain("Troll", 2));

    private static async Task FetchExclusive(IDocumentSession session, Guid id)
    {
        var stream = await session.Events.FetchForExclusiveWriting<QuestParty>(id, Ct);
        stream.AppendOne(new MonsterSlain("Troll", 2));
    }

    private async Task AssertOnlyTheSeedIsDurableAsync(DeadlockOutcome outcome)
    {
        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(outcome.StreamA, token: Ct)).Count.ShouldBe(1);
        (await query.Events.FetchStreamAsync(outcome.StreamB, token: Ct)).Count.ShouldBe(1);
    }

    // ---- a transaction that died outside a save ---------------------------------------------------

    [Fact]
    public async Task a_save_after_losing_a_deadlock_writes_nothing_and_names_the_deadlock()
    {
        var outcome = await LoseADeadlockAsync(AppendExclusive);
        await outcome.Survivor.DisposeAsync();

        // The contract #652 pinned is unchanged: the deadlock itself is a StreamLockedException.
        outcome.Failure.ShouldBeOfType<StreamLockedException>();

        var note = new Note { Id = Guid.NewGuid(), Name = "after the deadlock" };
        outcome.Victim.Store(note);

        // Before #832: "This SqlTransaction has completed", thrown by the commit, after every statement
        // of the save had already autocommitted.
        var refused = await Should.ThrowAsync<SessionTransactionUnusableException>(
            () => outcome.Victim.SaveChangesAsync(Ct));
        refused.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(DeadlockVictim);

        await outcome.Victim.DisposeAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Note>(note.Id, Ct)).ShouldBeNull();
        await AssertOnlyTheSeedIsDurableAsync(outcome);
    }

    [Theory]
    [InlineData(nameof(AppendExclusive))]
    [InlineData(nameof(FetchExclusive))]
    public async Task an_exclusive_lock_lost_to_a_deadlock_ends_the_session(string how)
    {
        var outcome = await LoseADeadlockAsync(how == nameof(AppendExclusive) ? AppendExclusive : FetchExclusive);
        await using var survivor = outcome.Survivor;
        await using var victim = outcome.Victim;

        outcome.Failure.ShouldBeOfType<StreamLockedException>();

        // The caller believes it holds a lock it no longer holds, so none of these may quietly carry on.
        // Each one asked twice: a refusal must not itself clear the state that caused it.
        for (var i = 0; i < 2; i++)
        {
            await Should.ThrowAsync<SessionTransactionUnusableException>(
                () => victim.LoadAsync<Note>(Guid.NewGuid(), Ct));

            await Should.ThrowAsync<SessionTransactionUnusableException>(
                () => victim.Events.FetchForExclusiveWriting<QuestParty>(outcome.StreamA, Ct));

            // Not a second StreamLockedException, which would read as "try again on this session".
            await Should.ThrowAsync<SessionTransactionUnusableException>(
                () => victim.Events.AppendExclusive(outcome.StreamA, Ct, new MonsterSlain("Rat", 1)));

            await Should.ThrowAsync<SessionTransactionUnusableException>(() => victim.SaveChangesAsync(Ct));
        }
    }

    [Fact]
    public async Task a_refused_save_does_not_run_the_listeners_of_a_save_that_cannot_happen()
    {
        var listener = new CountingListener();
        ConfigureStore(opts => opts.Listeners.Add(listener));

        var outcome = await LoseADeadlockAsync(AppendExclusive);
        await using var survivor = outcome.Survivor;
        await using var victim = outcome.Victim;

        var before = listener.BeforeSaves;

        await Should.ThrowAsync<SessionTransactionUnusableException>(() => victim.SaveChangesAsync(Ct));

        listener.BeforeSaves.ShouldBe(before);
    }

    // ---- a transaction that died inside a save ----------------------------------------------------

    [Fact]
    public async Task a_deadlock_inside_the_save_of_an_exclusive_lock_ends_the_session()
    {
        var (a, b) = await SeedTwoStreamsAsync();

        await using var one = theStore.LightweightSession();
        await using var two = theStore.LightweightSession();

        await AppendExclusive(one, a);
        await AppendExclusive(two, b);

        // Each save needs the row the other session has locked.
        one.QueueSqlCommand($"UPDATE {StreamsTable} SET version = version WHERE id = '{b}'");
        two.QueueSqlCommand($"UPDATE {StreamsTable} SET version = version WHERE id = '{a}'");

        async Task<(IDocumentSession Session, Exception? Failure)> Save(IDocumentSession session)
        {
            try
            {
                await session.SaveChangesAsync(Ct);
                return (session, null);
            }
            catch (Exception e)
            {
                return (session, e);
            }
        }

        var results = await Task.WhenAll(Task.Run(() => Save(one), Ct), Task.Run(() => Save(two), Ct));
        var victim = results.Single(x => x.Failure != null);

        victim.Failure.ShouldBeOfType<SqlException>().Number.ShouldBe(DeadlockVictim);

        // What this used to do is the worst case in the issue: the failed save cleared Transaction, so
        // this one began a clean transaction and wrote without the lock the caller thought it held.
        var refused = await Should.ThrowAsync<SessionTransactionUnusableException>(
            () => victim.Session.SaveChangesAsync(Ct));
        refused.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(DeadlockVictim);

        // Only the survivor's event landed.
        await using var query = theStore.QuerySession();
        var appended = (await query.Events.FetchStreamAsync(a, token: Ct)).Count
                       + (await query.Events.FetchStreamAsync(b, token: Ct)).Count;
        appended.ShouldBe(3);
    }

    [Fact]
    public async Task a_save_that_holds_nothing_fails_atomically_and_the_next_one_starts_clean()
    {
        await CreateScratchTablesAsync();

        const string delay = "WAITFOR DELAY '00:00:00.300'";

        await using var one = theStore.LightweightSession();
        await using var two = theStore.LightweightSession();

        one.QueueSqlCommand(
            $"UPDATE {RowsTable} SET n = n + 1 WHERE id = 1; {delay}; UPDATE {RowsTable} SET n = n + 1 WHERE id = 2");
        two.QueueSqlCommand(
            $"UPDATE {RowsTable} SET n = n + 1 WHERE id = 2; {delay}; UPDATE {RowsTable} SET n = n + 1 WHERE id = 1");

        async Task<(IDocumentSession Session, Exception? Failure)> Save(IDocumentSession session)
        {
            try
            {
                await session.SaveChangesAsync(Ct);
                return (session, null);
            }
            catch (Exception e)
            {
                return (session, e);
            }
        }

        var results = await Task.WhenAll(Task.Run(() => Save(one), Ct), Task.Run(() => Save(two), Ct));
        var victim = results.Single(x => x.Failure != null);
        victim.Failure.ShouldBeOfType<SqlException>().Number.ShouldBe(DeadlockVictim);

        // Only the survivor's work is there, so the victim's first UPDATE really was rolled back.
        (await ScalarAsync<int>($"SELECT SUM(n) FROM {RowsTable}")).ShouldBe(2);

        // That session was relying on nothing: its transaction was the failed save's own. Its pending
        // work is still pending, and saving it again is a whole new atomic attempt.
        await victim.Session.SaveChangesAsync(Ct);

        (await ScalarAsync<int>($"SELECT COUNT(*) FROM {RowsTable} WHERE n = 2")).ShouldBe(2);

        // And the session goes on working.
        await victim.Session.LoadAsync<Note>(Guid.NewGuid(), Ct);
    }

    [Fact]
    public async Task a_snapshot_conflict_leaves_nothing_durable_and_cannot_be_replayed_by_saving_again()
    {
        await session_isolation_level_tests.AllowSnapshotIsolationAsync(Ct);
        var (stream, _) = await SeedTwoStreamsAsync();

        await using var session = theStore.LightweightSession(
            new SessionOptions { IsolationLevel = IsolationLevel.Snapshot });

        await session.Events.FetchStreamAsync(stream, token: Ct);

        await using (var writer = theStore.LightweightSession())
        {
            writer.Events.Append(stream, new MonsterSlain("Theirs", 1));
            await writer.SaveChangesAsync(Ct);
        }

        var extra = new Note { Id = Guid.NewGuid(), Name = "extra" };
        session.Events.Append(stream, new MonsterSlain("Mine", 1));
        session.Store(extra);

        var conflict = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
        conflict.Number.ShouldBe(SnapshotUpdateConflict);

        // Saving again is marten#5528's replay by hand: a fresh snapshot in which the conflict no longer
        // exists, so the append would land on a stream it never saw. Only re-reading can resolve this.
        await Should.ThrowAsync<SessionTransactionUnusableException>(() => session.SaveChangesAsync(Ct));

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(stream, token: Ct)).Count.ShouldBe(2);
        (await query.LoadAsync<Note>(extra.Id, Ct)).ShouldBeNull();
    }

    // ---- a failure that does not end the transaction -----------------------------------------------

    [Fact]
    public async Task a_lock_timeout_aborts_the_statement_and_the_session_keeps_its_transaction()
    {
        var (a, b) = await SeedTwoStreamsAsync();

        await using var session = theStore.LightweightSession();
        await AppendExclusive(session, a);

        var internalSession = (DocumentSessionBase)session;
        await using (var setTimeout = new SqlCommand("SET LOCK_TIMEOUT 100;"))
        {
            await internalSession.ExecuteAsync(setTimeout, Ct);
        }

        await using var holder = new SqlConnection(ConnectionSource.ConnectionString);
        await holder.OpenAsync(Ct);
        await using var hold = (SqlTransaction)await holder.BeginTransactionAsync(Ct);
        await using (var cmd = holder.CreateCommand())
        {
            cmd.Transaction = hold;
            cmd.CommandText = $"UPDATE {StreamsTable} SET version = version WHERE id = '{b}'";
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var failure = await Should.ThrowAsync<StreamLockedException>(() => AppendExclusive(session, b));
        failure.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(LockRequestTimeout);

        await hold.RollbackAsync(Ct);

        // Unlike 1205, nothing was rolled back: the first lock is still held and its append still lands.
        // Treating every lock failure alike would poison a session that is perfectly healthy.
        PolecatRetryPredicates.IsUnsafeToReplay(failure).ShouldBeFalse();
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(a, token: Ct)).Count.ShouldBe(2);
    }

    // ---- what a retry strategy meets ---------------------------------------------------------------

    private int _retries;

    /// <summary>A strategy that handles everything: what a caller who did not read the docs configures.</summary>
    private void RetryEverything()
    {
        ConfigureStore(opts => opts.ConfigurePolly(builder => builder.AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = 2,
            Delay = TimeSpan.FromMilliseconds(1),
            ShouldHandle = args => ValueTask.FromResult(args.Outcome.Exception is SqlException),
            OnRetry = _ =>
            {
                Interlocked.Increment(ref _retries);
                return ValueTask.CompletedTask;
            }
        })));
    }

    [Fact]
    public async Task a_retried_exclusive_read_is_refused_rather_than_run_outside_its_transaction()
    {
        RetryEverything();

        // Before #832 the retry of this read autocommitted without its UPDLOCK, so the append succeeded
        // and told the caller it held a lock nobody held. LoseADeadlockAsync then found no victim at all.
        var outcome = await LoseADeadlockAsync(AppendExclusive);
        await using var survivor = outcome.Survivor;
        await using var victim = outcome.Victim;

        _retries.ShouldBe(1);

        // A retry the caller configured has turned the lock failure into "this session is finished".
        var refused = outcome.Failure.ShouldBeOfType<SessionTransactionUnusableException>();
        refused.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(DeadlockVictim);

        await Should.ThrowAsync<SessionTransactionUnusableException>(() => victim.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_retried_document_batch_is_refused_rather_than_autocommitted()
    {
        RetryEverything();
        await CreateScratchTablesAsync();
        var (a, b) = await SeedTwoStreamsAsync();

        await using var one = theStore.LightweightSession();
        await using var two = theStore.LightweightSession();

        await AppendExclusive(one, a);
        await AppendExclusive(two, b);

        // The issue's scenario: the session's events are already in the transaction by the time the
        // document batch loses the deadlock. A replay that autocommits writes this probe with the events
        // rolled back underneath it, and then the commit reports failure.
        one.QueueSqlCommand(
            $"UPDATE {StreamsTable} SET version = version WHERE id = '{b}'; INSERT INTO {ProbeTable}(who) VALUES ('one')");
        two.QueueSqlCommand(
            $"UPDATE {StreamsTable} SET version = version WHERE id = '{a}'; INSERT INTO {ProbeTable}(who) VALUES ('two')");

        async Task<Exception?> Save(IDocumentSession session)
        {
            try
            {
                await session.SaveChangesAsync(Ct);
                return null;
            }
            catch (Exception e)
            {
                return e;
            }
        }

        var failures = await Task.WhenAll(Task.Run(() => Save(one), Ct), Task.Run(() => Save(two), Ct));
        failures.Count(x => x != null).ShouldBe(1);

        var failure = failures.Single(x => x != null);
        failure.ShouldBeOfType<SessionTransactionUnusableException>();
        failure!.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(DeadlockVictim);

        _retries.ShouldBe(1);

        // One unit of work committed, one did not, and nothing in between.
        (await ScalarAsync<int>($"SELECT COUNT(*) FROM {ProbeTable}")).ShouldBe(1);

        await using var query = theStore.QuerySession();
        var appended = (await query.Events.FetchStreamAsync(a, token: Ct)).Count
                       + (await query.Events.FetchStreamAsync(b, token: Ct)).Count;
        appended.ShouldBe(3);
    }

    [Fact]
    public async Task a_retried_snapshot_conflict_is_refused_rather_than_autocommitted()
    {
        await session_isolation_level_tests.AllowSnapshotIsolationAsync(Ct);
        RetryEverything();
        await CreateScratchTablesAsync();

        await using var session = theStore.LightweightSession(
            new SessionOptions { IsolationLevel = IsolationLevel.Snapshot });

        await using (var read = new SqlCommand($"SELECT n FROM {RowsTable} WHERE id = 1;"))
        {
            await ((DocumentSessionBase)session).ExecuteScalarAsync(read, Ct);
        }

        await ExecuteAsync($"UPDATE {RowsTable} SET n = 100 WHERE id = 1");

        session.QueueSqlCommand(
            $"UPDATE {RowsTable} SET n = 1 WHERE id = 1; INSERT INTO {ProbeTable}(who) VALUES ('replayed')");

        // The conflict's own transaction is gone, so a replay that autocommits finds nothing to conflict
        // with, overwrites the concurrent write, and is then reported as a failed save.
        var refused = await Should.ThrowAsync<SessionTransactionUnusableException>(
            () => session.SaveChangesAsync(Ct));
        refused.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(SnapshotUpdateConflict);

        _retries.ShouldBe(1);

        (await ScalarAsync<int>($"SELECT n FROM {RowsTable} WHERE id = 1")).ShouldBe(100);
        (await ScalarAsync<int>($"SELECT COUNT(*) FROM {ProbeTable}")).ShouldBe(0);
    }

    [Fact]
    public async Task a_strategy_that_asks_the_predicate_stops_at_the_refused_replay()
    {
        ConfigureStore(opts => opts.ExtendPolly(builder => builder.AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = 2,
            Delay = TimeSpan.FromMilliseconds(1),
            ShouldHandle = args => ValueTask.FromResult(
                args.Outcome.Exception is not null
                && !PolecatRetryPredicates.IsUnsafeToReplay(args.Outcome.Exception)),
            OnRetry = _ =>
            {
                Interlocked.Increment(ref _retries);
                return ValueTask.CompletedTask;
            }
        })));

        var outcome = await LoseADeadlockAsync(AppendExclusive);
        await using var survivor = outcome.Survivor;
        await using var victim = outcome.Victim;

        // The number can't say the transaction is gone, so 1205 at ReadCommitted costs one attempt. The
        // replay is refused rather than run outside its transaction, and the refusal stops the strategy.
        _retries.ShouldBe(1);
        var refused = outcome.Failure.ShouldBeOfType<SessionTransactionUnusableException>();
        refused.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(1205);
        PolecatRetryPredicates.IsUnsafeToReplay(outcome.Failure, IsolationLevel.ReadCommitted).ShouldBeTrue();
    }

    [Fact]
    public void the_refusal_itself_is_never_safe_to_replay()
    {
        var refusal = new SessionTransactionUnusableException(null);

        PolecatRetryPredicates.IsUnsafeToReplay(refusal).ShouldBeTrue();
        PolecatRetryPredicates.IsUnsafeToReplay(new InvalidOperationException("wrapped", refusal)).ShouldBeTrue();
    }

    // ---- what the guard must not catch --------------------------------------------------------------

    [Theory]
    [InlineData(IsolationLevel.ReadCommitted)]
    [InlineData(IsolationLevel.Serializable)]
    public async Task a_post_commit_listener_can_read_through_the_session_that_just_committed(IsolationLevel level)
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // A committed SqlTransaction reports a null Connection exactly as a dead one does.
        var listener = new ReadingListener();
        await using var session = theStore.LightweightSession(
            new SessionOptions { IsolationLevel = level, Listeners = { listener } });

        var first = new Note { Id = Guid.NewGuid(), Name = "first" };
        session.Store(first);
        await session.SaveChangesAsync(Ct);

        // At Serializable that read began the next transaction, which must not be thrown away unseen.
        var second = new Note { Id = Guid.NewGuid(), Name = "second" };
        session.Store(second);
        await session.SaveChangesAsync(Ct);

        listener.Reads.ShouldBe(2);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Note>(second.Id, Ct)).ShouldNotBeNull();
    }

    private sealed class CountingListener : IDocumentSessionListener
    {
        public int BeforeSaves { get; private set; }

        public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
        {
            BeforeSaves++;
            return Task.CompletedTask;
        }

        public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
            => Task.CompletedTask;
    }

    private sealed class ReadingListener : IDocumentSessionListener
    {
        public int Reads { get; private set; }

        public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token) => Task.CompletedTask;

        public async Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        {
            await session.LoadAsync<Note>(Guid.NewGuid(), token);
            Reads++;
        }
    }
}
