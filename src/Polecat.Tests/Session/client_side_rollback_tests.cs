using System.Data;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Services;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Session;

/// <summary>
///     #854. #832 finished a session whose transaction the SERVER rolled back. A save that fails on the
///     client ends the same transaction the same way, by rolling it back itself, and the caller's
///     exclusive lock or snapshot goes with it. The next save used to begin a clean transaction and carry
///     on without either: the version check still guards an append, but nothing guards a document.
/// </summary>
/// <remarks>
///     A session that lost a lock refuses writes and goes on reading, because a read outside a
///     transaction claims nothing the session has lost. A session at another isolation level refuses
///     reads as well: its next read would begin a new transaction, and with it a different view of the
///     data than the one the caller has been reading.
/// </remarks>
public class client_side_rollback_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public class Note
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class VersionedNote : JasperFx.Metadata.IVersioned
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public Guid Version { get; set; }
    }

    public const string ConcurrencyViolation = nameof(ConcurrencyViolation);
    public const string CancelledBeforeTheSave = nameof(CancelledBeforeTheSave);
    public const string CancelledBeforeTheCommit = nameof(CancelledBeforeTheCommit);
    public const string FailedBeforeTheCommit = nameof(FailedBeforeTheCommit);

    private async Task<Guid> SeedStreamAsync()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var stream = Guid.NewGuid();
        await using var seed = theStore.LightweightSession();
        seed.Events.StartStream(stream, new QuestStarted("seed"));
        await seed.SaveChangesAsync(Ct);
        return stream;
    }

    /// <summary>
    ///     Queues a version check that will fail, by reading a document and changing it behind the
    ///     session's back.
    /// </summary>
    private async Task QueueAStaleWriteAsync(IDocumentSession session)
    {
        var id = Guid.NewGuid();
        await using (var seed = theStore.LightweightSession())
        {
            seed.Store(new VersionedNote { Id = id, Name = "original" });
            await seed.SaveChangesAsync(Ct);
        }

        var mine = await session.LoadAsync<VersionedNote>(id, Ct);

        await using (var writer = theStore.LightweightSession())
        {
            var theirs = await writer.LoadAsync<VersionedNote>(id, Ct);
            theirs!.Name = "theirs";
            writer.Store(theirs);
            await writer.SaveChangesAsync(Ct);
        }

        session.Store(mine!);
    }

    /// <summary>
    ///     Fails a save in one of the ways that ends with Polecat rolling the transaction back itself,
    ///     and returns what the caller saw.
    /// </summary>
    private async Task<Exception> FailASaveAsync(IDocumentSession session, FailBeforeSave listener, string how)
    {
        using var cts = new CancellationTokenSource();

        switch (how)
        {
            case ConcurrencyViolation:
                await QueueAStaleWriteAsync(session);
                break;

            case CancelledBeforeTheSave:
                listener.Arm(cts);
                break;

            case CancelledBeforeTheCommit:
                session.AddTransactionParticipant(new FailOnceBeforeCommit(cts, null));
                break;

            case FailedBeforeTheCommit:
                session.AddTransactionParticipant(new FailOnceBeforeCommit(null, new InvalidOperationException("no")));
                break;
        }

        var failure = await Should.ThrowAsync<Exception>(() => session.SaveChangesAsync(cts.Token));

        switch (how)
        {
            case ConcurrencyViolation:
                failure.ShouldBeOfType<ConcurrencyException>();
                break;

            case CancelledBeforeTheSave or CancelledBeforeTheCommit:
                failure.ShouldBeAssignableTo<OperationCanceledException>();
                break;

            case FailedBeforeTheCommit:
                failure.ShouldBeOfType<InvalidOperationException>();
                break;
        }

        return failure;
    }

    [Theory]
    [InlineData(ConcurrencyViolation)]
    [InlineData(CancelledBeforeTheSave)]
    [InlineData(CancelledBeforeTheCommit)]
    [InlineData(FailedBeforeTheCommit)]
    public async Task a_session_that_lost_its_exclusive_lock_refuses_the_next_write(string how)
    {
        var stream = await SeedStreamAsync();
        var listener = new FailBeforeSave();

        await using var session = theStore.LightweightSession(new SessionOptions { Listeners = { listener } });
        await session.Events.AppendExclusive(stream, Ct, new MonsterSlain("Troll", 2));

        var failure = await FailASaveAsync(session, listener, how);

        // What the caller was relying on really has gone: someone else can take the lock and write.
        await using (var other = theStore.LightweightSession())
        {
            await other.Events.AppendExclusive(stream, Ct, new MonsterSlain("Rat", 1));
            await other.SaveChangesAsync(Ct);
        }

        // Dropping the failed work is not enough, because this one was decided under the same lock.
        session.EjectAllPendingChanges();
        var note = new Note { Id = Guid.NewGuid(), Name = "decided under a lock that is gone" };
        session.Store(note);

        var refused = await Should.ThrowAsync<SessionTransactionUnusableException>(() => session.SaveChangesAsync(Ct));

        // Not compared by reference: a cancelled task rethrows a TaskCanceledException of its own.
        if (failure is OperationCanceledException)
        {
            refused.InnerException.ShouldBeAssignableTo<OperationCanceledException>();
        }
        else
        {
            refused.InnerException.ShouldBeSameAs(failure);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Note>(note.Id, Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task a_write_that_was_left_pending_is_not_saved_without_the_lock_either()
    {
        var stream = await SeedStreamAsync();
        var listener = new FailBeforeSave();

        await using var session = theStore.LightweightSession(new SessionOptions { Listeners = { listener } });
        await session.Events.AppendExclusive(stream, Ct, new MonsterSlain("Troll", 2));

        var note = new Note { Id = Guid.NewGuid() };
        session.Store(note);

        await FailASaveAsync(session, listener, CancelledBeforeTheSave);

        await Should.ThrowAsync<SessionTransactionUnusableException>(() => session.SaveChangesAsync(Ct));

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Note>(note.Id, Ct)).ShouldBeNull();
        (await query.Events.FetchStreamAsync(stream, token: Ct)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task a_session_that_lost_its_lock_goes_on_reading_current_data()
    {
        var stream = await SeedStreamAsync();
        var listener = new FailBeforeSave();

        var existing = new Note { Id = Guid.NewGuid(), Name = "existing" };
        await using (var seed = theStore.LightweightSession())
        {
            seed.Store(existing);
            await seed.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession(new SessionOptions { Listeners = { listener } });
        await session.Events.AppendExclusive(stream, Ct, new MonsterSlain("Troll", 2));

        await FailASaveAsync(session, listener, CancelledBeforeTheSave);

        await using (var other = theStore.LightweightSession())
        {
            other.Events.Append(stream, new MonsterSlain("Rat", 1));
            await other.SaveChangesAsync(Ct);
        }

        // A read outside a transaction claims no lock and sees what is there, so it is telling the truth.
        for (var i = 0; i < 2; i++)
        {
            (await session.LoadAsync<Note>(existing.Id, Ct)).ShouldNotBeNull();
            (await session.Events.FetchStreamAsync(stream, token: Ct)).Count.ShouldBe(2);

            // And the refusal outlasts any number of them.
            await Should.ThrowAsync<SessionTransactionUnusableException>(() => session.SaveChangesAsync(Ct));
        }
    }

    [Fact]
    public async Task a_lost_lock_cannot_be_taken_again_on_the_same_session()
    {
        var stream = await SeedStreamAsync();
        var listener = new FailBeforeSave();

        await using var session = theStore.LightweightSession(new SessionOptions { Listeners = { listener } });
        await session.Events.AppendExclusive(stream, Ct, new MonsterSlain("Troll", 2));

        await FailASaveAsync(session, listener, CancelledBeforeTheSave);

        // The work queued under the first lock is still pending, so a second lock would cover it falsely.
        await Should.ThrowAsync<SessionTransactionUnusableException>(
            () => session.Events.FetchForExclusiveWriting<QuestParty>(stream, Ct));

        await Should.ThrowAsync<SessionTransactionUnusableException>(
            () => session.Events.AppendExclusive(stream, Ct, new MonsterSlain("Rat", 1)));
    }

    [Fact]
    public async Task a_refused_save_does_not_run_the_listeners()
    {
        var stream = await SeedStreamAsync();
        var listener = new FailBeforeSave();

        await using var session = theStore.LightweightSession(new SessionOptions { Listeners = { listener } });
        await session.Events.AppendExclusive(stream, Ct, new MonsterSlain("Troll", 2));

        await FailASaveAsync(session, listener, CancelledBeforeTheSave);

        var before = listener.Calls;

        await Should.ThrowAsync<SessionTransactionUnusableException>(() => session.SaveChangesAsync(Ct));

        listener.Calls.ShouldBe(before);
    }

    [Theory]
    [InlineData(IsolationLevel.Snapshot)]
    [InlineData(IsolationLevel.Serializable)]
    [InlineData(IsolationLevel.RepeatableRead)]
    public async Task a_session_that_lost_its_view_of_the_data_refuses_reads_as_well(IsolationLevel level)
    {
        await session_isolation_level_tests.AllowSnapshotIsolationAsync(Ct);
        var stream = await SeedStreamAsync();
        var listener = new FailBeforeSave();

        await using var session = theStore.LightweightSession(
            new SessionOptions { IsolationLevel = level, Listeners = { listener } });

        await session.Events.FetchStreamAsync(stream, token: Ct);

        session.Store(new Note { Id = Guid.NewGuid() });
        await FailASaveAsync(session, listener, CancelledBeforeTheSave);

        // A read here would begin a new transaction and quietly move the session to a different view
        // of the data from the one the caller has been reading.
        await Should.ThrowAsync<SessionTransactionUnusableException>(
            () => session.LoadAsync<Note>(Guid.NewGuid(), Ct));

        await Should.ThrowAsync<SessionTransactionUnusableException>(
            () => session.Events.FetchStreamAsync(stream, token: Ct));

        session.Store(new Note { Id = Guid.NewGuid() });
        await Should.ThrowAsync<SessionTransactionUnusableException>(() => session.SaveChangesAsync(Ct));
    }

    [Theory]
    [InlineData(ConcurrencyViolation)]
    [InlineData(CancelledBeforeTheSave)]
    [InlineData(CancelledBeforeTheCommit)]
    public async Task a_session_that_held_nothing_goes_on_writing(string how)
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        var listener = new FailBeforeSave();

        await using var session = theStore.LightweightSession(new SessionOptions { Listeners = { listener } });

        var pending = new Note { Id = Guid.NewGuid(), Name = "pending" };
        session.Store(pending);

        await FailASaveAsync(session, listener, how);

        // That transaction was the failed save's own, so the session has lost nothing it was relying on.
        session.EjectAllPendingChanges();
        session.Store(pending);
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Note>(pending.Id, Ct)).ShouldNotBeNull();
    }

    private sealed class FailBeforeSave : IDocumentSessionListener
    {
        private CancellationTokenSource? _cts;

        public int Calls { get; private set; }

        public void Arm(CancellationTokenSource cts) => _cts = cts;

        public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
        {
            Calls++;

            var cts = Interlocked.Exchange(ref _cts, null);
            if (cts == null) return Task.CompletedTask;

            cts.Cancel();
            token.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
            => Task.CompletedTask;
    }

    private sealed class FailOnceBeforeCommit(CancellationTokenSource? cts, Exception? failure) : ITransactionParticipant
    {
        private bool _done;

        public Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
        {
            if (_done) return Task.CompletedTask;
            _done = true;

            cts?.Cancel();
            return failure == null ? Task.CompletedTask : throw failure;
        }
    }
}
