using Polecat.Linq;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon;
using Polecat.Services;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Session;

/// <summary>
///     #783, the Polecat half of marten#5688: a unit of work that fails, or whose token is cancelled,
///     must end its transaction there and then, and the exception the caller sees must be the one
///     that failed it.
/// </summary>
/// <remarks>
///     SQL Server makes a transaction left open more expensive than PostgreSQL does. With
///     <c>XACT_ABORT</c> off, which Polecat relies on, a failed or cancelled statement aborts only
///     the statement, so the transaction keeps every lock it took until a client rolls it back.
/// </remarks>
public class cancelled_save_releases_its_transaction : OneOffConfigurationsContext
{
    private readonly FailBeforeSave _listener = new();

    public override async ValueTask InitializeAsync()
    {
        ConfigureStore(opts => opts.Listeners.Add(_listener));
        await base.InitializeAsync();
    }

    /// <summary>
    ///     <c>AppendExclusive</c> begins the session's transaction and takes <c>UPDLOCK, HOLDLOCK</c> on
    ///     the stream row before <c>SaveChangesAsync</c> is called. A save that then fails before it
    ///     reaches the commit (here a listener's <c>BeforeSaveChangesAsync</c>, the first thing it
    ///     runs) used to leave that transaction open, and with it the stream lock, until the session
    ///     was disposed: every other writer of the stream waited out its command timeout.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_save_that_fails_after_an_exclusive_append_releases_the_stream_lock(bool cancel)
    {
        var token = TestContext.Current.CancellationToken;
        var streamId = Guid.NewGuid();

        await using (var seed = theStore.LightweightSession())
        {
            seed.Events.StartStream(streamId, new QuestStarted("Lock"));
            await seed.SaveChangesAsync(token);
        }

        using var cts = new CancellationTokenSource();

        // Deliberately not disposed until the end: disposal would release the lock either way, and a
        // request-scoped or long-lived session keeps living after its save fails.
        await using var session = theStore.LightweightSession();
        await session.Events.AppendExclusive(streamId, token, new MonsterSlain("Troll", 2));

        _listener.Arm(cts, cancel);
        if (cancel)
        {
            await Should.ThrowAsync<OperationCanceledException>(() => session.SaveChangesAsync(cts.Token));
        }
        else
        {
            await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(cts.Token));
        }

        // A second writer of the same stream gets its lock at once rather than queueing behind the
        // failed session.
        await assertStreamIsNotLockedAsync(streamId, token);

        // The failed unit of work wrote nothing.
        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(streamId, token: token)).Count.ShouldBe(1);
    }

    /// <summary>
    ///     A token cancelled after the unit of work's statements ran, but before the COMMIT, rolls the
    ///     unit of work back and surfaces as <see cref="OperationCanceledException" />, with the
    ///     stream lock released at once. Already true of the session path, whose <c>using</c> disposes
    ///     the transaction with no token; pinned because it is the scenario marten#5688 broke.
    /// </summary>
    [Fact]
    public async Task a_token_cancelled_just_before_the_commit_rolls_back_and_throws()
    {
        var token = TestContext.Current.CancellationToken;
        var streamId = Guid.NewGuid();

        await using (var seed = theStore.LightweightSession())
        {
            seed.Events.StartStream(streamId, new QuestStarted("Lock"));
            await seed.SaveChangesAsync(token);
        }

        using var cts = new CancellationTokenSource();

        await using var session = theStore.LightweightSession();
        await session.Events.AppendExclusive(streamId, token, new MonsterSlain("Troll", 2));
        session.AddTransactionParticipant(new CancelBeforeCommit(cts));

        await Should.ThrowAsync<OperationCanceledException>(() => session.SaveChangesAsync(cts.Token));

        await assertStreamIsNotLockedAsync(streamId, token);

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(streamId, token: token)).Count.ShouldBe(1);
    }

    /// <summary>
    ///     The daemon's projection batch rolled back with the token it was handed. When that token was
    ///     already cancelled, <c>RollbackAsync</c> threw an <see cref="OperationCanceledException" />
    ///     that replaced the exception which failed the batch, so the shard's failure named a
    ///     cancellation rather than its cause.
    /// </summary>
    [Fact]
    public async Task a_failed_daemon_batch_reports_its_own_failure_when_the_token_is_cancelled()
    {
        using var cts = new CancellationTokenSource();

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database);
        var session = batch.SessionForTenant(theStore.Options.Tenancy!.DefaultTenantId);
        session.Store(new BatchDoc { Id = Guid.NewGuid() });
        session.AddTransactionParticipant(new CancelThenFail(cts));

        var failure = await Should.ThrowAsync<InvalidOperationException>(() => batch.ExecuteAsync(cts.Token));
        failure.Message.ShouldBe(CancelThenFail.Message);

        // Rolled back, so the projected document never landed.
        await using var query = theStore.QuerySession();
        (await query.Query<BatchDoc>().ToListAsync(TestContext.Current.CancellationToken)).ShouldBeEmpty();
    }

    private async Task assertStreamIsNotLockedAsync(Guid streamId, CancellationToken token)
    {
        // LOCK_TIMEOUT turns "it waited" into error 1222 instead of a hang
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SET LOCK_TIMEOUT 2000;
            SELECT version FROM [{theStore.Options.DatabaseSchemaName}].[pc_streams] WITH (UPDLOCK)
            WHERE id = @id;
            """;
        cmd.Parameters.AddWithValue("@id", streamId);
        (await cmd.ExecuteScalarAsync(token)).ShouldBe(1L);
    }

    public class BatchDoc
    {
        public Guid Id { get; set; }
    }

    private sealed class FailBeforeSave : IDocumentSessionListener
    {
        private CancellationTokenSource? _cts;
        private bool _cancel;

        public void Arm(CancellationTokenSource cts, bool cancel)
        {
            _cts = cts;
            _cancel = cancel;
        }

        public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
        {
            var cts = Interlocked.Exchange(ref _cts, null);
            if (cts == null) return Task.CompletedTask;

            if (_cancel)
            {
                // A client that disconnects while the save is getting started
                cts.Cancel();
                token.ThrowIfCancellationRequested();
            }

            throw new InvalidOperationException("The save failed before its transaction began");
        }

        public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
            => Task.CompletedTask;
    }

    private sealed class CancelBeforeCommit(CancellationTokenSource cts) : ITransactionParticipant
    {
        public Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
        {
            // Every statement has run; the caller gives up before the COMMIT is sent
            cts.Cancel();
            return Task.CompletedTask;
        }
    }

    private sealed class CancelThenFail(CancellationTokenSource cts) : ITransactionParticipant
    {
        public const string Message = "The participant failed the batch";

        public Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
        {
            // The drain's deadline expiring at the same moment as a real failure
            cts.Cancel();
            throw new InvalidOperationException(Message);
        }
    }
}
