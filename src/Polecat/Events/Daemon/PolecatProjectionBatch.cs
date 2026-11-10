using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polly;
using Polecat.Internal;
using Polecat.Internal.Operations;
using Polecat.Internal.Sessions;
using Polecat.Services;
using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Events.Daemon;

/// <summary>
///     Implements IProjectionBatch for the async daemon.
///     Accumulates operations from multiple tenant sessions and flushes them in one SQL transaction.
///     Thread-safe: composite projections may call SessionForTenant concurrently.
///     All SQL execution is wrapped with Polly resilience.
/// </summary>
/// <remarks>
///     jasperfx#1045 (polecat#806): the store-agnostic half -- the sessions, the events projections raise,
///     the messages they publish, the order the commit hooks run in, and holding side effects back
///     outside <see cref="ShardExecutionMode.Continuous" /> -- is <see cref="ProjectionBatchBase{TSession}" />.
///     What is left here is SQL Server's: the one transaction, the progression rows, the transaction
///     participants (#867), and the async change listeners.
/// </remarks>
internal class PolecatProjectionBatch : ProjectionBatchBase<IDocumentSession>,
    IProjectionBatch<IDocumentSession, IQuerySession>, ITransactionParticipantRegistrar
{
    private readonly DocumentStore _store;
    private readonly EventGraph _events;
    private readonly PolecatDatabase _database;
    private readonly string _connectionString;
    private readonly ResiliencePipeline _resilience;
    private readonly ConcurrentQueue<Weasel.Storage.IStorageOperation> _progressOps = new();

    // #867: held here and not on the sessions, because this batch is what commits. See AdoptSession.
    private readonly ConcurrentQueue<ITransactionParticipant> _participants = new();

    // #420: the append operations for the events projections raised, built by PlanRaisedEventsAsync and
    // added to the transaction after the document operations, ahead of the progression row.
    private List<Weasel.Storage.IStorageOperation> _raisedEventOps = new();

    // Set by ExecuteOperationsInTransactionAsync for AfterCommitAsync: the session the async listeners
    // were handed, or null when no listener runs for this batch.
    private IDocumentSession? _listenerSession;

    // #807: the participants that ran BeforeCommitAsync inside the transaction that committed, set only
    // once the commit has returned, so a batch that rolled back hands none of them to AfterCommitAsync.
    private IReadOnlyList<ITransactionParticipant> _committedParticipants = [];

    // The change set committed by this batch, populated during ExecuteAsync. Exposed so the
    // subscription runner can hand it to the IChangeListener returned by ProcessEventsAsync.
    public IChangeSet? Commit { get; private set; }

    public PolecatProjectionBatch(DocumentStore store, EventGraph events, PolecatDatabase database,
        ShardExecutionMode mode) : base(mode)
    {
        _store = store;
        _events = events;
        _database = database;
        _connectionString = database.ConnectionString;
        _resilience = store.Options.ResiliencePipeline;
    }

    protected override JasperFx.Events.Documents.IMessageOutbox Outbox => _store.Options.Events.MessageOutbox;

    protected override IDocumentSession OpenSessionForTenant(string tenantId)
    {
        // #514: bind to the database this batch is working rather than routing tenantId through the
        // tenancy. The batch drains these sessions' work trackers and executes the operations
        // against _connectionString, so writes were already landing correctly — but the sessions'
        // own READS (loading an existing projected document before applying to it) went wherever
        // tenant routing sent them, and under database-per-tenant tenancy "*DEFAULT*" maps to no
        // tenant at all. Mirrors Marten's SessionOptions.ForDatabase.
        //
        // #683: and with NO connection held between commands. This batch keeps one session per tenant
        // alive until it commits, and a connection-holding session would pin a pooled connection for
        // that whole time -- so a conjoined store whose batch spans more tenants than the pool has
        // connections deadlocked, the batch's own commit waiting behind its own tenant sessions. That
        // is safe here precisely because this batch never commits through these sessions: it drains
        // their work trackers below and executes every operation on one connection of its own. See
        // DocumentStore.LightweightSessionWithoutHeldConnection.
        return takeParticipantsOf(_store.LightweightSessionWithoutHeldConnection(
            SessionOptions.ForDatabase(tenantId, _database)));
    }

    /// <summary>
    ///     Register an externally-created session with this batch so its pending operations, and the
    ///     transaction participants it picks up, are included in the batch's transaction.
    /// </summary>
    internal void AdoptSession(IDocumentSession session)
    {
        RegisterSession(takeParticipantsOf(session));
    }

    /// <remarks>
    ///     #867: a participant belongs to whatever commits its transaction, and for these sessions that
    ///     is this batch. They used to keep their own, and JasperFx disposes a session as soon as an
    ///     event projection's <c>ApplyAsync</c> returns (<c>ProjectionExecution</c>, and
    ///     <c>CompositeIProjectionExecution</c> here), which drained them long before the commit below
    ///     went looking. Mirrors Marten, whose ProjectionDocumentSession forwards to ProjectionUpdateBatch.
    /// </remarks>
    private IDocumentSession takeParticipantsOf(IDocumentSession session)
    {
        if (session is DocumentSessionBase sessionBase)
        {
            sessionBase.HandParticipantsTo(this);
        }

        return session;
    }

    /// <summary>
    ///     Enlist <paramref name="participant" /> in this batch's commit. This batch disposes it, on
    ///     every path, in <see cref="DisposeAsync" />.
    /// </summary>
    public void AddTransactionParticipant(ITransactionParticipant participant)
    {
        _participants.Enqueue(participant);
    }

    public override ValueTask RecordProgress(EventRange range)
    {
        var name = range.ShardName.Identity;
        var ceiling = range.SequenceCeiling;

        // #318: the progression write now rides the shared Weasel.Storage.EventStorage<TId> seam
        // (EventGraph.UpdateProgressOperation) instead of instantiating RecordProgressionOperation here.
        // Floor == 0 means the row may not exist yet, so upsert; otherwise it's already there.
        var op = _events.UpdateProgressOperation(name, ceiling, upsert: range.SequenceFloor == 0);

        _progressOps.Enqueue(op);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    ///     #420: turn the captured <see cref="StreamAction" />s into append operations that join
    ///     this batch's transaction, so raised events commit atomically with the projection
    ///     documents and the progression row they were produced alongside. Numbering the events needs
    ///     an async locking read of the stream row, which is why this is not done as they are raised.
    /// </summary>
    protected override async Task PlanRaisedEventsAsync(IReadOnlyList<StreamAction> raisedEvents,
        CancellationToken token)
    {
        var ops = new List<Weasel.Storage.IStorageOperation>();

        // One session per tenant, because each append below is bound to the session that will write it.
        // Opened before the session sweep in ExecuteOperationsInTransactionAsync, so it is picked up by it
        // and disposed with the rest.
        foreach (var group in raisedEvents.Where(a => a.Events.Any())
                     .GroupBy(a => a.TenantId ?? JasperFx.StorageConstants.DefaultTenantId))
        {
            var session = (DocumentSessionBase)SessionForTenant(group.Key);
            // One batched stream-state read per tenant group instead of one per action.
            var built = await session.BuildRaisedEventAppendsAsync(group.ToList(), token).ConfigureAwait(false);
            foreach (var op in built)
            {
                // The batch configures commands with a null session, so the append has to arrive
                // already bound to the one that will write it.
                ops.Add(new SessionBoundOperationAdapter(op, (Weasel.Storage.IStorageSession)session));
            }
        }

        _raisedEventOps = ops;
    }

    protected override async Task ExecuteOperationsInTransactionAsync(Func<CancellationToken, Task> beforeCommit,
        CancellationToken token)
    {
        // Collect all operations outside the lambda to keep state simple
        var allOps = new List<Weasel.Storage.IStorageOperation>();

        // #538: resolve the store's long-lived, per-database ensurer instead of constructing one
        // per batch. DocumentTableEnsurer memoizes on instance state, so a per-batch instance
        // re-ran the full schema check for every projected document type on every single daemon
        // batch — the same bug class as marten#4946. The store cache is keyed per connection string,
        // so scoping stays correct under database-per-tenant tenancy.
        var tableEnsurer = _store.EnsurerFor(_connectionString);

        foreach (var session in Sessions)
        {
            if (session is DocumentSessionBase sessionBase)
            {
                var workTracker = sessionBase.WorkTracker;

                if (workTracker.Operations.Count > 0)
                {
                    var providers = workTracker.Operations
                        .Select(op => op.DocumentType)
                        .Where(t => t != typeof(object))
                        .Distinct()
                        .Select(t => _store.GetProvider(t));
                    // Not inside a transaction: the batch only opens one further down.
                    await tableEnsurer.EnsureTablesAsync(providers, insideTransaction: false, token).ConfigureAwait(false);

                    // #919: a projection can take a document's id from an event's data
                    await sessionBase.AssertStringKeysFitAsync(token).ConfigureAwait(false);

                    allOps.AddRange(workTracker.Operations);
                }
            }
        }

        // #335: tenant-partitioned documents — the daemon batch executes session operations
        // directly (no DocumentSessionBase.SaveChangesAsync), so provision the partition ordinal
        // for every tenant this batch writes under before the transaction opens. The projection
        // write SQL resolves tenant_ordinal server-side from the registry these provisions
        // populate; known tenants are cached no-ops.
        if (_store.Options.Policies.DocumentTenantPartitioningEnabled)
        {
            foreach (var adapter in allOps.OfType<Polecat.Internal.Operations.ClosedShapeOperationAdapter>())
            {
                await _store.Events.TenantOrdinals.ResolveAsync(adapter.SessionTenantId, insideTransaction: false,
                    token).ConfigureAwait(false);
            }
        }

        // #420: raised-event appends ride the same transaction as the documents above.
        allOps.AddRange(_raisedEventOps);

        // Add progress operations
        while (_progressOps.TryDequeue(out var progressOp))
        {
            allOps.Add(progressOp);
        }

        // Snapshot the committed change set (document ops + any stream actions) so both the async
        // IChangeListeners below and the subscription runner (via Commit) can see what changed.
        var streams = Sessions
            .OfType<DocumentSessionBase>()
            .SelectMany(s => s.WorkTracker.Streams)
            .ToList();
        Commit = new ChangeSet(allOps, streams);

        if (allOps.Count == 0 && _participants.IsEmpty)
        {
            // jasperfx#1045: the message hook still runs as the last step before a commit, even when
            // there is nothing to commit, so a projection that only published is not lost.
            await beforeCommit(token).ConfigureAwait(false);
            return;
        }

        // Async daemon commit listeners (Projections.AsyncListeners). Suppressed during rebuilds so a
        // full replay does not re-fire post-commit side effects (mirrors Marten's ShouldApplyListeners).
        var asyncListeners = _store.Options.Projections.AsyncListeners;
        if (Mode != ShardExecutionMode.Rebuild && asyncListeners.Count > 0)
        {
            // Any tenant session works for the listener's reads/writes; create a throwaway (tracked for
            // disposal) if this batch only produced progress rows. #683: same reasoning as
            // OpenSessionForTenant -- this batch drains and executes, so the session has no commit of its
            // own to be atomic and no reason to pin a connection.
            _listenerSession = Sessions.FirstOrDefault();
            if (_listenerSession == null)
            {
                _listenerSession = _store.LightweightSessionWithoutHeldConnection(new SessionOptions());
                AdoptSession(_listenerSession);
            }

            // BeforeCommit runs before the DB commit → "at least once". A throw here aborts the batch
            // before anything is committed, so the exception is allowed to propagate.
            foreach (var listener in asyncListeners)
            {
                await listener.BeforeCommitAsync(_listenerSession, Commit, token).ConfigureAwait(false);
            }
        }

        // Collect transaction participants outside the lambda. Only after the BeforeCommit listeners
        // above, which may have enlisted one, and a retry of the lambda replays this same list.
        var participants = _participants.ToList();

        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, ops, txParticipants, beforeCommitHook, transforms) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);
            await using var tx = (await SqlTransactionRequests.BeginAsync(conn, ct).ConfigureAwait(false));

            try
            {
                if (ops.Count > 0)
                {
                    // Batch and reader must be fully disposed before transaction participants
                    // run (e.g. EF Core SaveChangesAsync), otherwise SQL Server rejects the
                    // second command with "batch is aborted / session busy".
                    var batch = new SqlBatch(conn) { Transaction = tx };
                    try
                    {
                        var builder = new BatchBuilder(batch);

                        var commandIndex = 0;
                        foreach (var operation in ops)
                        {
                            if (commandIndex > 0) builder.StartNewCommand();
                            // Every daemon-batch op carries its own session context (bespoke
                            // ops are fully bound; adapters captured their tenant session).
                            StorageOperationExecution.Configure(operation, builder, session: null);
                            commandIndex++;
                        }

                        builder.Compile();

                        try
                        {
                            var reader = await batch.ExecuteReaderAsync(ct).ConfigureAwait(false);
                            try
                            {
                                var exceptions = new List<Exception>();
                                await OperationResultSets.ApplyAsync(reader, ops, exceptions, ct).ConfigureAwait(false);

                                if (exceptions.Count > 0)
                                {
                                    throw new AggregateException(exceptions);
                                }
                            }
                            finally
                            {
                                await reader.DisposeAsync().ConfigureAwait(false);
                            }
                        }
                        catch (SqlException ex)
                            when (BatchExceptionTransforms.TryTransform(ex, batch, ops, transforms, out var transformed))
                        {
                            // The session's flush does the same (#549), so that an operation names its own
                            // error however it came to run, and a raised event's lost race reads as one.
                            ExceptionDispatchInfo.Capture(transformed!).Throw();
                        }
                    }
                    finally
                    {
                        await batch.DisposeAsync().ConfigureAwait(false);
                    }
                }

                foreach (var participant in txParticipants)
                {
                    await participant.BeforeCommitAsync(conn, tx, ct).ConfigureAwait(false);
                }

                // jasperfx#1045: the shared batch's message hook, always the last step before commit.
                await beforeCommitHook(ct).ConfigureAwait(false);

                await SqlTransactionRequests.CommitAsync(tx, ct).ConfigureAwait(false);
            }
            catch
            {
                // #783 (marten#5688): never roll back with `ct`. When it is already cancelled -- a drain
                // deadline landing beside a real failure -- DbTransaction.RollbackAsync returns a cancelled
                // task without rolling back, and its TaskCanceledException replaced the exception that
                // failed the batch. A rollback that fails for any other reason must not replace it either:
                // `await using` still disposes the transaction and the connection.
                try
                {
                    await SqlTransactionRequests.RollbackAsync(tx).ConfigureAwait(false);
                }
                catch
                {
                    // The original failure below is the one that explains the batch.
                }

                throw;
            }
        }, (_connectionString, allOps, participants, beforeCommit, _store.Options.ExceptionTransforms), token).ConfigureAwait(false);

        _committedParticipants = participants;
    }

    /// <summary>
    ///     AfterCommit runs once, after the transaction is durably committed → "at most once", and after
    ///     the message batch's own after-commit hook (see <see cref="ProjectionBatchBase{TSession}.ExecuteAsync" />
    ///     for why neither takes the batch's token). A faulting listener must not fail the batch (which
    ///     would reprocess the page and re-fire side effects), so exceptions are suppressed best-effort —
    ///     matching Marten's protective daemon behavior.
    /// </summary>
    protected override async Task AfterCommitAsync()
    {
        // #807: the participants first, for the reason the session gives: AfterCommitAsync reconciles
        // what BeforeCommitAsync left pending, so a listener must not get in its way. Unlike the session,
        // a fault here must not fail the batch: the page and its progression are committed, and a failed
        // batch would be reprocessed. So it is suppressed best-effort, as the listeners' is below.
        foreach (var participant in _committedParticipants)
        {
            try
            {
                await participant.AfterCommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // best-effort: the commit already succeeded.
            }
        }

        if (_listenerSession is null || Commit is null) return;

        foreach (var listener in _store.Options.Projections.AsyncListeners)
        {
            try
            {
                await listener.AfterCommitAsync(_listenerSession, Commit, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch
            {
                // best-effort: the commit already succeeded; a listener fault is swallowed so the
                // shard is not marked failed and the batch is not reprocessed.
            }
        }
    }

    /// <summary>
    ///     Dispose the participants this batch owns, then its sessions.
    /// </summary>
    /// <remarks>
    ///     #650 / #867: JasperFx disposes a batch on every route out of it, commit or failure, and this
    ///     is the one point a participant is known to be finished with. Dequeuing makes it safe to get
    ///     here twice, which a failing composite member does: it disposes the batch it shares and the
    ///     composite disposes that batch again. A throw from one participant must not strand the
    ///     others, or the sessions.
    /// </remarks>
    public override async ValueTask DisposeAsync()
    {
        List<Exception>? failures = null;

        while (_participants.TryDequeue(out var participant))
        {
            if (participant is not IAsyncDisposable disposable) continue;

            try
            {
                await disposable.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                (failures ??= []).Add(e);
            }
        }

        await base.DisposeAsync().ConfigureAwait(false);

        if (failures is not null)
        {
            throw failures.Count == 1 ? failures[0] : new AggregateException(failures);
        }
    }
}
