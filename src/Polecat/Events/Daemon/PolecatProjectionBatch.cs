using System.Collections.Concurrent;
using System.Linq.Expressions;
using FastExpressionCompiler;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polly;
using Polecat.Events.Aggregation;
using Polecat.Internal;
using Polecat.Internal.Operations;
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
internal class PolecatProjectionBatch : IProjectionBatch<IDocumentSession, IQuerySession>
{
    private readonly DocumentStore _store;
    private readonly EventGraph _events;
    private readonly PolecatDatabase _database;
    private readonly string _connectionString;
    private readonly ShardExecutionMode _mode;
    private readonly ResiliencePipeline _resilience;
    private readonly ConcurrentBag<IDocumentSession> _sessions = new();
    private readonly ConcurrentQueue<Weasel.Storage.IStorageOperation> _progressOps = new();

    // #420: StreamActions a projection raised events onto, captured by the three IProjectionBatch
    // append members and turned into append operations during ExecuteAsync.
    private readonly List<StreamAction> _raisedEventActions = new();
    private readonly object _raisedEventsLock = new();

    // The change set committed by this batch, populated during ExecuteAsync. Exposed so the
    // subscription runner can hand it to the IChangeListener returned by ProcessEventsAsync.
    public IChangeSet? Commit { get; private set; }

    // Lazily created on first PublishMessageAsync call; reused for every
    // subsequent publish in this batch. Stays null when no projection
    // emits a message, so the AfterCommit hook is a no-op for the common
    // case where no message-bus integration is configured.
    private IMessageBatch? _messageBatch;
    private readonly SemaphoreSlim _messageBatchGate = new(1, 1);

    public PolecatProjectionBatch(DocumentStore store, EventGraph events, PolecatDatabase database,
        ShardExecutionMode mode = ShardExecutionMode.Continuous)
    {
        _store = store;
        _events = events;
        _database = database;
        _connectionString = database.ConnectionString;
        _mode = mode;
        _resilience = store.Options.ResiliencePipeline;
    }

    public IDocumentSession SessionForTenant(string tenantId)
    {
        // #514: bind to the database this batch is working rather than routing tenantId through the
        // tenancy. The batch drains these sessions' work trackers and executes the operations
        // against _connectionString, so writes were already landing correctly — but the sessions'
        // own READS (loading an existing projected document before applying to it) went wherever
        // tenant routing sent them, and under database-per-tenant tenancy "*DEFAULT*" maps to no
        // tenant at all. Mirrors Marten's SessionOptions.ForDatabase.
        var session = _store.LightweightSession(SessionOptions.ForDatabase(tenantId, _database));
        _sessions.Add(session);
        return session;
    }

    /// <summary>
    ///     Register an externally-created session with this batch so its pending
    ///     operations are included in the batch's transaction.
    /// </summary>
    internal void RegisterSession(IDocumentSession session)
    {
        _sessions.Add(session);
    }

    public ValueTask RecordProgress(EventRange range)
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

    public async Task ExecuteAsync(CancellationToken token)
    {
        // Collect all operations outside the lambda to keep state simple
        var allOps = new List<Weasel.Storage.IStorageOperation>();

        // #538: resolve the store's long-lived, per-database ensurer instead of constructing one
        // per batch. DocumentTableEnsurer memoizes on instance state, so a per-batch instance
        // re-ran the full schema check (SchemaMigration.DetermineAsync + version-column widening +
        // index DDL) for every projected document type on every single daemon batch — the same bug
        // class as marten#4946. The store cache is keyed per connection string, so scoping stays
        // correct under database-per-tenant tenancy.
        var tableEnsurer = _store.EnsurerFor(_connectionString);

        // #420: built before the session sweep below so any tenant session this creates is picked
        // up by it (and disposed with the rest). The appends themselves are added after the
        // document operations, ahead of the progression row.
        var raisedEventOps = await BuildRaisedEventOperationsAsync(token);

        foreach (var session in _sessions)
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
                    await tableEnsurer.EnsureTablesAsync(providers, token);

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
                await _store.Events.TenantOrdinals.ResolveAsync(adapter.SessionTenantId, token);
            }
        }

        // #420: raised-event appends ride the same transaction as the documents above.
        allOps.AddRange(raisedEventOps);

        // Add progress operations
        while (_progressOps.TryDequeue(out var progressOp))
        {
            allOps.Add(progressOp);
        }

        // Snapshot the committed change set (document ops + any stream actions) so both the async
        // IChangeListeners below and the subscription runner (via Commit) can see what changed.
        var streams = _sessions
            .OfType<DocumentSessionBase>()
            .SelectMany(s => s.WorkTracker.Streams)
            .ToList();
        Commit = new ChangeSet(allOps, streams);

        if (allOps.Count == 0 && !_sessions.Any(s => s is DocumentSessionBase sb && sb.TransactionParticipants.Any()))
        {
            return;
        }

        // Async daemon commit listeners (Projections.AsyncListeners). Suppressed during rebuilds so a
        // full replay does not re-fire post-commit side effects (mirrors Marten's ShouldApplyListeners).
        var asyncListeners = _store.Options.Projections.AsyncListeners;
        var applyListeners = _mode != ShardExecutionMode.Rebuild && asyncListeners.Count > 0;
        IDocumentSession? listenerSession = null;
        if (applyListeners)
        {
            // Any tenant session works for the listener's reads/writes; create a throwaway (tracked for
            // disposal) if this batch only produced progress rows.
            listenerSession = _sessions.FirstOrDefault();
            if (listenerSession == null)
            {
                listenerSession = _store.LightweightSession();
                _sessions.Add(listenerSession);
            }

            // BeforeCommit runs before the DB commit → "at least once". A throw here aborts the batch
            // before anything is committed, so the exception is allowed to propagate.
            foreach (var listener in asyncListeners)
            {
                await listener.BeforeCommitAsync(listenerSession, Commit, token).ConfigureAwait(false);
            }
        }

        // Collect transaction participants outside the lambda
        var participants = _sessions
            .OfType<DocumentSessionBase>()
            .SelectMany(s => s.TransactionParticipants)
            .ToList();

        // Snapshot the message batch (may be null when no projection in this
        // batch published a message). Passed via state into the resilience
        // lambda so BeforeCommitAsync runs inside the SQL transaction; the
        // AfterCommitAsync hook fires once below, after the resilience
        // pipeline returns successfully.
        var messageBatch = _messageBatch;

        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, ops, txParticipants, msgBatch) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);
            await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(ct);

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
                        var reader = await batch.ExecuteReaderAsync(ct);
                        try
                        {
                            var exceptions = new List<Exception>();
                            for (var i = 0; i < ops.Count; i++)
                            {
                                await ops[i].PostprocessAsync(reader, exceptions, ct);
                                if (i < ops.Count - 1)
                                {
                                    await reader.NextResultAsync(ct);
                                }
                            }

                            if (exceptions.Count > 0)
                            {
                                throw new AggregateException(exceptions);
                            }
                        }
                        finally
                        {
                            await reader.DisposeAsync();
                        }
                    }
                    finally
                    {
                        await batch.DisposeAsync();
                    }
                }

                foreach (var participant in txParticipants)
                {
                    await participant.BeforeCommitAsync(conn, tx, ct);
                }

                if (msgBatch is not null)
                {
                    await msgBatch.BeforeCommitAsync(ct);
                }

                await tx.CommitAsync(ct);
            }
            catch
            {
                await tx.RollbackAsync(ct);
                throw;
            }
        }, (_connectionString, allOps, participants, messageBatch), token);

        // Outside the resilience pipeline so the post-commit hook does not
        // re-fire on a transient retry — by definition the SQL transaction
        // has committed exactly once by the time we reach here.
        if (messageBatch is not null)
        {
            await messageBatch.AfterCommitAsync(token).ConfigureAwait(false);
        }

        // AfterCommit runs once, after the transaction is durably committed → "at most once". A faulting
        // listener must not fail the batch (which would reprocess the page and re-fire side effects), so
        // exceptions are suppressed best-effort — matching Marten's protective daemon behavior.
        if (applyListeners)
        {
            foreach (var listener in asyncListeners)
            {
                try
                {
                    await listener.AfterCommitAsync(listenerSession!, Commit, token).ConfigureAwait(false);
                }
                catch
                {
                    // best-effort: the commit already succeeded; a listener fault is swallowed so the
                    // shard is not marked failed and the batch is not reprocessed.
                }
            }
        }
    }

    // #420: the three members JasperFx drives event-raising through. EventSlice.BuildOperations
    // calls them whenever a projection raises an event; all three were empty, so an async
    // projection that appends events lost them silently -- no exception, no shard failure, no dead
    // letter, no log line. The documents committed, the progression row advanced, and the raised
    // events simply never existed.
    //
    // All three funnel into one list because the StreamAction JasperFx hands over already carries
    // every raised event, and the single-stream-start path passes the SAME instance to each call
    // (once per event to QuickAppendEventWithVersion, then once to UpdateStreamVersion), so
    // reference identity dedupes them into one append per stream. The actual operation is built in
    // ExecuteAsync, because numbering the events correctly needs an async locking read of the
    // stream row that these synchronous members cannot do.

    public void QuickAppendEventWithVersion(StreamAction action, IEvent @event) => CaptureRaisedEvents(action);

    public void UpdateStreamVersion(StreamAction action) => CaptureRaisedEvents(action);

    public void QuickAppendEvents(StreamAction action) => CaptureRaisedEvents(action);

    private void CaptureRaisedEvents(StreamAction action)
    {
        lock (_raisedEventsLock)
        {
            // Reference identity, not stream id: two distinct actions for the same stream in one
            // batch are two real appends, while the same instance arriving three times is one.
            foreach (var captured in _raisedEventActions)
            {
                if (ReferenceEquals(captured, action)) return;
            }

            _raisedEventActions.Add(action);
        }
    }

    /// <summary>
    ///     #420: turn the captured <see cref="StreamAction" />s into append operations that join
    ///     this batch's transaction, so raised events commit atomically with the projection
    ///     documents and the progression row they were produced alongside.
    /// </summary>
    private async Task<List<Weasel.Storage.IStorageOperation>> BuildRaisedEventOperationsAsync(
        CancellationToken token)
    {
        List<StreamAction> actions;
        lock (_raisedEventsLock)
        {
            actions = _raisedEventActions.ToList();
        }

        var ops = new List<Weasel.Storage.IStorageOperation>();
        if (actions.Count == 0) return ops;

        // One session per tenant: the stream-state read is scoped by the session's tenant id.
        foreach (var group in actions.Where(a => a.Events.Any())
                     .GroupBy(a => a.TenantId ?? JasperFx.StorageConstants.DefaultTenantId))
        {
            var session = (DocumentSessionBase)SessionForTenant(group.Key);
            foreach (var action in group)
            {
                var op = await session.BuildRaisedEventAppendAsync(action, token).ConfigureAwait(false);
                // The batch configures commands with a null session, so the append has to arrive
                // already bound to the one that will write it.
                ops.Add(new SessionBoundOperationAdapter(op, (Weasel.Storage.IStorageSession)session));
            }
        }

        return ops;
    }

    public async Task PublishMessageAsync(object message, string tenantId)
    {
        var batch = await CurrentMessageBatchAsync().ConfigureAwait(false);
        await PublishToBatchAsync(batch, message, tenantId).ConfigureAwait(false);
    }

    public async Task PublishMessageAsync(object message, MessageMetadata metadata)
    {
        var batch = await CurrentMessageBatchAsync().ConfigureAwait(false);
        await PublishToBatchAsync(batch, message, metadata).ConfigureAwait(false);
    }

    /// <summary>
    ///     Lazily obtain the per-batch <see cref="IMessageBatch"/> from the
    ///     configured <see cref="IMessageOutbox"/>. Created on first publish;
    ///     stays null if no projection in this batch ever publishes a message,
    ///     which keeps the AfterCommit hook a no-op for the common case.
    /// </summary>
    private async ValueTask<IMessageBatch> CurrentMessageBatchAsync()
    {
        if (_messageBatch is not null) return _messageBatch;

        await _messageBatchGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_messageBatch is not null) return _messageBatch;

            var session = _sessions.FirstOrDefault()
                ?? throw new InvalidOperationException(
                    "Cannot publish a message from a projection batch with no document session.");

            _messageBatch = await _store.Options.Events.MessageOutbox
                .CreateBatch(session)
                .ConfigureAwait(false);

            return _messageBatch;
        }
        finally
        {
            _messageBatchGate.Release();
        }
    }

    // Cached generic method definitions — `IMessageSink.PublishAsync<T>` takes
    // T as the first parameter, so a `GetMethod(name, [typeof(object), …])`
    // lookup misses (it doesn't match the generic parameter). Filter by arity
    // and second-param type instead, then close over the runtime message type.
    private static readonly System.Reflection.MethodInfo PublishWithTenantMethod = typeof(IMessageSink)
        .GetMethods()
        .First(m => m.Name == nameof(IMessageSink.PublishAsync)
                    && m.IsGenericMethodDefinition
                    && m.GetParameters() is { Length: 2 } parms
                    && parms[1].ParameterType == typeof(string));

    private static readonly System.Reflection.MethodInfo PublishWithMetadataMethod = typeof(IMessageSink)
        .GetMethods()
        .First(m => m.Name == nameof(IMessageSink.PublishAsync)
                    && m.IsGenericMethodDefinition
                    && m.GetParameters() is { Length: 2 } parms
                    && parms[1].ParameterType == typeof(MessageMetadata));

    // Polecat#46 cold-start row: per-message-type delegate caches for the
    // batch-publish dispatch. Each closes IMessageSink.PublishAsync<T> over
    // the runtime message type exactly once, then reuses the compiled delegate
    // for every subsequent publish of the same type. Replaces the prior
    // MakeGenericMethod + MethodInfo.Invoke per call. Sibling pattern to
    // Marten#4308's LINQ handler-factory cache, applied to method-closing
    // (rather than type-closing) generics.
    private static readonly ConcurrentDictionary<Type, Func<IMessageBatch, object, string, ValueTask>>
        _publishWithTenantDelegates = new();

    private static readonly ConcurrentDictionary<Type, Func<IMessageBatch, object, MessageMetadata, ValueTask>>
        _publishWithMetadataDelegates = new();

    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Closes IMessageSink.PublishAsync<T> over runtime message type via MethodInfo.MakeGenericMethod for the per-type cache miss; cached delegate amortizes the cost across subsequent calls.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "FastExpressionCompiler.CompileFast inside the cache-miss path; AOT consumers must pre-register the message types they publish per the Polecat AOT publishing guide so this method is never reached at runtime under trimming.")]
    private static Func<IMessageBatch, object, string, ValueTask> BuildPublishWithTenantDelegate(Type messageType)
    {
        var closed = PublishWithTenantMethod.MakeGenericMethod(messageType);
        var batchParam = Expression.Parameter(typeof(IMessageBatch), "batch");
        var messageParam = Expression.Parameter(typeof(object), "message");
        var tenantParam = Expression.Parameter(typeof(string), "tenantId");

        var call = Expression.Call(
            batchParam,
            closed,
            Expression.Convert(messageParam, messageType),
            tenantParam);

        return Expression.Lambda<Func<IMessageBatch, object, string, ValueTask>>(
            call, batchParam, messageParam, tenantParam).CompileFast();
    }

    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Closes IMessageSink.PublishAsync<T> over runtime message type via MethodInfo.MakeGenericMethod for the per-type cache miss; cached delegate amortizes the cost across subsequent calls.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "FastExpressionCompiler.CompileFast inside the cache-miss path; AOT consumers must pre-register the message types they publish per the Polecat AOT publishing guide so this method is never reached at runtime under trimming.")]
    private static Func<IMessageBatch, object, MessageMetadata, ValueTask> BuildPublishWithMetadataDelegate(Type messageType)
    {
        var closed = PublishWithMetadataMethod.MakeGenericMethod(messageType);
        var batchParam = Expression.Parameter(typeof(IMessageBatch), "batch");
        var messageParam = Expression.Parameter(typeof(object), "message");
        var metadataParam = Expression.Parameter(typeof(MessageMetadata), "metadata");

        var call = Expression.Call(
            batchParam,
            closed,
            Expression.Convert(messageParam, messageType),
            metadataParam);

        return Expression.Lambda<Func<IMessageBatch, object, MessageMetadata, ValueTask>>(
            call, batchParam, messageParam, metadataParam).CompileFast();
    }

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Forwards a method-group reference to BuildPublishWithTenantDelegate, which is [RequiresDynamicCode]. The cascade stops here because IProjectionBatch.PublishMessageAsync (the interface boundary) is not RDC-annotated; propagating the constraint upward would be a downstream API break. Cache-miss path is at most once per message type — AOT consumers pre-register message types per the Polecat AOT publishing guide.")]
    private static ValueTask PublishToBatchAsync(IMessageBatch batch, object message, string tenantId)
    {
        // Lookup-or-compile the per-message-type dispatch delegate; steady-state
        // calls hit the ConcurrentDictionary directly with no reflective work.
        var dispatch = _publishWithTenantDelegates.GetOrAdd(message.GetType(), BuildPublishWithTenantDelegate);
        return dispatch(batch, message, tenantId);
    }

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Forwards a method-group reference to BuildPublishWithMetadataDelegate, which is [RequiresDynamicCode]. The cascade stops here because IProjectionBatch.PublishMessageAsync (the interface boundary) is not RDC-annotated; propagating the constraint upward would be a downstream API break. Cache-miss path is at most once per message type — AOT consumers pre-register message types per the Polecat AOT publishing guide.")]
    private static ValueTask PublishToBatchAsync(IMessageBatch batch, object message, MessageMetadata metadata)
    {
        var dispatch = _publishWithMetadataDelegates.GetOrAdd(message.GetType(), BuildPublishWithMetadataDelegate);
        return dispatch(batch, message, metadata);
    }

    public async ValueTask DisposeAsync()
    {
        _messageBatchGate.Dispose();

        foreach (var session in _sessions)
        {
            await session.DisposeAsync();
        }
    }
}
