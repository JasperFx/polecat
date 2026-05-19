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
    private readonly string _connectionString;
    private readonly ResiliencePipeline _resilience;
    private readonly ConcurrentBag<IDocumentSession> _sessions = new();
    private readonly ConcurrentQueue<IStorageOperation> _progressOps = new();

    // Lazily created on first PublishMessageAsync call; reused for every
    // subsequent publish in this batch. Stays null when no projection
    // emits a message, so the AfterCommit hook is a no-op for the common
    // case where no message-bus integration is configured.
    private IMessageBatch? _messageBatch;
    private readonly SemaphoreSlim _messageBatchGate = new(1, 1);

    public PolecatProjectionBatch(DocumentStore store, EventGraph events, string connectionString)
    {
        _store = store;
        _events = events;
        _connectionString = connectionString;
        _resilience = store.Options.ResiliencePipeline;
    }

    public IDocumentSession SessionForTenant(string tenantId)
    {
        var session = _store.LightweightSession(new SessionOptions { TenantId = tenantId });
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
        var progressionTable = _events.ProgressionTableName;
        var extendedTracking = _events.EnableExtendedProgressionTracking;
        var name = range.ShardName.Identity;
        var ceiling = range.SequenceCeiling;

        IStorageOperation op = range.SequenceFloor == 0
            ? new ProgressMergeOperation(progressionTable, name, ceiling, extendedTracking)
            : new ProgressUpdateOperation(progressionTable, name, ceiling, extendedTracking);

        _progressOps.Enqueue(op);
        return ValueTask.CompletedTask;
    }

    public async Task ExecuteAsync(CancellationToken token)
    {
        // Collect all operations outside the lambda to keep state simple
        var allOps = new List<IStorageOperation>();
        var tableEnsurer = new DocumentTableEnsurer(
            new ConnectionFactory(_connectionString), _store.Options);

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

        // Add progress operations
        while (_progressOps.TryDequeue(out var progressOp))
        {
            allOps.Add(progressOp);
        }

        if (allOps.Count == 0 && !_sessions.Any(s => s is DocumentSessionBase sb && sb.TransactionParticipants.Any()))
        {
            return;
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
                            operation.ConfigureCommand(builder);
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
    }

    public void QuickAppendEventWithVersion(StreamAction action, IEvent @event)
    {
        // Event appending from projections is not used in Polecat's current scope
    }

    public void UpdateStreamVersion(StreamAction action)
    {
        // Stream version updates from projections are not used in Polecat's current scope
    }

    public void QuickAppendEvents(StreamAction action)
    {
        // Event appending from projections is not used in Polecat's current scope
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
