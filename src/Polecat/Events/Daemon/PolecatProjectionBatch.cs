using System.Collections.Concurrent;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polly;
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

        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, ops, txParticipants) = state;
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

                await tx.CommitAsync(ct);
            }
            catch
            {
                await tx.RollbackAsync(ct);
                throw;
            }
        }, (_connectionString, allOps, participants), token);
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

    public Task PublishMessageAsync(object message, string tenantId)
    {
        // Message bus support deferred
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var session in _sessions)
        {
            await session.DisposeAsync();
        }
    }
}
