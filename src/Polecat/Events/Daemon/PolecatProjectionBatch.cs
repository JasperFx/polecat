using System.Collections.Concurrent;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polly;
using Polecat.Internal;
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
    private readonly ConcurrentQueue<ProgressOperation> _progressOps = new();

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
        _progressOps.Enqueue(new ProgressOperation(range.ShardName.Identity, range.SequenceFloor, range.SequenceCeiling));
        return ValueTask.CompletedTask;
    }

    public async Task ExecuteAsync(CancellationToken token)
    {
        await _resilience.ExecuteAsync(async (_, ct) =>
        {
            await using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync(ct);
            await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(ct);

            try
            {
                // Ensure document tables exist for projected types
                var tableEnsurer = new DocumentTableEnsurer(
                    new ConnectionFactory(_connectionString), _store.Options);

                // Collect all operations from all sessions and progress ops into a single batch
                var allOps = new List<IStorageOperation>();

                foreach (var session in _sessions)
                {
                    if (session is DocumentSessionBase sessionBase)
                    {
                        var workTracker = sessionBase.WorkTracker;

                        // Ensure projected document tables exist (skip non-document ops like FlatTable)
                        if (workTracker.Operations.Count > 0)
                        {
                            var providers = workTracker.Operations
                                .Select(op => op.DocumentType)
                                .Where(t => t != typeof(object))
                                .Distinct()
                                .Select(t => _store.GetProvider(t));
                            await tableEnsurer.EnsureTablesAsync(providers, ct);

                            allOps.AddRange(workTracker.Operations);
                        }
                    }
                }

                // Execute all document operations + progress operations in a single SqlBatch
                var progressArray = _progressOps.ToArray();
                var totalCommands = allOps.Count + progressArray.Length;

                if (totalCommands > 0)
                {
                    await using var batch = new SqlBatch(conn);
                    batch.Transaction = tx;
                    var builder = new BatchBuilder(batch);

                    var commandIndex = 0;

                    // Document operations
                    foreach (var operation in allOps)
                    {
                        if (commandIndex > 0) builder.StartNewCommand();
                        operation.ConfigureCommand(builder);
                        commandIndex++;
                    }

                    // Progress operations
                    foreach (var progressOp in progressArray)
                    {
                        if (commandIndex > 0) builder.StartNewCommand();

                        if (progressOp.Floor == 0)
                        {
                            builder.Append($"""
                                MERGE {_events.ProgressionTableName} AS target
                                USING (SELECT @name AS name) AS source ON target.name = source.name
                                WHEN MATCHED THEN UPDATE SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
                                WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                                    VALUES (@name, @seq, SYSDATETIMEOFFSET());
                                """);
                        }
                        else
                        {
                            builder.Append($"""
                                UPDATE {_events.ProgressionTableName}
                                SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
                                WHERE name = @name;
                                """);
                        }

                        builder.AddParameters(new Dictionary<string, object?>
                        {
                            ["name"] = progressOp.Name,
                            ["seq"] = progressOp.Ceiling
                        });

                        commandIndex++;
                    }

                    builder.Compile();
                    await using var reader = await batch.ExecuteReaderAsync(ct);

                    // Process document operation results
                    var exceptions = new List<Exception>();
                    for (var i = 0; i < allOps.Count; i++)
                    {
                        await allOps[i].PostprocessAsync(reader, exceptions, ct);
                        if (i < totalCommands - 1)
                        {
                            await reader.NextResultAsync(ct);
                        }
                    }

                    if (exceptions.Count > 0)
                    {
                        throw new AggregateException(exceptions);
                    }
                    // Progress ops don't need result processing
                }

                // Call transaction participants (e.g., EF Core DbContext) before commit
                foreach (var session in _sessions)
                {
                    if (session is DocumentSessionBase sessionBase)
                    {
                        foreach (var participant in sessionBase.TransactionParticipants)
                        {
                            await participant.BeforeCommitAsync(conn, tx, ct);
                        }
                    }
                }

                await tx.CommitAsync(ct);
            }
            catch
            {
                await tx.RollbackAsync(ct);
                throw;
            }
        }, token);
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

    private record ProgressOperation(string Name, long Floor, long Ceiling);
}
