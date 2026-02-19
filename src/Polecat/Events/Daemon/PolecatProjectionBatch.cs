using System.Collections.Concurrent;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal;

namespace Polecat.Events.Daemon;

/// <summary>
///     Implements IProjectionBatch for the async daemon.
///     Accumulates operations from multiple tenant sessions and flushes them in one SQL transaction.
///     Thread-safe: composite projections may call SessionForTenant concurrently.
/// </summary>
internal class PolecatProjectionBatch : IProjectionBatch<IDocumentSession, IQuerySession>
{
    private readonly DocumentStore _store;
    private readonly EventGraph _events;
    private readonly string _connectionString;
    private readonly ConcurrentBag<IDocumentSession> _sessions = new();
    private readonly ConcurrentQueue<ProgressOperation> _progressOps = new();

    public PolecatProjectionBatch(DocumentStore store, EventGraph events, string connectionString)
    {
        _store = store;
        _events = events;
        _connectionString = connectionString;
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
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(token);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(token);

        try
        {
            // Ensure document tables exist for projected types
            var tableEnsurer = new DocumentTableEnsurer(
                new ConnectionFactory(_connectionString), _store.Options);

            // Collect all operations from all sessions
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
                        await tableEnsurer.EnsureTablesAsync(providers, token);
                    }

                    // Execute document operations
                    foreach (var operation in workTracker.Operations)
                    {
                        await using var cmd = conn.CreateCommand();
                        cmd.Transaction = tx;
                        operation.ConfigureCommand(cmd);
                        await operation.PostprocessAsync(cmd, token);
                    }
                }
            }

            // Execute progress operations
            foreach (var progressOp in _progressOps.ToArray())
            {
                await using var cmd = conn.CreateCommand();
                cmd.Transaction = tx;

                if (progressOp.Floor == 0)
                {
                    // MERGE for initial progress
                    cmd.CommandText = $"""
                        MERGE {_events.ProgressionTableName} AS target
                        USING (SELECT @name AS name) AS source ON target.name = source.name
                        WHEN MATCHED THEN UPDATE SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
                        WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                            VALUES (@name, @seq, SYSDATETIMEOFFSET());
                        """;
                }
                else
                {
                    cmd.CommandText = $"""
                        UPDATE {_events.ProgressionTableName}
                        SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
                        WHERE name = @name;
                        """;
                }

                cmd.Parameters.AddWithValue("@name", progressOp.Name);
                cmd.Parameters.AddWithValue("@seq", progressOp.Ceiling);
                await cmd.ExecuteNonQueryAsync(token);
            }

            await tx.CommitAsync(token);
        }
        catch
        {
            await tx.RollbackAsync(token);
            throw;
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
