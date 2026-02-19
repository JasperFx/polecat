using Microsoft.Data.SqlClient;
using Polecat.Schema.Identity.Sequences;

namespace Polecat;

public class AdvancedOperations
{
    private readonly DocumentStore _store;

    internal AdvancedOperations(DocumentStore store)
    {
        _store = store;
    }

    public HiloSettings HiloSequenceDefaults => _store.Options.HiloSequenceDefaults;

    public Task ResetHiloSequenceFloor<T>(long floor)
    {
        var sequence = _store.Sequences.SequenceFor(typeof(T));
        return sequence.SetFloor(floor);
    }

    /// <summary>
    ///     Delete all rows from all pc_doc_* tables in the configured schema.
    /// </summary>
    public async Task CleanAllDocumentsAsync(CancellationToken token = default)
    {
        var schema = _store.Options.DatabaseSchemaName;
        await using var conn = new SqlConnection(_store.Options.ConnectionString);
        await conn.OpenAsync(token);

        // Find all pc_doc_* tables in the schema
        await using var findCmd = conn.CreateCommand();
        findCmd.CommandText = $"""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = @schema AND TABLE_NAME LIKE 'pc_doc_%'
            ORDER BY TABLE_NAME;
            """;
        findCmd.Parameters.AddWithValue("@schema", schema);

        var tables = new List<string>();
        await using (var reader = await findCmd.ExecuteReaderAsync(token))
        {
            while (await reader.ReadAsync(token))
            {
                tables.Add(reader.GetString(0));
            }
        }

        foreach (var table in tables)
        {
            await using var deleteCmd = conn.CreateCommand();
            deleteCmd.CommandText = $"DELETE FROM [{schema}].[{table}];";
            await deleteCmd.ExecuteNonQueryAsync(token);
        }
    }

    /// <summary>
    ///     Delete all rows from the document table for type T.
    /// </summary>
    public async Task CleanAsync<T>(CancellationToken token = default)
    {
        var provider = _store.GetProvider(typeof(T));
        await using var conn = new SqlConnection(_store.Options.ConnectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM {provider.Mapping.QualifiedTableName};";
        await cmd.ExecuteNonQueryAsync(token);
    }

    /// <summary>
    ///     Delete all rows from event store tables (pc_events, pc_streams, pc_event_progression).
    /// </summary>
    public async Task CleanAllEventDataAsync(CancellationToken token = default)
    {
        var events = _store.Events;
        await using var conn = new SqlConnection(_store.Options.ConnectionString);
        await conn.OpenAsync(token);

        // Delete in FK-safe order: events first, then streams, then progression
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"DELETE FROM {events.EventsTableName};";
            await cmd.ExecuteNonQueryAsync(token);
        }

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"DELETE FROM {events.StreamsTableName};";
            await cmd.ExecuteNonQueryAsync(token);
        }

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"DELETE FROM {events.ProgressionTableName};";
            await cmd.ExecuteNonQueryAsync(token);
        }
    }
}
