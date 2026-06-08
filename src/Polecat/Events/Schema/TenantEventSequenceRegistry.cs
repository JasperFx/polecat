using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Polly;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Events.Schema;

/// <summary>
///     Provisions and caches each tenant's storage when
///     <see cref="EventGraph.UseTenantPartitionedEvents" /> is enabled (#163 / polecat#171).
///
///     The first time a tenant appends events this:
///     <list type="number">
///       <item>allocates the tenant's compact integer <c>ordinal</c> and SPLITs the physical
///       per-tenant partition of <c>pc_events</c> via Weasel.SqlServer's
///       <see cref="ManagedTenantPartitions" /> (idempotent — existing tenants return their ordinal
///       with no DDL), and</item>
///       <item>creates the tenant's <c>pc_events_sequence_{ordinal}</c> SEQUENCE object (idempotent,
///       serialized per ordinal via <c>sp_getapplock</c>, its own short transaction so it never rolls
///       back with the caller's append, all through the store resilience pipeline).</item>
///     </list>
///     Subsequent appends hit the in-memory cache. The ordinal is both the physical partition value
///     written to <c>tenant_ordinal</c> and the suffix of the tenant's event sequence.
/// </summary>
internal sealed class TenantEventSequenceRegistry
{
    private readonly ManagedTenantPartitions _partitions;
    private readonly PolecatDatabase _database;
    private readonly string _connectionString;
    private readonly string _schemaName;
    private readonly ResiliencePipeline _resilience;

    private readonly ConcurrentDictionary<string, TenantStorage> _cache = new(StringComparer.Ordinal);

    public TenantEventSequenceRegistry(ManagedTenantPartitions partitions, PolecatDatabase database,
        string connectionString, string schemaName, ResiliencePipeline resilience)
    {
        _partitions = partitions;
        _database = database;
        _connectionString = connectionString;
        _schemaName = schemaName;
        _resilience = resilience;
    }

    /// <summary>
    ///     The tenant's partition ordinal and fully-qualified per-tenant sequence name
    ///     (e.g. <c>3</c>, <c>[dbo].[pc_events_sequence_3]</c>), provisioning both on first use.
    /// </summary>
    public async ValueTask<TenantStorage> ResolveAsync(string tenantId, CancellationToken token)
    {
        if (_cache.TryGetValue(tenantId, out var cached)) return cached;

        // Allocate the tenant ordinal + SPLIT the physical pc_events partition (idempotent).
        var ordinal = await _partitions.AddPartitionToAllTables(_database, tenantId, token)
            .ConfigureAwait(false);

        // Ensure the tenant's per-tenant event sequence exists.
        await EnsureSequenceAsync(ordinal, token).ConfigureAwait(false);

        var storage = new TenantStorage(ordinal, $"[{_schemaName}].[pc_events_sequence_{ordinal}]");
        return _cache.GetOrAdd(tenantId, storage);
    }

    private async Task EnsureSequenceAsync(int ordinal, CancellationToken token)
    {
        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, schema, ordinal) = state;

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                DECLARE @seq sysname = N'pc_events_sequence_' + CAST(@ordinal AS varchar(20));
                BEGIN TRANSACTION;
                EXEC sp_getapplock @Resource = @seq, @LockMode = 'Exclusive', @LockOwner = 'Transaction';
                IF NOT EXISTS (
                    SELECT 1 FROM sys.sequences sq
                    JOIN sys.schemas sc ON sq.schema_id = sc.schema_id
                    WHERE sq.name = @seq AND sc.name = @schema)
                BEGIN
                    DECLARE @ddl nvarchar(max) =
                        N'CREATE SEQUENCE [' + @schema + N'].[' + @seq + N'] AS bigint START WITH 1 INCREMENT BY 1;';
                    EXEC sp_executesql @ddl;
                END
                COMMIT TRANSACTION;
                """;
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@ordinal", ordinal);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, (_connectionString, _schemaName, ordinal), token).ConfigureAwait(false);
    }

    internal readonly record struct TenantStorage(int Ordinal, string SequenceName);
}
