using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Polly;

namespace Polecat.Events.Schema;

/// <summary>
///     Resolves (and lazily provisions) the per-tenant event sequence used by the append path when
///     <see cref="EventGraph.UseTenantPartitionedEvents" /> is enabled (#163 Phase 1).
///
///     The first time a tenant appends events, this registers the tenant in
///     <see cref="TenantPartitionsTable" /> (assigning a compact <c>partition_id</c>) and creates the
///     tenant's <c>pc_events_sequence_{partition_id}</c> SEQUENCE object. Subsequent appends hit the
///     in-memory cache. The provisioning step is its own short transaction (idempotent, serialized per
///     tenant via <c>sp_getapplock</c>) so it never depends on, or rolls back with, the caller's append
///     transaction — and it runs entirely through the store's resilience pipeline per the project's
///     "all command execution goes through ResiliencePipeline" rule.
/// </summary>
internal sealed class TenantEventSequenceRegistry
{
    private readonly string _connectionString;
    private readonly string _schemaName;
    private readonly ResiliencePipeline _resilience;
    private readonly ConcurrentDictionary<string, string> _cache = new(StringComparer.Ordinal);

    public TenantEventSequenceRegistry(string connectionString, string schemaName, ResiliencePipeline resilience)
    {
        _connectionString = connectionString;
        _schemaName = schemaName;
        _resilience = resilience;
    }

    /// <summary>
    ///     Returns the fully-qualified per-tenant sequence name (e.g. <c>[dbo].[pc_events_sequence_3]</c>),
    ///     provisioning the registry row and sequence object on first use for the tenant.
    /// </summary>
    public async ValueTask<string> ResolveSequenceNameAsync(string tenantId, CancellationToken token)
    {
        if (_cache.TryGetValue(tenantId, out var cached)) return cached;

        var partitionId = await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, schema, tenantId) = state;

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            // Serialize per-tenant so concurrent first-appends don't race on the INSERT / CREATE SEQUENCE.
            cmd.CommandText = $"""
                SET NOCOUNT ON;
                DECLARE @pid int;
                BEGIN TRANSACTION;
                EXEC sp_getapplock @Resource = @lock, @LockMode = 'Exclusive', @LockOwner = 'Transaction';

                MERGE [{schema}].[pc_tenant_partitions] WITH (HOLDLOCK) AS target
                USING (SELECT @tenant AS tenant_id) AS source ON target.tenant_id = source.tenant_id
                WHEN NOT MATCHED THEN INSERT (tenant_id) VALUES (@tenant);

                SELECT @pid = partition_id FROM [{schema}].[pc_tenant_partitions] WHERE tenant_id = @tenant;

                DECLARE @seq sysname = N'pc_events_sequence_' + CAST(@pid AS varchar(20));
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
                SELECT @pid;
                """;
            cmd.Parameters.AddWithValue("@tenant", tenantId);
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@lock", $"pc_tenant_partition_{tenantId}");

            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return Convert.ToInt32(result);
        }, (_connectionString, _schemaName, tenantId), token).ConfigureAwait(false);

        var sequenceName = $"[{_schemaName}].[pc_events_sequence_{partitionId}]";
        return _cache.GetOrAdd(tenantId, sequenceName);
    }
}
