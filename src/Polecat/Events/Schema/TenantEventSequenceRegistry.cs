using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Polly;
using Weasel.SqlServer;
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
///       <item>creates the tenant's <c>pc_events_sequence_{ordinal}</c> SEQUENCE object through Weasel
///       (idempotent, serialized per ordinal via <c>sp_getapplock</c>, on its own connection so it never
///       rolls back with the caller's append, all through the store resilience pipeline).</item>
///     </list>
///     Subsequent appends hit the in-memory cache. The ordinal is both the physical partition value
///     written to <c>tenant_ordinal</c> and the suffix of the tenant's event sequence.
/// </summary>
internal sealed class TenantEventSequenceRegistry
{
    /// <summary>
    ///     Nothing holds this lock for longer than a <c>CREATE SEQUENCE</c>, so waiting for it is only ever
    ///     a matter of tenants registering at once.
    /// </summary>
    private const int SequenceLockTimeoutMilliseconds = 30_000;

    private readonly TenantPartitionOrdinalRegistry _ordinals;
    private readonly string _connectionString;
    private readonly string _schemaName;
    private readonly ResiliencePipeline _resilience;

    private readonly ConcurrentDictionary<string, TenantStorage> _cache = new(StringComparer.Ordinal);

    public TenantEventSequenceRegistry(TenantPartitionOrdinalRegistry ordinals,
        string connectionString, string schemaName, ResiliencePipeline resilience)
    {
        _ordinals = ordinals;
        _connectionString = connectionString;
        _schemaName = schemaName;
        _resilience = resilience;
    }

    /// <summary>
    ///     The tenant's partition ordinal and fully-qualified per-tenant sequence name
    ///     (e.g. <c>3</c>, <c>[dbo].[pc_events_sequence_3]</c>), provisioning both on first use.
    /// </summary>
    /// <param name="tenantId">The tenant.</param>
    /// <param name="insideTransaction">See <see cref="TenantPartitionOrdinalRegistry.ResolveAsync" />.</param>
    /// <param name="token">The token.</param>
    public async ValueTask<TenantStorage> ResolveAsync(string tenantId, bool insideTransaction,
        CancellationToken token)
    {
        // Allocate the tenant ordinal + SPLIT every managed-partitioned table (idempotent),
        // through the store's shared ordinal registry (#335) so document writes and the
        // stream-row SQL see the same tenant -> ordinal cache. Asked before the cache below, because a
        // tenant can have its sequence and still be owed a SPLIT (#892).
        var ordinal = await _ordinals.ResolveAsync(tenantId, insideTransaction, token).ConfigureAwait(false);

        if (_cache.TryGetValue(tenantId, out var cached)) return cached;

        // Ensure the tenant's per-tenant event sequence exists.
        await EnsureSequenceAsync(ordinal, token).ConfigureAwait(false);

        var storage = new TenantStorage(ordinal,
            Polecat.Internal.SqlEscaping.QualifiedName(_schemaName, $"pc_events_sequence_{ordinal}"));
        return _cache.GetOrAdd(tenantId, storage);
    }

    /// <summary>
    ///     Evict a removed tenant so a later append re-provisions instead of reusing a stale
    ///     ordinal/sequence pairing (#335 runtime tenant removal).
    /// </summary>
    public void Evict(string tenantId) => _cache.TryRemove(tenantId, out _);

    /// <summary>
    ///     Imperative sequence provisioning for <see cref="AdvancedOperations.AddPolecatManagedTenantsAsync(CancellationToken,string[])" />
    ///     — creates <c>pc_events_sequence_{ordinal}</c> for each ordinal (idempotent).
    /// </summary>
    public async Task EnsureSequencesForOrdinalsAsync(IEnumerable<int> ordinals, CancellationToken token)
    {
        foreach (var ordinal in ordinals.Distinct())
        {
            await EnsureSequenceAsync(ordinal, token).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     A tenant's event sequence as a Weasel schema object. One per ordinal, known only once a tenant
    ///     registers, so it isn't part of the event store's feature schema: the migration can't declare it,
    ///     and this creates or drops it at the point the tenant comes or goes.
    /// </summary>
    internal static Sequence SequenceFor(string schemaName, int ordinal)
        => new(new SqlServerObjectName(schemaName, $"pc_events_sequence_{ordinal}"));

    private async Task EnsureSequenceAsync(int ordinal, CancellationToken token)
    {
        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, schema, ordinal) = state;
            var resource = SequenceFor(schema, ordinal).Identifier.Name;

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            // Weasel's create is guarded so that it can run twice, not so that it can run twice at once.
            // Two callers could both pass the guard, and the loser would fail on a name it was happy to find.
            await conn.GetGlobalLock(resource, ct, lockTimeoutMs: SequenceLockTimeoutMilliseconds)
                .ConfigureAwait(false);

            try
            {
                // #922: a sequence that has to be created starts past whatever the ordinal still holds. A
                // tenant removed with RetainData leaves its events behind and drops its sequence, so a
                // fresh one restarting at 1 hands out seq_ids the retained rows already own, whether the
                // same tenant comes back or another takes the ordinal. That's a primary key violation,
                // reported as a false ExistingStreamIdCollisionException on a brand new stream id.
                var sequence = new Sequence(SequenceFor(schema, ordinal).Identifier,
                    await nextSeqIdAsync(conn, schema, ordinal, ct).ConfigureAwait(false));
                await sequence.CreateAsync(conn, ct).ConfigureAwait(false);
            }
            finally
            {
                await conn.ReleaseGlobalLock(resource, CancellationToken.None).ConfigureAwait(false);
            }
        }, (_connectionString, _schemaName, ordinal), token).ConfigureAwait(false);
    }

    // 1 for a sequence that exists already (its start value is never read again) or an ordinal with no events
    private static async Task<long> nextSeqIdAsync(SqlConnection conn, string schema, int ordinal,
        CancellationToken ct)
    {
        await using var cmd = new SqlCommand($"""
            IF OBJECT_ID(@sequence, N'SO') IS NULL
                SELECT MAX(seq_id) FROM {Polecat.Internal.SqlEscaping.QualifiedName(schema, "pc_events")}
                WHERE tenant_ordinal = @ordinal;
            ELSE
                SELECT CAST(NULL AS bigint);
            """, conn);
        cmd.Parameters.Add("@sequence", System.Data.SqlDbType.NVarChar, 300).Value =
            SequenceFor(schema, ordinal).Identifier.QualifiedName;
        cmd.Parameters.Add("@ordinal", System.Data.SqlDbType.Int).Value = ordinal;

        return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) is long max ? max + 1 : 1;
    }

    internal readonly record struct TenantStorage(int Ordinal, string SequenceName);
}
