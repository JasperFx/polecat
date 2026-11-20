using System.Data;
using System.Globalization;
using System.Text;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Schema;
using Polecat.Internal;

namespace Polecat.Storage;

// marten#6003 / #1053: the parked identities and the slice index, over pc_parked_identities and the
// pc_slice_index_* tables
public partial class PolecatDatabase
{
    private const string ParkedColumns = "projection_name, projection_version, shard_key, tenant_id, identity_text, "
                                         + "identity_type, first_failing_seq, resume_after, failure_category, "
                                         + "exception_type, exception_message, event_type, event_id, failing_stream, "
                                         + "parked_at, replay_attempts, last_attempt_at, last_attempt_error, "
                                         + "requested_action, requested_at";

    private const string ParkedOrder = "ORDER BY parked_at, first_failing_seq, identity_text";

    /// <inheritdoc />
    public async Task<IReadOnlyList<ParkedIdentity>> FetchParkedIdentitiesAsync(ParkedIdentityQuery query,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        if (!_events.EnableParkedIdentities || query.Limit <= 0) return [];

        await EnsureParkedIdentitiesAsync(token).ConfigureAwait(false);

        var where = new StringBuilder("1 = 1");
        var parameters = new List<SqlParameter> { new("@limit", SqlDbType.Int) { Value = query.Limit } };
        if (query.ProjectionName != null)
        {
            where.Append(" AND projection_name = @name");
            parameters.Add(keyParameter("@name", query.ProjectionName));
        }

        if (query.ProjectionVersion != null)
        {
            where.Append(" AND projection_version = @version");
            parameters.Add(new SqlParameter("@version", SqlDbType.Int) { Value = (int)query.ProjectionVersion });
        }

        if (query.ShardKey != null)
        {
            where.Append(" AND shard_key = @shard");
            parameters.Add(keyParameter("@shard", query.ShardKey));
        }

        if (query.TenantId != null)
        {
            where.Append(" AND tenant_id = @tenant");
            parameters.Add(keyParameter("@tenant", query.TenantId));
        }

        return await readParkedAsync(
            $"SELECT TOP(@limit) {ParkedColumns} FROM {_events.ParkedIdentitiesTableName} WHERE {where} {ParkedOrder};",
            parameters, token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ParkedShardCount>> FetchParkedCountsAsync(string? tenantId,
        CancellationToken token = default)
    {
        if (!_events.EnableParkedIdentities) return [];

        await EnsureParkedIdentitiesAsync(token).ConfigureAwait(false);

        return await executeAsync(static async (state, ct) =>
        {
            var (connectionString, table, tenant, unicode) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                SELECT projection_name, projection_version, shard_key, tenant_id, COUNT(*) FROM {table}
                {(tenant == null ? "" : "WHERE tenant_id = @tenant")}
                GROUP BY projection_name, projection_version, shard_key, tenant_id
                ORDER BY projection_name, projection_version, shard_key, tenant_id;
                """;
            if (tenant != null) cmd.Parameters.AddKey("@tenant", tenant, unicode);

            var counts = new List<ParkedShardCount>();
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                counts.Add(new ParkedShardCount(reader.GetString(0), (uint)reader.GetInt32(1), reader.GetString(2),
                    reader.GetString(3), reader.GetInt32(4)));
            }

            return (IReadOnlyList<ParkedShardCount>)counts;
        }, (_connectionString, _events.ParkedIdentitiesTableName, tenantId, _events.UnicodeKeys), token)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<int> RequestParkedActionAsync(ParkedActionRequest request, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!_events.EnableParkedIdentities)
        {
            throw new NotSupportedException(
                "This store does not park projection identities. Set StoreOptions.Events.EnableParkedIdentities = true "
                + "(this adds the pc_parked_identities table).");
        }

        await EnsureParkedIdentitiesAsync(token).ConfigureAwait(false);

        var where = new StringBuilder("projection_name = @name");
        var parameters = new List<SqlParameter>
        {
            keyParameter("@name", request.ProjectionName),
            new("@action", SqlDbType.VarChar, 20) { Value = request.Action.ToString() }
        };

        if (request.ProjectionVersion != null)
        {
            where.Append(" AND projection_version = @version");
            parameters.Add(new SqlParameter("@version", SqlDbType.Int) { Value = (int)request.ProjectionVersion });
        }

        if (request.TenantId != null)
        {
            where.Append(" AND tenant_id = @tenant");
            parameters.Add(keyParameter("@tenant", request.TenantId));
        }

        if (request.IdentityText != null)
        {
            where.Append(" AND identity_text = @identity");
            parameters.Add(keyParameter("@identity", request.IdentityText));
        }

        var table = _events.ParkedIdentitiesTableName;
        string sql;
        if (request.IdentityText == null && request.Oldest is { } oldest)
        {
            parameters.Add(new SqlParameter("@oldest", SqlDbType.Int) { Value = oldest });
            sql = $"""
                WITH targets AS (SELECT TOP(@oldest) * FROM {table} WHERE {where} {ParkedOrder})
                UPDATE targets SET requested_action = @action, requested_at = SYSDATETIMEOFFSET();
                """;
        }
        else
        {
            sql = $"UPDATE {table} SET requested_action = @action, requested_at = SYSDATETIMEOFFSET() WHERE {where};";
        }

        return await executeAsync(static async (state, ct) =>
        {
            var (connectionString, text, values) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = text;
            foreach (var parameter in values) cmd.Parameters.Add(((ICloneable)parameter).Clone());

            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, (_connectionString, sql, parameters), token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ParkedIdentity>> LoadParkedIdentitiesAsync(ShardName shard,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(shard);
        if (!_events.EnableParkedIdentities) return [];

        await EnsureParkedIdentitiesAsync(token).ConfigureAwait(false);

        var (where, parameters) = shardFilter(shard);
        return await readParkedAsync(
            $"SELECT {ParkedColumns} FROM {_events.ParkedIdentitiesTableName} WHERE {where} {ParkedOrder};",
            parameters, token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ParkedIdentity>> LoadRequestedParkedActionsAsync(ShardName shard,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(shard);
        if (!_events.EnableParkedIdentities) return [];

        await EnsureParkedIdentitiesAsync(token).ConfigureAwait(false);

        var (where, parameters) = shardFilter(shard);
        return await readParkedAsync(
            $"SELECT {ParkedColumns} FROM {_events.ParkedIdentitiesTableName} WHERE {where} AND requested_action IS NOT NULL ORDER BY requested_at, parked_at, first_failing_seq;",
            parameters, token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RecordParkedReplayFailureAsync(ParkedIdentityKey key, DateTimeOffset attemptedAt, string error,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (!_events.EnableParkedIdentities) return;

        await EnsureParkedIdentitiesAsync(token).ConfigureAwait(false);

        await executeAsync(static async (state, ct) =>
        {
            var (connectionString, table, key, at, message, unicode) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                UPDATE {table} SET replay_attempts = replay_attempts + 1, last_attempt_at = @at,
                    last_attempt_error = @error, requested_action = NULL, requested_at = NULL
                WHERE projection_name = @name AND projection_version = @version AND shard_key = @shard
                  AND tenant_id = @tenant AND identity_text = @identity;
                """;
            cmd.Parameters.Add("@at", SqlDbType.DateTimeOffset).Value = at;
            cmd.Parameters.Add("@error", SqlDbType.NVarChar, -1).Value = message;
            cmd.Parameters.AddKey("@name", key.ProjectionName, unicode);
            cmd.Parameters.Add("@version", SqlDbType.Int).Value = (int)key.ProjectionVersion;
            cmd.Parameters.AddKey("@shard", key.ShardKey, unicode);
            cmd.Parameters.AddKey("@tenant", key.TenantId, unicode);
            cmd.Parameters.AddKey("@identity", key.IdentityText, unicode);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, (_connectionString, _events.ParkedIdentitiesTableName, key, attemptedAt, error, _events.UnicodeKeys), token)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    /// <remarks>
    ///     A row whose event is gone (compacted or tombstoned) is old enough by definition. Under tenant-partitioned
    ///     events each tenant has its own sequence, so the event is matched by its tenant as well.
    /// </remarks>
    public async Task<long?> PruneSliceIndexAsync(SliceIndexPruneRequest request, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!_events.SupportsSliceIndex) return null;

        await EnsureParkedIdentitiesAsync(token).ConfigureAwait(false);

        var table = SqlEscaping.QualifiedName(_events.DatabaseSchemaName,
            SliceIndexTable.TableNameFor(request.ProjectionName, request.ProjectionVersion));

        return await executeAsync(static async (state, ct) =>
        {
            var (connectionString, table, events, request, partitioned, unicode) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            var tenantMatch = partitioned ? " AND e.tenant_id = s.tenant_id" : "";
            cmd.CommandText = $"""
                IF OBJECT_ID({SqlEscaping.Literal(table)}, 'U') IS NULL
                BEGIN
                    SELECT CAST(NULL AS bigint);
                    RETURN;
                END;
                DECLARE @deleted TABLE (seq_id bigint);
                DELETE s OUTPUT deleted.seq_id INTO @deleted
                FROM {table} s
                LEFT JOIN {events} e ON e.seq_id = s.seq_id{tenantMatch}
                WHERE s.seq_id <= @through AND (e.seq_id IS NULL OR e.timestamp < @older)
                {(request.TenantId == null ? "" : "AND s.tenant_id = @tenant")};
                SELECT MAX(seq_id) FROM @deleted;
                """;
            cmd.Parameters.Add("@through", SqlDbType.BigInt).Value = request.DeleteThrough;
            cmd.Parameters.Add("@older", SqlDbType.DateTimeOffset).Value = request.OlderThan;
            if (request.TenantId != null) cmd.Parameters.AddKey("@tenant", request.TenantId, unicode);

            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return result is long through ? through : (long?)null;
        }, (_connectionString, table, _events.EventsTableName, request, _events.UseTenantPartitionedEvents,
            _events.UnicodeKeys), token).ConfigureAwait(false);
    }

    private async Task deleteParkingForShardAsync(string shardIdentity, CancellationToken token)
    {
        if (!_events.EnableParkedIdentities || !ShardName.TryParse(shardIdentity, out var shard) || shard == null)
        {
            return;
        }

        var tenantFilter = shard.TenantId == null ? "" : " AND tenant_id = @tenant";
        var parked = _events.ParkedIdentitiesTableName;
        var sql = new StringBuilder($"IF OBJECT_ID({SqlEscaping.Literal(parked)}, 'U') IS NOT NULL DELETE FROM {parked} WHERE projection_name = @name AND projection_version = @version AND shard_key = @shard{tenantFilter};");

        if (_events.SupportsSliceIndex)
        {
            var table = SqlEscaping.QualifiedName(_events.DatabaseSchemaName,
                SliceIndexTable.TableNameFor(shard.Name, shard.Version));
            sql.AppendLine().Append($"IF OBJECT_ID({SqlEscaping.Literal(table)}, 'U') IS NOT NULL DELETE FROM {table}{(shard.TenantId == null ? "" : " WHERE tenant_id = @tenant")};");
        }

        await executeAsync(static async (state, ct) =>
        {
            var (connectionString, text, shard, unicode) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = text;
            cmd.Parameters.AddKey("@name", shard.Name, unicode);
            cmd.Parameters.Add("@version", SqlDbType.Int).Value = (int)shard.Version;
            cmd.Parameters.AddKey("@shard", shard.ShardKey, unicode);
            if (shard.TenantId != null) cmd.Parameters.AddKey("@tenant", shard.TenantId, unicode);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, (_connectionString, sql.ToString(), shard, _events.UnicodeKeys), token).ConfigureAwait(false);
    }

    // The daemons of this process that run against this database, for WaitForNonStaleProjectionDataAsync. Weak, so
    // a daemon nobody disposed is not kept alive by the database.
    private readonly System.Runtime.CompilerServices.ConditionalWeakTable<IProjectionDaemon, object> _daemons = new();

    internal void TrackDaemon(IProjectionDaemon daemon) => _daemons.AddOrUpdate(daemon, daemon);

    /// <summary>
    ///     #1053: has every running agent of this process's daemons on this database taken in the batches its
    ///     progression row says are committed? A batch commits before the agent applies what it did in memory (its
    ///     parked identities, its aggregate cache), so a wait that only reads the rows can hand control back in
    ///     between, and a replay of an identity that just parked answers <c>NotParked</c>. An agent moves its
    ///     position after that, so the position is what says the batch is done. See jasperfx#1258.
    /// </summary>
    private bool inProcessAgentsHaveCaughtUp(IReadOnlyList<ShardState> progress)
    {
        foreach (var (daemon, _) in _daemons)
        {
            foreach (var agent in daemon.CurrentAgents())
            {
                if (agent.Status != JasperFx.AgentStatus.Running) continue;

                var row = progress.FirstOrDefault(x => x.ShardName == agent.Name.Identity);
                if (row != null && agent.Position < row.Sequence) return false;
            }
        }

        return true;
    }

    private async Task EnsureParkedIdentitiesAsync(CancellationToken token)
        => await EnsureStorageExistsAsync(typeof(EventStoreFeatureSchema), token).ConfigureAwait(false);

    private SqlParameter keyParameter(string name, string value)
        => _events.UnicodeKeys
            ? new SqlParameter(name, SqlDbType.NVarChar, 4000) { Value = value }
            : new SqlParameter(name, SqlDbType.VarChar, 8000) { Value = value };

    // One shard's rows: its name, version and key, and its tenant when the shard is a tenant's own
    private (string Where, List<SqlParameter> Parameters) shardFilter(ShardName shard)
    {
        var where = "projection_name = @name AND projection_version = @version AND shard_key = @shard";
        var parameters = new List<SqlParameter>
        {
            keyParameter("@name", shard.Name),
            new("@version", SqlDbType.Int) { Value = (int)shard.Version },
            keyParameter("@shard", shard.ShardKey)
        };

        if (shard.TenantId != null)
        {
            where += " AND tenant_id = @tenant";
            parameters.Add(keyParameter("@tenant", shard.TenantId));
        }

        return (where, parameters);
    }

    private async Task<IReadOnlyList<ParkedIdentity>> readParkedAsync(string sql, List<SqlParameter> parameters,
        CancellationToken token)
    {
        return await executeAsync(static async (state, ct) =>
        {
            var (connectionString, text, values) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = text;
            // Cloned, because a retry runs this again and a parameter can only belong to one command
            foreach (var parameter in values) cmd.Parameters.Add(((ICloneable)parameter).Clone());

            var rows = new List<ParkedIdentity>();
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                rows.Add(ReadParkedRow(reader));
            }

            return (IReadOnlyList<ParkedIdentity>)rows;
        }, (_connectionString, sql, parameters), token).ConfigureAwait(false);
    }

    internal static ParkedIdentity ReadParkedRow(SqlDataReader reader)
    {
        string? text(int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        DateTimeOffset? timestamp(int ordinal)
            => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<DateTimeOffset>(ordinal);

        var identityText = reader.GetString(4);
        var identityType = reader.GetString(5);

        return new ParkedIdentity
        {
            ProjectionName = reader.GetString(0),
            ProjectionVersion = (uint)reader.GetInt32(1),
            ShardKey = reader.GetString(2),
            TenantId = reader.GetString(3),
            IdentityText = identityText,
            IdentityType = identityType,
            Identity = ParseIdentity(identityText, identityType),
            FirstFailingSequence = reader.GetInt64(6),
            ResumeAfter = reader.GetInt64(7),
            Category = Enum.TryParse<ShardFailureCategory>(text(8), out var category)
                ? category
                : ShardFailureCategory.Other,
            ExceptionType = text(9),
            ExceptionMessage = text(10),
            EventTypeName = text(11),
            EventId = reader.IsDBNull(12) ? null : reader.GetGuid(12),
            FailingStream = text(13),
            ParkedAt = reader.GetFieldValue<DateTimeOffset>(14),
            ReplayAttempts = reader.GetInt32(15),
            LastAttemptAt = timestamp(16),
            LastAttemptError = text(17),
            RequestedAction = Enum.TryParse<ParkedAction>(text(18), out var action) ? action : null,
            RequestedAt = timestamp(19)
        };
    }

    // The simple value back from its text. The daemon wraps it in a strong-typed id when the projection uses one.
    internal static object ParseIdentity(string text, string identityType) => identityType switch
    {
        "guid" => Guid.Parse(text),
        "int" => int.Parse(text, CultureInfo.InvariantCulture),
        "long" => long.Parse(text, CultureInfo.InvariantCulture),
        _ => text
    };
}
