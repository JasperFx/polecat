using JasperFx;
using Microsoft.Data.SqlClient;
using Polly;
using Polecat.Internal;
using Weasel.Core;
using Weasel.Core.Sequences;
using Weasel.SqlServer;

namespace Polecat.Schema.Identity.Sequences;

/// <summary>
///     SQL Server Hi-Lo sequence. The dialect-agnostic hi/lo state and client-side
///     id arithmetic live on <see cref="HiloSequenceBase" /> (lifted into Weasel.Core
///     in weasel#287); this subclass supplies only the SQL Server I/O — an optimistic
///     UPDATE/INSERT against <c>pc_hilo</c>, wrapped in a resilience pipeline.
/// </summary>
internal class HiloSequence : HiloSequenceBase
{
    private readonly ConnectionFactory _connectionFactory;
    private readonly string _schemaName;
    private readonly ResiliencePipeline _resilience;
    private readonly AutoCreate _autoCreate;
    private bool _tableEnsured;

    public HiloSequence(ConnectionFactory connectionFactory, string schemaName, string entityName,
        HiloSettings settings, ResiliencePipeline resilience, AutoCreate autoCreate)
        : base(entityName, settings)
    {
        _connectionFactory = connectionFactory;
        _schemaName = schemaName;
        _resilience = resilience;
        _autoCreate = autoCreate;
    }

    public override async Task SetFloor(long floor)
    {
        var numberOfPages = (long)Math.Ceiling((double)floor / MaxLo);

        // Guarantee the hilo row exists
        await AdvanceToNextHi();

        await _resilience.ExecuteAsync(async (state, ct) =>
        {
            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"UPDATE [{_schemaName}].[pc_hilo] SET hi_value = @floor WHERE entity_name = @name;";
            cmd.Parameters.AddWithValue("@floor", state);
            cmd.Parameters.AddWithValue("@name", EntityName);
            await cmd.ExecuteNonQueryAsync(ct);
        }, numberOfPages);

        // Advance again to pick up the new floor
        await AdvanceToNextHi();
    }

    public override async Task AdvanceToNextHi(CancellationToken ct = default)
    {
        await _resilience.ExecuteAsync(async (_, cancellation) =>
        {
            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(cancellation);

            await EnsureHiloTableAsync(conn, cancellation);

            for (var attempts = 0; attempts < Settings.MaxAdvanceToNextHiAttempts; attempts++)
            {
                var result = await TryGetNextHiAsync(conn, cancellation);
                if (TrySetCurrentHi(result))
                {
                    return;
                }
            }

            throw new HiloSequenceAdvanceToNextHiAttemptsExceededException();
        }, ct);
    }

    protected override void AdvanceToNextHiSync()
    {
        // #148: the async AdvanceToNextHi wraps its work in the resilience
        // pipeline; the synchronous path must do the same (via the pipeline's
        // synchronous Execute) so all pc_hilo access is covered.
        _resilience.Execute(() =>
        {
            using var conn = _connectionFactory.Create();
            conn.Open();

            EnsureHiloTableSync(conn);

            for (var attempts = 0; attempts < Settings.MaxAdvanceToNextHiAttempts; attempts++)
            {
                var result = TryGetNextHiSync(conn);
                if (TrySetCurrentHi(result))
                {
                    return;
                }
            }

            throw new HiloSequenceAdvanceToNextHiAttemptsExceededException();
        });
    }

    private async Task EnsureHiloTableAsync(SqlConnection conn, CancellationToken ct)
    {
        if (_tableEnsured) return;

        // #267: honor AutoCreate.None — the user manages the pc_hilo table themselves, so never
        // emit DDL at runtime (mirrors DocumentTableEnsurer). A subsequent UPDATE/INSERT against a
        // missing table surfaces as a clean "invalid object" error, the same opt-out contract as
        // document and event-store tables.
        if (_autoCreate == AutoCreate.None)
        {
            _tableEnsured = true;
            return;
        }

        var table = new HiloTable(_schemaName);
        var migrator = new SqlServerMigrator();
        var migration = await SchemaMigration.DetermineAsync(conn, ct, table);
        await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct);
        _tableEnsured = true;
    }

    private void EnsureHiloTableSync(SqlConnection conn)
    {
        if (_tableEnsured) return;

        // #267: honor AutoCreate.None (see EnsureHiloTableAsync).
        if (_autoCreate == AutoCreate.None)
        {
            _tableEnsured = true;
            return;
        }

        var table = new HiloTable(_schemaName);
        var migrator = new SqlServerMigrator();
        var migration = SchemaMigration.DetermineAsync(conn, table).GetAwaiter().GetResult();
        migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate).GetAwaiter().GetResult();
        _tableEnsured = true;
    }

    private async Task<long> TryGetNextHiAsync(SqlConnection conn, CancellationToken ct)
    {
        // Read current hi_value
        long? currentHi;
        await using (var readCmd = conn.CreateCommand())
        {
            readCmd.CommandText =
                $"SELECT hi_value FROM [{_schemaName}].[pc_hilo] WHERE entity_name = @entity;";
            readCmd.Parameters.AddWithValue("@entity", EntityName);
            var raw = await readCmd.ExecuteScalarAsync(ct);
            currentHi = raw == null || raw == DBNull.Value ? null : Convert.ToInt64(raw);
        }

        if (currentHi == null)
        {
            // Row doesn't exist — try to insert it
            try
            {
                await using var insertCmd = conn.CreateCommand();
                insertCmd.CommandText =
                    $"INSERT INTO [{_schemaName}].[pc_hilo] (entity_name, hi_value) VALUES (@entity, 0);";
                insertCmd.Parameters.AddWithValue("@entity", EntityName);
                await insertCmd.ExecuteNonQueryAsync(ct);
                return 0;
            }
            catch (SqlException ex) when (ex.Number == 2627)
            {
                // Concurrent insert — retry
                return -1;
            }
        }

        // Attempt optimistic update
        var nextHi = currentHi.Value + 1;
        await using (var updateCmd = conn.CreateCommand())
        {
            updateCmd.CommandText =
                $"UPDATE [{_schemaName}].[pc_hilo] SET hi_value = @next WHERE entity_name = @entity AND hi_value = @current;";
            updateCmd.Parameters.AddWithValue("@next", nextHi);
            updateCmd.Parameters.AddWithValue("@entity", EntityName);
            updateCmd.Parameters.AddWithValue("@current", currentHi.Value);
            var rows = await updateCmd.ExecuteNonQueryAsync(ct);
            return rows == 0 ? -1 : nextHi;
        }
    }

    private long TryGetNextHiSync(SqlConnection conn)
    {
        // Read current hi_value
        long? currentHi;
        using (var readCmd = conn.CreateCommand())
        {
            readCmd.CommandText =
                $"SELECT hi_value FROM [{_schemaName}].[pc_hilo] WHERE entity_name = @entity;";
            readCmd.Parameters.AddWithValue("@entity", EntityName);
            var raw = readCmd.ExecuteScalar();
            currentHi = raw == null || raw == DBNull.Value ? null : Convert.ToInt64(raw);
        }

        if (currentHi == null)
        {
            try
            {
                using var insertCmd = conn.CreateCommand();
                insertCmd.CommandText =
                    $"INSERT INTO [{_schemaName}].[pc_hilo] (entity_name, hi_value) VALUES (@entity, 0);";
                insertCmd.Parameters.AddWithValue("@entity", EntityName);
                insertCmd.ExecuteNonQuery();
                return 0;
            }
            catch (SqlException ex) when (ex.Number == 2627)
            {
                return -1;
            }
        }

        var nextHi = currentHi.Value + 1;
        using (var updateCmd = conn.CreateCommand())
        {
            updateCmd.CommandText =
                $"UPDATE [{_schemaName}].[pc_hilo] SET hi_value = @next WHERE entity_name = @entity AND hi_value = @current;";
            updateCmd.Parameters.AddWithValue("@next", nextHi);
            updateCmd.Parameters.AddWithValue("@entity", EntityName);
            updateCmd.Parameters.AddWithValue("@current", currentHi.Value);
            var rows = updateCmd.ExecuteNonQuery();
            return rows == 0 ? -1 : nextHi;
        }
    }
}
