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

    /// <summary>#390: one escaped builder for the name, rather than seven hand-written copies.</summary>
    private readonly string _hiloTable;

    private readonly StoreOptions _options;
    private readonly ResiliencePipeline _resilience;
    private volatile bool _tableEnsured;

    public HiloSequence(ConnectionFactory connectionFactory, StoreOptions options, string entityName,
        HiloSettings settings)
        : base(entityName, settings)
    {
        _connectionFactory = connectionFactory;
        _options = options;
        _schemaName = options.DatabaseSchemaName;
        _hiloTable = Polecat.Internal.SqlEscaping.QualifiedName(_schemaName, HiloTable.TableName);
        _resilience = options.ResiliencePipeline;
    }

    /// <summary>
    ///     Ensure <c>pc_hilo</c> on behalf of a caller that knows whether it has a transaction open, which
    ///     is a session about to assign an id. See <see cref="EnsureHiloTableAsync" />.
    /// </summary>
    internal void EnsureStorage(bool insideTransaction)
    {
        if (_tableEnsured) return;

        _resilience.Execute(() =>
        {
            using var conn = _connectionFactory.Create();
            conn.Open();

            EnsureHiloTableAsync(conn, insideTransaction, CancellationToken.None).GetAwaiter().GetResult();
        });
    }

    internal void ForgetEnsuredTable() => _tableEnsured = false;

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
                $"UPDATE {_hiloTable} SET hi_value = @floor WHERE entity_name = @name;";
            cmd.Parameters.AddWithValue("@floor", state);
            cmd.Parameters.AddVarChar("@name", EntityName);
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

            await EnsureHiloTableAsync(conn, insideTransaction: true, cancellation);

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

            EnsureHiloTableAsync(conn, insideTransaction: true, CancellationToken.None).GetAwaiter().GetResult();

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

    /// <remarks>
    ///     <para>
    ///     #883: on the same lock as every other path that provisions this schema, because the daemon's
    ///     ensure declares <c>pc_hilo</c> too. <paramref name="insideTransaction" /> means what it means for
    ///     <see cref="SchemaMigrationLock.HoldForFirstUseAsync" />, with a difference that decides who
    ///     passes what: nothing below <see cref="ISequence" /> can see a transaction, and an id can be asked
    ///     for at any point in one. So the sequence's own paths always pass true and never wait, whatever the
    ///     caller is doing. A busy lock skips the ensure, a table that exists is simply used, and one that
    ///     does not fails with nothing remembered, for the next id to try again.
    ///     </para>
    ///     <para>
    ///     That alone would be worse than what it replaced on a fresh schema: every <c>Store()</c> during
    ///     a migration failing on a missing table, where the old code only collided in the instant both
    ///     ran the CREATE. So a session, which does know, comes through <see cref="EnsureStorage" /> first
    ///     and waits like any other first use when it has no transaction open.
    ///     </para>
    /// </remarks>
    private async Task EnsureHiloTableAsync(SqlConnection conn, bool insideTransaction, CancellationToken ct)
    {
        if (_tableEnsured) return;

        // #267: honor AutoCreate.None — the user manages the pc_hilo table themselves, so never
        // emit DDL at runtime (mirrors DocumentTableEnsurer). A subsequent UPDATE/INSERT against a
        // missing table surfaces as a clean "invalid object" error, the same opt-out contract as
        // document and event-store tables.
        if (_options.AutoCreateSchemaObjects == AutoCreate.None)
        {
            _tableEnsured = true;
            return;
        }

        await using var migrationLock =
            await SchemaMigrationLock.HoldForFirstUseAsync(_options, conn, insideTransaction, ct);
        if (migrationLock is null) return;

        var table = new HiloTable(_schemaName);
        var migrator = new SqlServerMigrator();
        var migration = await SchemaMigration.DetermineAsync(conn, ct, table);
        await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct);
        _tableEnsured = true;
    }

    private async Task<long> TryGetNextHiAsync(SqlConnection conn, CancellationToken ct)
    {
        // Read current hi_value
        long? currentHi;
        await using (var readCmd = conn.CreateCommand())
        {
            readCmd.CommandText =
                $"SELECT hi_value FROM {_hiloTable} WHERE entity_name = @entity;";
            readCmd.Parameters.AddVarChar("@entity", EntityName);
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
                    $"INSERT INTO {_hiloTable} (entity_name, hi_value) VALUES (@entity, 0);";
                insertCmd.Parameters.AddVarChar("@entity", EntityName);
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
                $"UPDATE {_hiloTable} SET hi_value = @next WHERE entity_name = @entity AND hi_value = @current;";
            updateCmd.Parameters.AddWithValue("@next", nextHi);
            updateCmd.Parameters.AddVarChar("@entity", EntityName);
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
                $"SELECT hi_value FROM {_hiloTable} WHERE entity_name = @entity;";
            readCmd.Parameters.AddVarChar("@entity", EntityName);
            var raw = readCmd.ExecuteScalar();
            currentHi = raw == null || raw == DBNull.Value ? null : Convert.ToInt64(raw);
        }

        if (currentHi == null)
        {
            try
            {
                using var insertCmd = conn.CreateCommand();
                insertCmd.CommandText =
                    $"INSERT INTO {_hiloTable} (entity_name, hi_value) VALUES (@entity, 0);";
                insertCmd.Parameters.AddVarChar("@entity", EntityName);
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
                $"UPDATE {_hiloTable} SET hi_value = @next WHERE entity_name = @entity AND hi_value = @current;";
            updateCmd.Parameters.AddWithValue("@next", nextHi);
            updateCmd.Parameters.AddVarChar("@entity", EntityName);
            updateCmd.Parameters.AddWithValue("@current", currentHi.Value);
            var rows = updateCmd.ExecuteNonQuery();
            return rows == 0 ? -1 : nextHi;
        }
    }
}
