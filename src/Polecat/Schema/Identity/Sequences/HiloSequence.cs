using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Internal;

namespace Polecat.Schema.Identity.Sequences;

internal class HiloSequence : ISequence
{
    private readonly ConnectionFactory _connectionFactory;
    private readonly string _schemaName;
    private readonly HiloSettings _settings;
    private readonly object _lock = new();
    private bool _tableEnsured;

    public HiloSequence(ConnectionFactory connectionFactory, string schemaName, string entityName,
        HiloSettings settings)
    {
        _connectionFactory = connectionFactory;
        _schemaName = schemaName;
        EntityName = entityName;
        CurrentHi = -1;
        CurrentLo = 1;
        MaxLo = settings.MaxLo;
        _settings = settings;
    }

    public string EntityName { get; }
    public long CurrentHi { get; private set; }
    public int CurrentLo { get; private set; }
    public int MaxLo { get; }

    public int NextInt()
    {
        return (int)NextLong();
    }

    public long NextLong()
    {
        lock (_lock)
        {
            if (ShouldAdvanceHi())
            {
                AdvanceToNextHiSync();
            }

            return AdvanceValue();
        }
    }

    public async Task SetFloor(long floor)
    {
        var numberOfPages = (long)Math.Ceiling((double)floor / MaxLo);

        // Guarantee the hilo row exists
        await AdvanceToNextHi();

        await using var conn = _connectionFactory.Create();
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"UPDATE [{_schemaName}].[pc_hilo] SET hi_value = @floor WHERE entity_name = @name;";
        cmd.Parameters.AddWithValue("@floor", numberOfPages);
        cmd.Parameters.AddWithValue("@name", EntityName);
        await cmd.ExecuteNonQueryAsync();

        // Advance again to pick up the new floor
        await AdvanceToNextHi();
    }

    public async Task AdvanceToNextHi(CancellationToken ct = default)
    {
        await using var conn = _connectionFactory.Create();
        await conn.OpenAsync(ct);

        await EnsureHiloTableAsync(conn, ct);

        for (var attempts = 0; attempts < _settings.MaxAdvanceToNextHiAttempts; attempts++)
        {
            var result = await TryGetNextHiAsync(conn, ct);
            if (TrySetCurrentHi(result))
            {
                return;
            }
        }

        throw new HiloSequenceAdvanceToNextHiAttemptsExceededException();
    }

    private void AdvanceToNextHiSync()
    {
        using var conn = _connectionFactory.Create();
        conn.Open();

        EnsureHiloTableSync(conn);

        for (var attempts = 0; attempts < _settings.MaxAdvanceToNextHiAttempts; attempts++)
        {
            var result = TryGetNextHiSync(conn);
            if (TrySetCurrentHi(result))
            {
                return;
            }
        }

        throw new HiloSequenceAdvanceToNextHiAttemptsExceededException();
    }

    private async Task EnsureHiloTableAsync(SqlConnection conn, CancellationToken ct)
    {
        if (_tableEnsured) return;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = HiloTableDdl();
        await cmd.ExecuteNonQueryAsync(ct);
        _tableEnsured = true;
    }

    private void EnsureHiloTableSync(SqlConnection conn)
    {
        if (_tableEnsured) return;

        using var cmd = conn.CreateCommand();
        cmd.CommandText = HiloTableDdl();
        cmd.ExecuteNonQuery();
        _tableEnsured = true;
    }

    private string HiloTableDdl()
    {
        return $"""
            IF NOT EXISTS (SELECT 1 FROM sys.tables t
                           JOIN sys.schemas s ON t.schema_id = s.schema_id
                           WHERE s.name = '{_schemaName}' AND t.name = 'pc_hilo')
            BEGIN
                IF SCHEMA_ID('{_schemaName}') IS NULL
                    EXEC('CREATE SCHEMA [{_schemaName}]');

                CREATE TABLE [{_schemaName}].[pc_hilo] (
                    entity_name varchar(250) NOT NULL PRIMARY KEY,
                    hi_value bigint NOT NULL DEFAULT 0
                );
            END
            """;
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

    private bool TrySetCurrentHi(long raw)
    {
        CurrentHi = raw;
        if (CurrentHi >= 0)
        {
            CurrentLo = 1;
            return true;
        }

        return false;
    }

    public long AdvanceValue()
    {
        var result = (CurrentHi * MaxLo) + CurrentLo;
        CurrentLo++;
        return result;
    }

    public bool ShouldAdvanceHi()
    {
        return CurrentHi < 0 || CurrentLo > MaxLo;
    }
}
