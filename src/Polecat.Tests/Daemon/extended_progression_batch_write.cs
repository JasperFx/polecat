using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal.Operations;
using Polecat.Storage;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Daemon;

/// <summary>
///     polecat#437 (marten 334283ee7 + marten#5167 / ef2c117db): the batched extended-progression
///     write. JasperFx's <c>ExtendedProgressionWriter</c> coalesces a database's shard states and
///     drives <c>WriteExtendedProgressionAsync(IReadOnlyList&lt;ShardState&gt;)</c>; the default
///     interface member just loops the single-state overload, so before this Polecat rented one
///     connection PER STATE on every flush (jasperfx#553).
///     <para>
///         The batch amortizes the CONNECTION, not the transaction. Marten's first cut was one
///         multi-row statement and that built a lock convoy (marten#5167): a multi-row UPDATE holds a
///         row lock on every row it matches until it commits, so one slow projection batch on one
///         progression row stalled every other shard's telemetry on the database. The shape ported
///         here is the post-convoy-fix one — one single-row statement per shard, each its own implicit
///         transaction, in shard-name order.
///     </para>
/// </summary>
public class extended_progression_batch_write : IAsyncLifetime
{
    private const string Schema = "extended_progression_batch";
    private const string First = "BatchTelemetryStream:All";
    private const string Second = "OtherBatchTelemetry:All";

    private DocumentStore _store = null!;

    public async ValueTask InitializeAsync()
    {
        await DropSchemaTablesAsync(Schema);

        _store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.EnableExtendedProgressionTracking = true;
        });

        await _store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Seed the two committed progression rows directly. Nothing here needs a running daemon, and a
        // daemon shutdown emits its own "Stopped" telemetry asynchronously, which would race back in and
        // clobber the rows under assertion (marten#5022).
        foreach (var shard in new[] { First, Second })
        {
            await SeedAsync(shard, 10);
        }
    }

    public ValueTask DisposeAsync()
    {
        _store.Dispose();
        return ValueTask.CompletedTask;
    }

    private PolecatDatabase theDatabase => _store.Database;

    private static ShardState Telemetry(string shard, string status, string? reason = null, int? node = null)
    {
        return new ShardState(shard, 10)
        {
            Action = ShardAction.Updated,
            AgentStatus = status,
            PauseReason = reason,
            LastHeartbeat = DateTimeOffset.UtcNow,
            RunningOnNode = node
        };
    }

    [Fact]
    public async Task updates_every_existing_row_in_one_batch()
    {
        await theDatabase.WriteExtendedProgressionAsync([
            Telemetry(First, "Running", node: 3),
            Telemetry(Second, "Paused", "boom", node: 7)
        ], TestContext.Current.CancellationToken);

        var first = await ReadRowAsync(First);
        first.Status.ShouldBe("Running");
        first.Heartbeat.ShouldNotBeNull();
        first.Reason.ShouldBeNull();
        Convert.ToInt32(first.Node).ShouldBe(3);

        var second = await ReadRowAsync(Second);
        second.Status.ShouldBe("Paused");
        second.Reason.ShouldBe("boom");
        Convert.ToInt32(second.Node).ShouldBe(7);
    }

    [Fact]
    public async Task never_inserts_a_row_and_never_touches_committed_progress()
    {
        var rowsBefore = await CountRowsAsync();

        await theDatabase.WriteExtendedProgressionAsync([
            Telemetry(First, "Running"),
            // A shard that has never committed progression: no row to decorate, so it must be skipped
            // silently, exactly like the single-state path.
            Telemetry("NoSuchProjection:All:98123456", "Running")
        ], TestContext.Current.CancellationToken);

        var updated = await ReadRowAsync(First);
        updated.Status.ShouldBe("Running");
        Convert.ToInt64(updated.Sequence).ShouldBe(10); // committed progress untouched

        (await CountRowsAsync()).ShouldBe(rowsBefore); // and nothing was inserted
        (await ReadRowAsync("NoSuchProjection:All:98123456")).Status.ShouldBeNull();
    }

    [Fact]
    public async Task an_empty_batch_is_a_no_op_and_the_single_state_overload_delegates()
    {
        await theDatabase.WriteExtendedProgressionAsync(Array.Empty<ShardState>(),
            TestContext.Current.CancellationToken);

        await theDatabase.WriteExtendedProgressionAsync(Telemetry(First, "Stopped"),
            TestContext.Current.CancellationToken);

        (await ReadRowAsync(First)).Status.ShouldBe("Stopped");
    }

    /// <summary>
    ///     marten#5167, the lock-convoy regression. A row locked by an in-flight projection batch is the
    ///     normal case, and the telemetry write is going to wait on it either way. What must NOT happen
    ///     is the batch dragging every OTHER shard's row into that wait.
    /// </summary>
    /// <remarks>
    ///     Rows go in shard-name order, so <c>BatchTelemetryStream:All</c> is written first and the batch
    ///     then parks on the deliberately-locked <c>OtherBatchTelemetry:All</c>. Seeing the first row's
    ///     telemetry from ANOTHER connection while the batch is still parked is proof that its write
    ///     committed on its own: under one multi-row statement nothing would be visible until the whole
    ///     batch committed, and that row would still be locked.
    /// </remarks>
    [Fact]
    public async Task a_contended_row_does_not_hold_the_locks_of_the_rows_already_written()
    {
        var token = TestContext.Current.CancellationToken;

        await using var blocker = new SqlConnection(ConnectionSource.ConnectionString);
        await blocker.OpenAsync(token);
        await using var blocking = (SqlTransaction)await blocker.BeginTransactionAsync(token);

        // Stands in for a projection batch transaction sitting on its own progression row.
        await using (var lockCmd = blocker.CreateCommand())
        {
            lockCmd.Transaction = blocking;
            lockCmd.CommandText =
                $"UPDATE [{Schema}].[pc_event_progression] SET last_seq_id = last_seq_id WHERE name = @name;";
            lockCmd.Parameters.Add("@name", System.Data.SqlDbType.VarChar, 200).Value = Second;
            await lockCmd.ExecuteNonQueryAsync(token);
        }

        var write = theDatabase.WriteExtendedProgressionAsync([
            Telemetry(First, "Running", node: 3),
            Telemetry(Second, "Paused", "boom")
        ], token);

        // The first row's write commits on its own while the batch is parked on the contended one.
        var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
        while ((await ReadRowAsync(First)).Status == null)
        {
            DateTimeOffset.UtcNow.ShouldBeLessThan(deadline,
                "The first row's telemetry never became visible -- the batch is holding its lock");
            await Task.Delay(50, token);
        }

        // ...and it is genuinely still parked, so that visibility was not just "the batch finished".
        write.IsCompleted.ShouldBeFalse();

        // The counterfactual from the report: an unrelated writer touching the already-written row must
        // not queue behind the batch. LOCK_TIMEOUT turns "it waited" into a failure instead of a hang.
        await using (var unrelated = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await unrelated.OpenAsync(token);
            await using var cmd = unrelated.CreateCommand();
            cmd.CommandText = $"""
                SET LOCK_TIMEOUT 2000;
                UPDATE [{Schema}].[pc_event_progression] SET last_seq_id = last_seq_id
                WHERE name = @name;
                """;
            cmd.Parameters.Add("@name", System.Data.SqlDbType.VarChar, 200).Value = First;
            await cmd.ExecuteNonQueryAsync(token);
        }

        await blocking.RollbackAsync(token);

        await write;
        (await ReadRowAsync(Second)).Status.ShouldBe("Paused");
    }

    /// <summary>
    ///     marten#5167 finding 3 — the SET list is unconditional, so without the
    ///     <c>NOT EXISTS (... INTERSECT ...)</c> guard every flush rewrote every matched row whether
    ///     anything had changed or not, on a small hot table. Each avoided rewrite is also an avoided
    ///     row lock.
    /// </summary>
    /// <remarks>
    ///     SQL Server has no <c>xmin</c>, so "was this row rewritten?" is measured with an AFTER UPDATE
    ///     trigger that records every row the statement actually touched.
    /// </remarks>
    [Fact]
    public async Task replaying_identical_telemetry_does_not_rewrite_the_row()
    {
        var token = TestContext.Current.CancellationToken;
        var state = Telemetry(First, "Running", node: 3);

        await theDatabase.WriteExtendedProgressionAsync([state], token);

        await CreateUpdateAuditAsync();
        try
        {
            // Byte-identical replay: nothing to change, so nothing is written.
            await theDatabase.WriteExtendedProgressionAsync([state], token);
            (await AuditCountAsync()).ShouldBe(0);

            // ...but a real change still lands.
            await theDatabase.WriteExtendedProgressionAsync([
                Telemetry(First, "Paused", "boom", node: 3)
            ], token);

            (await AuditCountAsync()).ShouldBe(1);
        }
        finally
        {
            await DropUpdateAuditAsync();
        }

        (await ReadRowAsync(First)).Status.ShouldBe("Paused");
    }

    private record ProgressionRow(object? Heartbeat, object? Status, object? Reason, object? Node, object? Sequence);

    private static async Task<ProgressionRow> ReadRowAsync(string shard)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT heartbeat, agent_status, pause_reason, running_on_node, last_seq_id
            FROM [{Schema}].[pc_event_progression] WHERE name = @name;
            """;
        cmd.Parameters.Add("@name", System.Data.SqlDbType.VarChar, 200).Value = shard;

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return new ProgressionRow(null, null, null, null, null);
        }

        object? At(int i) => reader.IsDBNull(i) ? null : reader.GetValue(i);
        return new ProgressionRow(At(0), At(1), At(2), At(3), At(4));
    }

    private static Task<long> CountRowsAsync() =>
        ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_event_progression];");

    private static Task<long> AuditCountAsync() =>
        ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_progression_update_audit];");

    private static async Task<long> ScalarAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task CreateUpdateAuditAsync()
    {
        await ExecuteAsync($"""
            IF OBJECT_ID('[{Schema}].[pc_progression_update_audit]', 'U') IS NULL
                CREATE TABLE [{Schema}].[pc_progression_update_audit] (name varchar(500) NOT NULL);
            DELETE FROM [{Schema}].[pc_progression_update_audit];
            """);

        // A separate batch: CREATE TRIGGER must be the first statement in its own batch.
        await ExecuteAsync($"""
            CREATE TRIGGER [{Schema}].[trg_pc_progression_update_audit]
            ON [{Schema}].[pc_event_progression] AFTER UPDATE AS
            BEGIN
                SET NOCOUNT ON;
                INSERT INTO [{Schema}].[pc_progression_update_audit] (name) SELECT name FROM inserted;
            END
            """);
    }

    private static Task DropUpdateAuditAsync() =>
        ExecuteAsync($"DROP TRIGGER IF EXISTS [{Schema}].[trg_pc_progression_update_audit];");

    private static async Task ExecuteAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedAsync(string shardIdentity, long ceiling)
    {
        // The production write path (polecat#323).
        var events = theDatabase.Events;
        var op = new RecordProgressionOperation(
            events.ProgressionTableName,
            shardIdentity,
            ceiling,
            events.EnableExtendedProgressionTracking,
            upsert: true);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var batch = new SqlBatch(conn);
        var builder = new Weasel.SqlServer.BatchBuilder(batch);
        op.ConfigureCommand(builder);
        builder.Compile();
        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);
        await op.PostprocessAsync(reader, new List<Exception>(), CancellationToken.None);
    }

    private static async Task DropSchemaTablesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;

            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }
}
