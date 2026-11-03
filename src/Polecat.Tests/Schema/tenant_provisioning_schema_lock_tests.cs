using System.Data;
using System.Diagnostics;
using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer;

namespace Polecat.Tests.Schema;

public record ProvisioningLockHappened(int Number);

public class ProvisioningLockDocument
{
    public Guid Id { get; set; }
}

/// <summary>
///     #883: provisioning a tenant registers it and SPLITs every partitioned table through Weasel, which
///     takes no lock, so it was the one thing changing a schema that everything else on #868's lock could
///     still collide with. These hold the lock from outside, as <see cref="first_use_schema_lock_tests" />
///     does, and the two rules are the same: the resource is the schema, and a caller with a transaction
///     open never waits. The one difference is what a loser does, because a tenant's first write can't be
///     skipped and a busy schema doesn't fail a write (#892).
/// </summary>
[Collection("tenant-partitioning")]
public class tenant_provisioning_schema_lock_tests : IAsyncLifetime
{
    private const string Schema = "pt_provision_lock";
    private const string Resource = $"polecat:migrate:{Schema}";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore BuildStore(TimeSpan? lockTimeout = null, int commandTimeout = StoreOptions.DefaultTimeout)
        => DocumentStore.For(opts =>
        {
            opts.ConnectionString = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                CommandTimeout = commandTimeout
            }.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.StartupMigrationLockTimeout = lockTimeout ?? TimeSpan.FromSeconds(30);
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Events.AddEventType<ProvisioningLockHappened>();
            opts.Schema.For<ProvisioningLockDocument>();
        });

    private static async Task<DocumentStore> CreateProvisionedStoreAsync(TimeSpan? lockTimeout = null)
    {
        var store = BuildStore(lockTimeout);

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // After this a session's ensure asks for nothing, so all that's left to take the lock is a new tenant
        await AppendAsync(store, "incumbent");

        await using var session = store.LightweightSession();
        await session.LoadAsync<ProvisioningLockDocument>(Guid.NewGuid(), Ct);

        return store;
    }

    private static async Task AppendAsync(DocumentStore store, string tenant)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        session.Events.StartStream(Guid.NewGuid(), new ProvisioningLockHappened(1));
        await session.SaveChangesAsync(Ct);
    }

    private static async Task<IReadOnlyList<string>> RegisteredTenantsAsync()
    {
        var rows = await TestSchema.QueryAsync($"SELECT tenant_id FROM [{Schema}].[pc_tenant_partitions]");
        return rows.Select(x => (string)x[0]).ToList();
    }

    private static async Task<SqlConnection> TakeTheLockAsync()
    {
        var holder = new SqlConnection(ConnectionSource.ConnectionString);
        await holder.OpenAsync(Ct);
        (await holder.TryGetGlobalLock(Resource, cancellation: Ct)).ShouldBeTrue();
        return holder;
    }

    private static async Task<bool> SomethingIsQueuedForTheLockAsync(SqlConnection conn)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          SELECT COUNT(*) FROM sys.dm_tran_locks
                          WHERE resource_type = 'APPLICATION'
                            AND resource_database_id = DB_ID()
                            AND request_status = 'WAIT'
                          """;
        return (int)(await cmd.ExecuteScalarAsync(Ct))! > 0;
    }

    /// <remarks>
    ///     A separate pool, because a lock left behind on a pooled connection is freed by opening that same
    ///     connection again. See <see cref="first_use_schema_lock_tests" />.
    /// </remarks>
    private static async Task<bool> LockIsFreeAsync()
    {
        var separatePool = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            ApplicationName = "tenant_provisioning_lock_probe"
        };

        await using var probe = new SqlConnection(separatePool.ConnectionString);
        await probe.OpenAsync(Ct);

        var free = await probe.TryGetGlobalLock(Resource, cancellation: Ct, lockTimeoutMs: 0);
        if (free) await probe.ReleaseGlobalLock(Resource, Ct);

        return free;
    }

    private static async Task<(SqlConnection connection, SqlTransaction transaction)> BlockSplitsOnStreamsAsync()
    {
        var connection = new SqlConnection(ConnectionSource.ConnectionString);
        await connection.OpenAsync(Ct);
        var transaction = connection.BeginTransaction();

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"SELECT COUNT(*) FROM [{Schema}].[pc_streams] WITH (REPEATABLEREAD) WHERE tenant_id = 'incumbent'";
        await command.ExecuteScalarAsync(Ct);

        return (connection, transaction);
    }

    [Fact]
    public async Task a_new_tenant_waits_for_the_lock_and_then_is_provisioned()
    {
        using var store = await CreateProvisionedStoreAsync();
        await using var holder = await TakeTheLockAsync();

        var running = AppendAsync(store, "newcomer");

        var patience = Stopwatch.StartNew();
        while (!await SomethingIsQueuedForTheLockAsync(holder))
        {
            running.IsCompleted.ShouldBeFalse("this ran while another process held the migration lock");
            patience.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(20), "nothing ever asked for the migration lock");

            await Task.Delay(25, Ct);
        }

        (await RegisteredTenantsAsync()).ShouldNotContain("newcomer");

        await holder.ReleaseGlobalLock(Resource, Ct);
        await running;

        (await RegisteredTenantsAsync()).ShouldContain("newcomer");
    }

    [Fact]
    public async Task the_lock_is_held_for_the_split_as_well_as_the_registration()
    {
        using var store = await CreateProvisionedStoreAsync();

        var (connection, transaction) = await BlockSplitsOnStreamsAsync();
        await using (connection)
        {
            var running = AppendAsync(store, "newcomer");

            var patience = Stopwatch.StartNew();
            while (!(await RegisteredTenantsAsync()).Contains("newcomer"))
            {
                patience.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(20), "the tenant was never registered");
                await Task.Delay(25, Ct);
            }

            running.IsCompleted.ShouldBeFalse("the SPLIT should be stuck behind the open transaction");
            (await LockIsFreeAsync()).ShouldBeFalse();

            await transaction.RollbackAsync(Ct);
            await running;
        }

        (await LockIsFreeAsync()).ShouldBeTrue();
    }

    [Fact]
    public async Task a_session_with_a_transaction_open_does_not_wait_for_the_lock()
    {
        using var store = await CreateProvisionedStoreAsync();
        await using var holder = await TakeTheLockAsync();

        await using var session = store.LightweightSession(new SessionOptions
        {
            TenantId = "newcomer",
            IsolationLevel = IsolationLevel.Serializable
        });

        // Opens the transaction on a table the tenant's SPLIT has no interest in
        await session.LoadAsync<ProvisioningLockDocument>(Guid.NewGuid(), Ct);
        session.Events.StartStream(Guid.NewGuid(), new ProvisioningLockHappened(1));

        var stopwatch = Stopwatch.StartNew();
        await session.SaveChangesAsync(Ct);
        stopwatch.Stop();

        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5),
            "a session holding a transaction must never wait on the migration lock");

        (await RegisteredTenantsAsync()).ShouldContain("newcomer");
    }

    [Fact]
    public async Task a_lock_that_stays_taken_does_not_fail_the_write()
    {
        using var store = await CreateProvisionedStoreAsync(TimeSpan.FromMilliseconds(250));
        await using var holder = await TakeTheLockAsync();

        await AppendAsync(store, "newcomer");

        (await RegisteredTenantsAsync()).ShouldContain("newcomer");
    }

    [Fact]
    public async Task a_known_tenant_asks_for_no_lock()
    {
        using var store = await CreateProvisionedStoreAsync();
        await using var holder = await TakeTheLockAsync();

        var stopwatch = Stopwatch.StartNew();
        await AppendAsync(store, "incumbent");
        stopwatch.Stop();

        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task onboarding_a_tenant_explicitly_takes_the_lock_too()
    {
        using var store = await CreateProvisionedStoreAsync();
        await using var holder = await TakeTheLockAsync();

        var running = store.Advanced.AddPolecatManagedTenantsAsync(Ct, "newcomer");

        var patience = Stopwatch.StartNew();
        while (!await SomethingIsQueuedForTheLockAsync(holder))
        {
            running.IsCompleted.ShouldBeFalse("this ran while another process held the migration lock");
            patience.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(20), "nothing ever asked for the migration lock");

            await Task.Delay(25, Ct);
        }

        await holder.ReleaseGlobalLock(Resource, Ct);
        await running;

        (await RegisteredTenantsAsync()).ShouldContain("newcomer");
    }

    /// <summary>
    ///     #916. MERGE RANGE is a schema change like any other, and it took no lock at all.
    /// </summary>
    [Fact]
    public async Task removing_a_tenant_waits_for_the_lock()
    {
        using var store = await CreateProvisionedStoreAsync();
        await AppendAsync(store, "leaving");

        await using var holder = await TakeTheLockAsync();

        var running = store.Advanced.RemovePolecatManagedTenantsAsync(["leaving"], Ct);

        var patience = Stopwatch.StartNew();
        while (!await SomethingIsQueuedForTheLockAsync(holder))
        {
            running.IsCompleted.ShouldBeFalse("this ran while another process held the migration lock");
            patience.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(20), "nothing ever asked for the migration lock");

            await Task.Delay(25, Ct);
        }

        (await RegisteredTenantsAsync()).ShouldContain("leaving");

        await holder.ReleaseGlobalLock(Resource, Ct);
        await running;

        (await RegisteredTenantsAsync()).ShouldNotContain("leaving");
    }

    [Fact]
    public async Task removing_a_tenant_gives_up_rather_than_go_ahead_without_the_lock()
    {
        using var store = await CreateProvisionedStoreAsync(TimeSpan.FromMilliseconds(250));
        await AppendAsync(store, "leaving");

        await using var holder = await TakeTheLockAsync();

        await Should.ThrowAsync<InvalidOperationException>(
            () => store.Advanced.RemovePolecatManagedTenantsAsync(["leaving"], Ct));

        (await RegisteredTenantsAsync()).ShouldContain("leaving");
    }

    [Fact]
    public async Task removing_a_tenant_gives_the_lock_back()
    {
        using var store = await CreateProvisionedStoreAsync();
        await AppendAsync(store, "leaving");

        await store.Advanced.RemovePolecatManagedTenantsAsync(["leaving"], Ct);

        (await LockIsFreeAsync()).ShouldBeTrue();
    }

    [Fact]
    public async Task a_split_retry_does_not_wait_for_the_lock_and_comes_round_again()
    {
        using var setup = await CreateProvisionedStoreAsync();

        // Gives up on the blocked SPLIT in two seconds, so the tenant is left owed one
        using var store = BuildStore(commandTimeout: 2);
        store.Events.TenantOrdinals.SplitRetryTimeoutSeconds = 1;
        store.Events.TenantOrdinals.SplitRetryInterval = TimeSpan.Zero;

        var (connection, transaction) = await BlockSplitsOnStreamsAsync();
        await using (connection)
        {
            await AppendAsync(store, "newcomer");
            await transaction.RollbackAsync(Ct);
        }

        var ordinal = (int)(await TestSchema.QueryAsync(
            $"SELECT ordinal FROM [{Schema}].[pc_tenant_partitions] WHERE tenant_id = 'newcomer'")).Single()[0];

        (await StreamBoundariesAsync()).ShouldNotContain(ordinal);

        await using (var holder = await TakeTheLockAsync())
        {
            var stopwatch = Stopwatch.StartNew();
            await AppendAsync(store, "newcomer");
            stopwatch.Stop();

            stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5));
            (await StreamBoundariesAsync()).ShouldNotContain(ordinal);

            await holder.ReleaseGlobalLock(Resource, Ct);
        }

        await AppendAsync(store, "newcomer");

        (await StreamBoundariesAsync()).ShouldContain(ordinal);
    }

    private static async Task<IReadOnlyList<int>> StreamBoundariesAsync()
    {
        var rows = await TestSchema.QueryAsync("""
            SELECT CAST(v.value AS int)
            FROM sys.partition_range_values v
            JOIN sys.partition_functions f ON f.function_id = v.function_id
            WHERE f.name = 'pf_pc_streams_tenant_ordinal'
            """);
        return rows.Select(x => (int)x[0]).ToList();
    }
}
