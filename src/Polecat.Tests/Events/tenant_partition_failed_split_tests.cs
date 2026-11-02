using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Tests.Harness;
using Polecat.Tests.Session;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

public record FailedSplitHappened(int Number);

public class FailedSplitDocument
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>
///     #892: a tenant's first SPLIT that loses to another session's lock. #864 stopped a save blocking
///     itself this way, but nothing stops somebody else's lock doing it, and Weasel isolates the failure
///     per table, so the save still has an ordinal to write with and the rows land in the partition next
///     door. The write is meant to survive that. What it must not do is go unnoticed and stay that way.
/// </summary>
/// <remarks>
///     Each store's connection string gives up after two seconds, so a blocked first attempt is not 30
///     seconds of test. A second store sets the schema up first, because that needs the time it needs. The
///     lock is a Repeatable Read scan of the incumbent's rows, which holds what a SPLIT has to wait for and
///     lets the newcomer's own insert through, which is the case the issue describes.
/// </remarks>
[Collection("tenant-partitioning")]
public class tenant_partition_failed_split_tests : IAsyncLifetime
{
    private const string Schema = "pt_failed_split";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly RecordingLogger _logger = new();

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private DocumentStore CreateStore(AutoCreate autoCreate, int commandTimeout,
        Action<StoreOptions>? configure) => DocumentStore.For(opts =>
    {
        opts.ConnectionString = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            CommandTimeout = commandTimeout
        }.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.AutoCreateSchemaObjects = autoCreate;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.EventGraph.UseTenantPartitionedEvents = true;
        opts.Events.AddEventType<FailedSplitHappened>();
        opts.Events.AddEventType<StudentEnrolled>();
        opts.DotNetLogger = _logger;
        configure?.Invoke(opts);
    });

    private async Task<DocumentStore> CreateProvisionedStoreAsync(Action<StoreOptions>? configure = null)
    {
        using (var setup = CreateStore(AutoCreate.All, StoreOptions.DefaultTimeout, configure))
        {
            await setup.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
            await AppendAsync(setup, "incumbent");
        }

        var store = CreateStore(AutoCreate.None, 2, configure);
        store.Events.TenantOrdinals.SplitRetryTimeoutSeconds = 1;
        store.Events.TenantOrdinals.SplitRetryInterval = TimeSpan.Zero;
        return store;
    }

    private static IDocumentSession SessionFor(DocumentStore store, string tenant)
        => store.LightweightSession(new SessionOptions { TenantId = tenant });

    private static async Task<Guid[]> AppendAsync(DocumentStore store, string tenant, int streams = 1)
    {
        var streamIds = Enumerable.Range(0, streams).Select(_ => Guid.NewGuid()).ToArray();

        await using var session = SessionFor(store, tenant);
        foreach (var streamId in streamIds)
        {
            session.Events.StartStream(streamId, new FailedSplitHappened(1));
        }

        await session.SaveChangesAsync(Ct);
        return streamIds;
    }

    private static async Task<TableLock> LockAsync(string table) => await TableLock.TakeAsync(table);

    private static async Task<IReadOnlyList<int>> BoundariesOfAsync(string table)
    {
        var rows = await TestSchema.QueryAsync($"""
            SELECT CAST(v.value AS int)
            FROM sys.partition_range_values v
            JOIN sys.partition_functions f ON f.function_id = v.function_id
            WHERE f.name = 'pf_{table}_tenant_ordinal'
            """);
        return rows.Select(x => (int)x[0]).ToList();
    }

    private static async Task<int> OrdinalOfAsync(string tenant)
    {
        var rows = await TestSchema.QueryAsync(
            $"SELECT ordinal FROM [{Schema}].[pc_tenant_partitions] WHERE tenant_id = '{tenant}'");
        return (int)rows.Single()[0];
    }

    /// <summary>
    ///     The boundary opening the partition each of the tenant's rows is in, as in
    ///     <see cref="tenant_partitioned_unseen_tenant_tests" />: RANGE RIGHT, so partition N starts at
    ///     boundary N - 1, and a row is at home exactly when that boundary is its tenant's ordinal.
    /// </summary>
    private static async Task<IReadOnlyList<object>> PartitionStartsOfRowsAsync(string table, string tenant)
    {
        var rows = await TestSchema.QueryAsync($"""
            SELECT CAST(v.value AS int)
            FROM [{Schema}].[{table}] r
            LEFT JOIN sys.partition_range_values v
                ON v.boundary_id = $PARTITION.pf_{table}_tenant_ordinal(r.tenant_ordinal) - 1
               AND v.function_id = (SELECT function_id FROM sys.partition_functions
                                    WHERE name = 'pf_{table}_tenant_ordinal')
            WHERE r.tenant_id = '{tenant}'
            """);
        return rows.Select(x => x[0]).ToList();
    }

    private static async Task ShouldBeInItsOwnPartitionsAsync(string table, string tenant, int rows)
    {
        var ordinal = await OrdinalOfAsync(tenant);

        (await BoundariesOfAsync(table)).ShouldContain(ordinal);
        (await PartitionStartsOfRowsAsync(table, tenant)).ShouldBe(Enumerable.Repeat<object>(ordinal, rows));
    }

    private static async Task ShouldBeUnsplitAsync(string table, string tenant, int rows)
    {
        var ordinal = await OrdinalOfAsync(tenant);

        (await BoundariesOfAsync(table)).ShouldNotContain(ordinal);

        var starts = await PartitionStartsOfRowsAsync(table, tenant);
        starts.Count.ShouldBe(rows);
        starts.ShouldNotContain(ordinal);
    }

    /// <summary>
    ///     Measured with sys.partitions as well as the function, because $PARTITION answers from the
    ///     boundaries and would say this after any SPLIT, moved or not.
    /// </summary>
    private static async Task<long> RowsPhysicallyInPartitionOfAsync(string table, int ordinal)
    {
        var rows = await TestSchema.QueryAsync($"""
            SELECT SUM(p.rows)
            FROM sys.partitions p
            WHERE p.object_id = OBJECT_ID('[{Schema}].[{table}]') AND p.index_id IN (0, 1)
              AND p.partition_number = $PARTITION.pf_{table}_tenant_ordinal({ordinal})
            """);
        return (long)rows.Single()[0];
    }

    [Fact]
    public async Task the_save_survives_and_a_later_save_moves_the_rows_home()
    {
        using var store = await CreateProvisionedStoreAsync();
        var incumbent = await OrdinalOfAsync("incumbent");

        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer", 3);
        }

        // pc_events was not locked, which is the partial failure the per-table statuses exist to show
        await ShouldBeUnsplitAsync("pc_streams", "newcomer", 3);
        await ShouldBeInItsOwnPartitionsAsync("pc_events", "newcomer", 3);
        (await RowsPhysicallyInPartitionOfAsync("pc_streams", incumbent)).ShouldBe(4);

        await AppendAsync(store, "newcomer");

        await ShouldBeInItsOwnPartitionsAsync("pc_streams", "newcomer", 4);
        (await RowsPhysicallyInPartitionOfAsync("pc_streams", incumbent)).ShouldBe(1);
        (await RowsPhysicallyInPartitionOfAsync("pc_streams", await OrdinalOfAsync("newcomer"))).ShouldBe(4);
    }

    [Fact]
    public async Task a_failed_split_can_be_read_back_while_it_waits()
    {
        using var store = await CreateProvisionedStoreAsync();

        Guid streamId;
        await using (await LockAsync("pc_streams"))
        {
            streamId = (await AppendAsync(store, "newcomer")).Single();
        }

        await using var query = store.QuerySession(new SessionOptions { TenantId = "newcomer" });
        (await query.Events.FetchStreamAsync(streamId, token: Ct)).Count.ShouldBe(1);

        await using var incumbent = store.QuerySession(new SessionOptions { TenantId = "incumbent" });
        (await incumbent.Events.FetchStreamAsync(streamId, token: Ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task a_failure_is_a_warning_naming_the_tenant_and_the_table()
    {
        using var store = await CreateProvisionedStoreAsync();

        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer");
        }

        var warning = _logger.Warnings.ShouldHaveSingleItem();
        warning.ShouldContain("newcomer");
        warning.ShouldContain("pc_streams");
        warning.ShouldNotContain("pc_events");
    }

    [Fact]
    public async Task nothing_is_logged_when_the_split_works()
    {
        using var store = await CreateProvisionedStoreAsync();

        await AppendAsync(store, "newcomer");

        _logger.Warnings.ShouldBeEmpty();
    }

    [Fact]
    public async Task a_split_that_fails_again_still_leaves_the_save_standing()
    {
        using var store = await CreateProvisionedStoreAsync();

        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer");
            await AppendAsync(store, "newcomer");
            await AppendAsync(store, "newcomer");

            await ShouldBeUnsplitAsync("pc_streams", "newcomer", 3);
        }

        _logger.Warnings.Count.ShouldBe(3);

        await AppendAsync(store, "newcomer");

        await ShouldBeInItsOwnPartitionsAsync("pc_streams", "newcomer", 4);
    }

    [Fact]
    public async Task once_it_works_there_are_no_more_attempts()
    {
        using var store = await CreateProvisionedStoreAsync();

        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer");
        }

        await AppendAsync(store, "newcomer");
        var warnings = _logger.Warnings.Count;

        // A retry here would run into this lock and say so
        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer");
        }

        _logger.Warnings.Count.ShouldBe(warnings);
    }

    [Fact]
    public async Task one_save_makes_one_attempt_however_many_streams_it_has()
    {
        using var store = await CreateProvisionedStoreAsync();

        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer");
            var warnings = _logger.Warnings.Count;

            store.Events.TenantOrdinals.SplitRetryInterval = TimeSpan.FromMinutes(1);
            await Task.Delay(10, Ct);

            await AppendAsync(store, "newcomer", 5);

            _logger.Warnings.Count.ShouldBe(warnings + 1);
        }
    }

    [Fact]
    public async Task saves_inside_the_retry_interval_do_not_attempt_it()
    {
        using var store = await CreateProvisionedStoreAsync();
        store.Events.TenantOrdinals.SplitRetryInterval = TimeSpan.FromMinutes(1);

        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer");
            await AppendAsync(store, "newcomer");
        }

        await AppendAsync(store, "newcomer");

        _logger.Warnings.Count.ShouldBe(1);
        await ShouldBeUnsplitAsync("pc_streams", "newcomer", 3);
    }

    /// <summary>
    ///     #864's rule, which the retry has to keep: a SPLIT can't run behind a lock on the table that the
    ///     same session holds. Here the session has one before the save starts, so the start of the save
    ///     is too late, and an attempt would only wait on itself and report the failure again.
    /// </summary>
    [Fact]
    public async Task a_session_already_holding_a_lock_on_the_table_does_not_retry()
    {
        using var store = await CreateProvisionedStoreAsync();
        var incumbentStream = (await AppendAsync(store, "incumbent")).Single();

        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer");
        }

        var warnings = _logger.Warnings.Count;

        await using (var session = SessionFor(store, "incumbent"))
        {
            await session.Events.AppendExclusive(incumbentStream, Ct, new FailedSplitHappened(2));
            session.ForTenant("newcomer").Events.StartStream(Guid.NewGuid(), new FailedSplitHappened(2));
            await session.SaveChangesAsync(Ct);
        }

        _logger.Warnings.Count.ShouldBe(warnings);
        await ShouldBeUnsplitAsync("pc_streams", "newcomer", 2);

        await AppendAsync(store, "newcomer");

        await ShouldBeInItsOwnPartitionsAsync("pc_streams", "newcomer", 3);
    }

    /// <summary>
    ///     An exclusive fetch is the one write that can't retry at the start of the save, because its lock
    ///     is older than that, so it has to happen ahead of the lock.
    /// </summary>
    [Fact]
    public async Task an_exclusive_fetch_retries_ahead_of_its_own_lock()
    {
        using var store = await CreateProvisionedStoreAsync();

        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer");
        }

        var warnings = _logger.Warnings.Count;

        await using (var session = SessionFor(store, "newcomer"))
        {
            var stream = await session.Events.FetchForExclusiveWriting<StudentCourseEnrollment>(Guid.NewGuid(), Ct);
            stream.AppendOne(new StudentEnrolled("Ann", "Math"));
            await session.SaveChangesAsync(Ct);
        }

        _logger.Warnings.Count.ShouldBe(warnings);
        await ShouldBeInItsOwnPartitionsAsync("pc_streams", "newcomer", 2);
    }

    /// <summary>The tenant's sequence is provisioned and cached by the failed save, which must not hide it.</summary>
    [Fact]
    public async Task an_append_to_an_existing_stream_retries_too()
    {
        using var store = await CreateProvisionedStoreAsync();

        Guid streamId;
        await using (await LockAsync("pc_streams"))
        {
            streamId = (await AppendAsync(store, "newcomer")).Single();
        }

        await using (var session = SessionFor(store, "newcomer"))
        {
            session.Events.Append(streamId, new FailedSplitHappened(2));
            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync("pc_streams", "newcomer", 1);
    }

    [Fact]
    public async Task add_polecat_managed_tenants_catches_it_up_on_demand()
    {
        using var store = await CreateProvisionedStoreAsync();
        store.Events.TenantOrdinals.SplitRetryInterval = TimeSpan.FromMinutes(1);

        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer");
        }

        await ShouldBeUnsplitAsync("pc_streams", "newcomer", 1);

        await store.Advanced.AddPolecatManagedTenantsAsync(Ct, "newcomer");

        await ShouldBeInItsOwnPartitionsAsync("pc_streams", "newcomer", 1);
    }

    [Fact]
    public async Task the_next_new_tenant_repairs_it_as_well()
    {
        using var store = await CreateProvisionedStoreAsync();
        store.Events.TenantOrdinals.SplitRetryInterval = TimeSpan.FromMinutes(1);

        await using (await LockAsync("pc_streams"))
        {
            await AppendAsync(store, "newcomer", 2);
        }

        await AppendAsync(store, "another");

        await ShouldBeInItsOwnPartitionsAsync("pc_streams", "newcomer", 2);
        await ShouldBeInItsOwnPartitionsAsync("pc_streams", "another", 1);
    }

    [Fact]
    public async Task a_document_write_retries_its_split()
    {
        using var store = await CreateProvisionedStoreAsync(opts =>
        {
            opts.EventGraph.UseTenantPartitionedEvents = false;
            opts.Policies.AllDocumentsAreMultiTenantedWithPartitioning();
            opts.Schema.For<FailedSplitDocument>();
        });

        await using (var session = SessionFor(store, "incumbent"))
        {
            session.Store(new FailedSplitDocument { Id = Guid.NewGuid(), Name = "first" });
            await session.SaveChangesAsync(Ct);
        }

        const string table = "pc_doc_failedsplitdocument";

        await using (await TableLock.TakeAsync(table))
        await using (var session = SessionFor(store, "newcomer"))
        {
            session.Store(new FailedSplitDocument { Id = Guid.NewGuid(), Name = "second" });
            await session.SaveChangesAsync(Ct);
        }

        _logger.Warnings.ShouldNotBeEmpty();
        _logger.Warnings.ShouldAllBe(x => x.Contains(table));
        await ShouldBeUnsplitAsync(table, "newcomer", 1);

        await using (var session = SessionFor(store, "newcomer"))
        {
            session.Store(new FailedSplitDocument { Id = Guid.NewGuid(), Name = "third" });
            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync(table, "newcomer", 2);
    }

    [Theory]
    [InlineData(30, 3, 3)]
    [InlineData(2, 3, 2)]
    [InlineData(0, 3, 3)]
    public void a_retry_is_limited_without_raising_a_lower_limit(int configured, int limit, int expected)
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                CommandTimeout = configured
            }.ConnectionString;
        });

        using var connection = store.Database.WithCommandTimeout(limit).CreateConnection();

        connection.CommandTimeout.ShouldBe(expected);
    }

    [Fact]
    public void the_default_limits_are_well_under_the_command_timeout()
    {
        using var store = CreateStore(AutoCreate.None, StoreOptions.DefaultTimeout, null);

        store.Events.TenantOrdinals.SplitRetryTimeoutSeconds.ShouldBeLessThan(StoreOptions.DefaultTimeout);
        store.Events.TenantOrdinals.SplitRetryInterval.ShouldBeGreaterThan(
            TimeSpan.FromSeconds(store.Events.TenantOrdinals.SplitRetryTimeoutSeconds));
    }

    [Fact]
    public void add_polecat_gives_the_store_the_hosts_logging()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
        services.AddPolecat(opts => opts.ConnectionString = ConnectionSource.ConnectionString);

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<StoreOptions>().LogFactory.ShouldBeSameAs(NullLoggerFactory.Instance);
    }

    private sealed class TableLock : IAsyncDisposable
    {
        private readonly SqlConnection _connection;
        private readonly SqlTransaction _transaction;

        private TableLock(SqlConnection connection, SqlTransaction transaction)
        {
            _connection = connection;
            _transaction = transaction;
        }

        public static async Task<TableLock> TakeAsync(string table)
        {
            var connection = new SqlConnection(ConnectionSource.ConnectionString);
            await connection.OpenAsync(Ct);
            var transaction = connection.BeginTransaction();

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText =
                $"SELECT COUNT(*) FROM [{Schema}].[{table}] WITH (REPEATABLEREAD) WHERE tenant_id = 'incumbent'";
            await command.ExecuteScalarAsync(Ct);

            return new TableLock(connection, transaction);
        }

        public async ValueTask DisposeAsync()
        {
            await _transaction.RollbackAsync();
            await _transaction.DisposeAsync();
            await _connection.DisposeAsync();
        }
    }

    private sealed class RecordingLogger : ILogger
    {
        private readonly ConcurrentQueue<string> _warnings = new();

        public IReadOnlyList<string> Warnings => _warnings.ToArray();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning) _warnings.Enqueue(formatter(state, exception));
        }
    }
}
