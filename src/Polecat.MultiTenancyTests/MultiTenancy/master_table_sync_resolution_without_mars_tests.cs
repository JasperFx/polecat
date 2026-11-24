using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using JasperFx;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Storage;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     Caps the thread pool, so nothing else may run beside it.
/// </summary>
[CollectionDefinition("thread-pool-cap", DisableParallelization = true)]
public class ThreadPoolCapCollection;

/// <summary>
///     #1100, the leftover from #1090 / #1096: the synchronous <c>pc_tenants</c> read behind <c>TryResolveTenant</c>
///     (the master table tenancy's synchronous lookup of an uncached tenant) opens its connection without MARS. On
///     Linux and macOS SqlClient completes a synchronous read on a MARS connection through the thread pool, so a burst
///     of these lookups on pool threads could hold every pool thread while each one waited for another. The
///     asynchronous control-plane calls keep the connection string as configured.
/// </summary>
[Collection("thread-pool-cap")]
public class master_table_sync_resolution_without_mars_tests
{
    private readonly ITestOutputHelper _output;

    public master_table_sync_resolution_without_mars_tests(ITestOutputHelper output)
    {
        _output = output;
    }

    // ConnectTimeout: SqlClient creates a pool's physical connections one at a time, so in the cold burst below the
    // last lookup waits behind every other one's login, and on a loaded CI runner that ran past the 5 seconds the
    // test connection string allows (2 of 8 lookups failed "prior to obtaining a connection from the pool", PR
    // #1150, SQL Server 2022). The burst's own 20 second bound is what this test asserts, so the wait for a pooled
    // connection is given longer than that and can't fail it first.
    private static readonly string MarsConnectionString =
        new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            MultipleActiveResultSets = true, ConnectTimeout = 30
        }.ConnectionString;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string NewSchema() => "msm_" + Guid.NewGuid().ToString("N")[..10];

    private static StoreOptions Options() => new() { ConnectionString = MarsConnectionString };

    private static bool UsesMars(string connectionString) =>
        new SqlConnectionStringBuilder(connectionString).MultipleActiveResultSets;

    private static async Task<string> SeedAsync(params string[] tenantIds)
    {
        var schema = NewSchema();
        var seeder = new MasterTableTenancy(Options(), MarsConnectionString, schema);
        foreach (var tenantId in tenantIds)
        {
            await seeder.AddDatabaseRecordAsync(tenantId, MarsConnectionString, Ct);
        }

        return schema;
    }

    /// <summary>
    ///     Provisions the control table through an async entry point and caches no tenant, so the next uncached
    ///     lookup is one synchronous read of <c>pc_tenants</c>.
    /// </summary>
    private static async Task<RecordingTenancy> ProvisionedTenancyAsync(string schema)
    {
        var tenancy = new RecordingTenancy(Options(), schema);
        (await tenancy.FindDatabaseAsync("not_a_tenant", Ct)).ShouldBeNull();
        return tenancy;
    }

    [Fact]
    public async Task the_synchronous_read_opens_a_connection_without_mars_and_disposes_it()
    {
        var schema = await SeedAsync("tenant_a");
        var tenancy = await ProvisionedTenancyAsync(schema);

        // The asynchronous read is as it was: on the configured connection string, MARS and all
        tenancy.Opens.ShouldNotBeEmpty();
        tenancy.Opens.ShouldAllBe(x => !x.Synchronous && x.Mars);
        tenancy.Opens.ShouldAllBe(x => x.Disposed);
        tenancy.Opens.Clear();

        var database = ((ITenancy)tenancy).GetDatabase("tenant_a");
        database.ShouldNotBeNull();

        var open = tenancy.Opens.ShouldHaveSingleItem();
        open.Synchronous.ShouldBeTrue();
        open.Mars.ShouldBeFalse();
        open.Disposed.ShouldBeTrue();

        // To the same server and database
        var opened = new SqlConnectionStringBuilder(open.ConnectionString);
        var configured = new SqlConnectionStringBuilder(MarsConnectionString);
        opened.DataSource.ShouldBe(configured.DataSource);
        opened.InitialCatalog.ShouldBe(configured.InitialCatalog);

        // A cached tenant takes no connection at all, on either path
        ((ITenancy)tenancy).GetDatabase("tenant_a").ShouldBeSameAs(database);
        tenancy.TryResolveTenant("tenant_a", out var cached).ShouldBeTrue();
        cached.Database.ShouldBeSameAs(database);
        (await tenancy.FindDatabaseAsync("tenant_a", Ct)).ShouldBeSameAs(database);
        tenancy.Opens.Count.ShouldBe(1);
    }

    [Fact]
    public async Task an_unknown_tenant_is_read_without_mars_and_not_cached()
    {
        var schema = await SeedAsync("tenant_a");
        var tenancy = await ProvisionedTenancyAsync(schema);
        tenancy.Opens.Clear();

        Should.Throw<UnknownTenantIdException>(() => ((ITenancy)tenancy).GetDatabase("nobody"));
        Should.Throw<UnknownTenantIdException>(() => ((ITenancy)tenancy).GetDatabase("nobody"));

        // An unknown tenant is read again each time, and each read is synchronous, without MARS, and disposed
        tenancy.Opens.Count.ShouldBe(2);
        tenancy.Opens.ShouldAllBe(x => x.Synchronous && !x.Mars && x.Disposed);
    }

    [Fact]
    public void the_synchronous_connection_string_turns_off_mars_and_keeps_one_that_has_it_off()
    {
        using var mars = new ControlPlaneDataSource(MarsConnectionString);
        mars.ConnectionString.ShouldBe(MarsConnectionString);
        UsesMars(mars.SynchronousConnectionString).ShouldBeFalse();

        var withoutMars = new SqlConnectionStringBuilder(MarsConnectionString) { MultipleActiveResultSets = false }
            .ConnectionString;
        using var plain = new ControlPlaneDataSource(withoutMars);

        // Used as is, so both paths share one connection pool
        plain.SynchronousConnectionString.ShouldBeSameAs(withoutMars);
    }

    /// <summary>
    ///     More synchronous lookups of cold tenants than the thread pool may have threads, each on a pool thread, as
    ///     a burst of synchronous sessions opened from request threads would be. The pool is capped at its minimum
    ///     for the burst, so it can't inject threads to get out of it: on Linux and macOS a read over MARS needs a
    ///     second pool thread to complete, so before the fix every pool thread blocked in a read none of them could
    ///     finish. Without MARS the reads finish on the threads that started them.
    /// </summary>
    /// <remarks>
    ///     Waits a bounded time for the burst rather than measuring how long it took. The pool is put back before
    ///     anything is asserted, so a failure leaves the blocked reads to finish once the pool can inject threads
    ///     again.
    /// </remarks>
    [Fact]
    public async Task a_burst_of_synchronous_lookups_on_a_capped_thread_pool_completes()
    {
        ThreadPool.GetMinThreads(out var minWorkers, out _);
        ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);

        // Twice as many lookups as the capped pool has threads, every one of them for a different cold tenant
        var lookups = minWorkers * 2;
        var tenantIds = Enumerable.Range(0, lookups).Select(i => $"burst_{i}").ToArray();
        var schema = await SeedAsync(tenantIds);
        var tenancy = await ProvisionedTenancyAsync(schema);
        tenancy.Opens.Clear();

        var failures = new ConcurrentQueue<Exception>();
        var databases = new ConcurrentDictionary<string, PolecatDatabase>();
        Task[] tasks;
        bool completed;
        var stopwatch = Stopwatch.StartNew();

        ThreadPool.SetMaxThreads(minWorkers, maxIo).ShouldBeTrue();
        try
        {
            tasks = tenantIds.Select(tenantId => Task.Run(() =>
            {
                try
                {
                    databases[tenantId] = ((ITenancy)tenancy).GetDatabase(tenantId);
                }
                catch (Exception e)
                {
                    failures.Enqueue(e);
                }
            })).ToArray();

            // A blocking wait, so this thread needs nothing from the pool either
            completed = Task.WaitAll(tasks, TimeSpan.FromSeconds(20));
        }
        finally
        {
            ThreadPool.SetMaxThreads(maxWorkers, maxIo).ShouldBeTrue();
        }

        var elapsed = stopwatch.Elapsed;
        _output.WriteLine(
            $"{lookups} synchronous lookups on a pool capped at {minWorkers} threads: completed {completed} after {elapsed.TotalMilliseconds:N0} ms");

        completed.ShouldBeTrue(
            $"{lookups} synchronous tenant lookups on a thread pool capped at {minWorkers} threads didn't finish within 20 seconds");

        failures.ShouldBeEmpty(string.Join(Environment.NewLine, failures.Select(x => x.ToString())));
        databases.Count.ShouldBe(lookups);

        // One synchronous read for each tenant, none of them over MARS, every connection disposed
        tenancy.Opens.Count.ShouldBe(lookups);
        tenancy.Opens.ShouldAllBe(x => x.Synchronous && !x.Mars && x.Disposed);
    }

    private sealed class Open(bool synchronous, string connectionString)
    {
        private volatile bool _disposed;

        public bool Synchronous => synchronous;

        // Read after the open: SqlClient leaves the password out of it, but not the MARS keyword
        public string ConnectionString => connectionString;
        public bool Mars => UsesMars(connectionString);

        public bool Disposed
        {
            get => _disposed;
            set => _disposed = value;
        }
    }

    /// <summary>
    ///     Records every connection the control plane opens: whether the open was synchronous, its connection
    ///     string, and whether it has been disposed since.
    /// </summary>
    private sealed class RecordingTenancy(StoreOptions options, string schema)
        : MasterTableTenancy(options, MarsConnectionString, schema)
    {
        public ConcurrentQueue<Open> Opens { get; } = new();

        protected override DbDataSource BuildDataSource(string connectionString)
            => new RecordingDataSource(base.BuildDataSource(connectionString), Opens);
    }

    private sealed class RecordingDataSource(DbDataSource inner, ConcurrentQueue<Open> opens) : DbDataSource
    {
        public override string ConnectionString => inner.ConnectionString;

        protected override DbConnection CreateDbConnection() => inner.CreateConnection();

        protected override DbConnection OpenDbConnection() => record(inner.OpenConnection(), true);

        protected override async ValueTask<DbConnection> OpenDbConnectionAsync(CancellationToken cancellationToken = default)
            => record(await inner.OpenConnectionAsync(cancellationToken), false);

        private DbConnection record(DbConnection connection, bool synchronous)
        {
            var open = new Open(synchronous, connection.ConnectionString);
            connection.Disposed += (_, _) => open.Disposed = true;
            opens.Enqueue(open);
            return connection;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
