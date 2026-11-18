using JasperFx;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Polecat.TestUtils;
using Polly;
using Weasel.Core;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     #991: the synchronous tenant lookups (<c>ITenancy.GetDatabase</c>, <c>GetConnectionFactory</c>, and the
///     synchronous session factories behind them) resolve an uncached tenant with one synchronous read of
///     <c>pc_tenants</c> (weasel#838's <c>TryResolveTenant</c>) instead of blocking a pool thread on
///     <c>ResolveTenantAsync</c>. A burst of cold tenants resolved that way could starve the thread pool
///     (marten#5985).
/// </summary>
/// <remarks>
///     The counts are the evidence. <see cref="CountingTenancy" /> sees every control-plane call the base
///     makes, synchronous and asynchronous, and a resolution that went through the old path shows up as an
///     <c>ExecuteAsync</c> call.
/// </remarks>
public class master_table_sync_resolution_tests
{
    private static string NewSchema() => "msr_" + Guid.NewGuid().ToString("N")[..10];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static StoreOptions Options() => new() { ConnectionString = ConnectionSource.ConnectionString };

    /// <summary>
    ///     Registers the tenants through one instance, so the instance under test starts with nothing cached.
    /// </summary>
    private static async Task<string> SeedAsync(params (string TenantId, bool Disabled)[] tenants)
    {
        var schema = NewSchema();
        var seeder = new MasterTableTenancy(Options(), ConnectionSource.ConnectionString, schema);

        foreach (var (tenantId, disabled) in tenants)
        {
            await seeder.AddDatabaseRecordAsync(tenantId, ConnectionSource.ConnectionString, Ct);
            if (disabled) await seeder.DisableTenantAsync(tenantId, Ct);
        }

        return schema;
    }

    /// <summary>
    ///     Provisions the control table on a fresh instance through an async entry point, the way any session
    ///     opened asynchronously or a start-up <c>BuildDatabasesAsync</c> would, and caches no tenant.
    /// </summary>
    private static async Task<CountingTenancy> ProvisionedTenancyAsync(string schema, StoreOptions? options = null)
    {
        var tenancy = new CountingTenancy(options ?? Options(), schema);
        (await tenancy.FindDatabaseAsync("not_a_tenant", Ct)).ShouldBeNull();
        tenancy.Reset();
        return tenancy;
    }

    [Fact]
    public async Task a_cold_tenant_resolves_synchronously_with_no_async_control_plane_call()
    {
        var schema = await SeedAsync(("tenant_a", false));
        var tenancy = await ProvisionedTenancyAsync(schema);

        var database = ((ITenancy)tenancy).GetDatabase("tenant_a");

        database.ShouldNotBeNull();
        tenancy.SynchronousCalls.ShouldBe(1);
        tenancy.AsynchronousCalls.ShouldBe(0);

        // The synchronous read cached it in the same cache the async path uses.
        ((ITenancy)tenancy).GetDatabase("tenant_a").ShouldBeSameAs(database);
        (await tenancy.FindDatabaseAsync("tenant_a", Ct)).ShouldBeSameAs(database);
        tenancy.SynchronousCalls.ShouldBe(1);
    }

    [Fact]
    public async Task the_connection_factory_takes_the_same_synchronous_path()
    {
        var schema = await SeedAsync(("tenant_a", false));
        var tenancy = await ProvisionedTenancyAsync(schema);

        ((ITenancy)tenancy).GetConnectionFactory("tenant_a").ShouldNotBeNull();

        tenancy.SynchronousCalls.ShouldBe(1);
        tenancy.AsynchronousCalls.ShouldBe(0);
    }

    /// <summary>
    ///     #655's refusals still tell an unknown tenant from a disabled one on the synchronous path, and neither
    ///     needs an async call to do it.
    /// </summary>
    [Fact]
    public async Task an_unknown_and_a_disabled_tenant_are_refused_synchronously()
    {
        var schema = await SeedAsync(("tenant_off", true));
        var tenancy = await ProvisionedTenancyAsync(schema);

        Should.Throw<DisabledTenantException>(() => ((ITenancy)tenancy).GetDatabase("tenant_off"));

        var unknown = Should.Throw<UnknownTenantIdException>(() => ((ITenancy)tenancy).GetDatabase("nobody"));
        unknown.ShouldNotBeOfType<DisabledTenantException>();

        tenancy.SynchronousCalls.ShouldBe(2);
        tenancy.AsynchronousCalls.ShouldBe(0);
    }

    /// <summary>
    ///     #1087 (marten#6058, weasel#850): a process whose first touch is synchronous used to decline every
    ///     uncached tenant until something asynchronous had provisioned the control table, and blocked a thread on
    ///     <c>ResolveTenantAsync</c> for each of them. The control table is now provisioned synchronously, so even
    ///     the first lookup makes no asynchronous call.
    /// </summary>
    [Fact]
    public async Task a_cold_process_resolves_its_first_tenant_synchronously()
    {
        var schema = await SeedAsync(("tenant_a", false), ("tenant_b", false));
        var tenancy = new CountingTenancy(Options(), schema);

        ((ITenancy)tenancy).GetDatabase("tenant_a").ShouldNotBeNull();
        ((ITenancy)tenancy).GetConnectionFactory("tenant_b").ShouldNotBeNull();
        Should.Throw<UnknownTenantIdException>(() => ((ITenancy)tenancy).GetDatabase("nobody"));

        tenancy.SynchronousCalls.ShouldBe(3);
        tenancy.AsynchronousCalls.ShouldBe(0);
        tenancy.FindCachedDatabase("tenant_b").ShouldNotBeNull();
    }

    [Fact]
    public async Task a_cold_process_creates_a_missing_control_table_synchronously()
    {
        var schema = NewSchema();
        var tenancy = new CountingTenancy(Options(), schema);

        // No control table, nor even its schema, yet: the synchronous lookup creates it, then reads it
        tenancy.TryResolveTenant("nobody", out var nobody).ShouldBeTrue();
        nobody.Database.ShouldBeNull();
        tenancy.AsynchronousCalls.ShouldBe(0);

        // The same table the asynchronous migration builds: nothing is left for it to do
        await using (var conn = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(Ct);
            var migration = await SchemaMigration.DetermineAsync(conn, Ct, new TenantsTable(schema));
            migration.Difference.ShouldBe(SchemaPatchDifference.None);
        }

        var seeder = new MasterTableTenancy(Options(), ConnectionSource.ConnectionString, schema);
        await seeder.AddDatabaseRecordAsync("created1", ConnectionSource.ConnectionString, Ct);

        ((ITenancy)tenancy).GetDatabase("created1").ShouldNotBeNull();
        tenancy.AsynchronousCalls.ShouldBe(0);
    }

    [Fact]
    public async Task a_cold_process_does_not_create_the_control_table_when_auto_create_is_off()
    {
        var schema = NewSchema();
        var options = Options();
        options.AutoCreateSchemaObjects = AutoCreate.None;
        var tenancy = new CountingTenancy(options, schema);

        // The operator owns the schema, so nothing is created, and the read fails as the asynchronous one does
        Should.Throw<SqlException>(() => tenancy.TryResolveTenant("nobody", out _)).Number.ShouldBe(208);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT OBJECT_ID(@name, 'U');";
        cmd.Parameters.AddWithValue("@name", $"[{schema}].[pc_tenants]");
        (await cmd.ExecuteScalarAsync(Ct)).ShouldBe(DBNull.Value);
    }

    /// <summary>
    ///     The synchronous read runs through <c>StoreOptions.ResiliencePipeline</c>: a transient failure of the
    ///     read is retried by the pipeline rather than reaching the caller.
    /// </summary>
    [Fact]
    public async Task the_synchronous_read_runs_through_the_resilience_pipeline()
    {
        var schema = await SeedAsync(("tenant_a", false));

        var retries = 0;
        var options = Options();
        options.ConfigurePolly(builder => builder.AddRetry(new Polly.Retry.RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<TransientReadException>(),
            MaxRetryAttempts = 1,
            Delay = TimeSpan.Zero,
            OnRetry = _ =>
            {
                retries++;
                return default;
            }
        }));

        var tenancy = await ProvisionedTenancyAsync(schema, options);
        tenancy.FailNextSynchronousRead = true;

        ((ITenancy)tenancy).GetDatabase("tenant_a").ShouldNotBeNull();

        retries.ShouldBe(1);
        tenancy.AsynchronousCalls.ShouldBe(0);
    }

    private sealed class TransientReadException : Exception;

    private sealed class CountingTenancy(StoreOptions options, string schema)
        : MasterTableTenancy(options, ConnectionSource.ConnectionString, schema)
    {
        private int _synchronous;
        private int _asynchronous;

        public int SynchronousCalls => _synchronous;
        public int AsynchronousCalls => _asynchronous;

        public bool FailNextSynchronousRead { get; set; }

        public void Reset()
        {
            _synchronous = 0;
            _asynchronous = 0;
        }

        protected override T ExecuteSynchronously<T>(Func<T> operation)
        {
            Interlocked.Increment(ref _synchronous);

            // Inside the operation handed to Polecat's override, so a failure here is one the pipeline sees.
            return base.ExecuteSynchronously(() =>
            {
                if (FailNextSynchronousRead)
                {
                    FailNextSynchronousRead = false;
                    throw new TransientReadException();
                }

                return operation();
            });
        }

        protected override Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken token)
        {
            Interlocked.Increment(ref _asynchronous);
            return base.ExecuteAsync(operation, token);
        }
    }
}
