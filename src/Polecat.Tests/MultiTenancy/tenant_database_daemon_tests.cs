using JasperFx;
using JasperFx.Core;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon;
using Polecat.Storage;
using Polecat.TestUtils;
using Polecat.Tests.Daemon;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     What a daemon needs from a tenant database beyond its projections. Neither of these could work on
///     database-per-tenant, and both have a reason nobody saw it: the dead-letter tests all run on one
///     database, and <c>IEventStore.BuildProjectionDaemonAsync(DatabaseId)</c> is called by Wolverine only.
/// </summary>
public class tenant_database_daemon_tests : IClassFixture<TenantDaemonDatabasesFixture>
{
    private const string TenantA = "dl_tenant_a";
    private const string TenantB = "dl_tenant_b";

    internal static readonly string DbA = ConnectionSource.Scoped("tdaemon_a");
    internal static readonly string DbB = ConnectionSource.Scoped("tdaemon_b");

    private static string ConnectionFor(string db) => ConnectionSource.ConnectionStringFor(db);

    private static string NewSchema() => "tdaemon_" + Guid.NewGuid().ToString("N")[..8];

    private static void ConfigureBasics(StoreOptions opts, string schema)
    {
        opts.DatabaseSchemaName = schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Projections.Errors.SkipApplyErrors = true;
        opts.Projections.Add<PoisonEventProjection>(ProjectionLifecycle.Async);
    }

    private static DocumentStore StaticTenancy(string schema)
    {
        return DocumentStore.For(opts =>
        {
            ConfigureBasics(opts, schema);
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant(TenantA, ConnectionFor(DbA));
                tenancy.AddTenant(TenantB, ConnectionFor(DbB));
            });
        });
    }

    private static async Task<DocumentStore> MasterTableTenancyAsync(string schema)
    {
        var store = DocumentStore.For(opts =>
        {
            ConfigureBasics(opts, schema);
            opts.MultiTenantedMasterTable(ConnectionFor(DbA), schema);
        });

        var tenancy = (MasterTableTenancy)store.Options.Tenancy!;
        await tenancy.AddDatabaseRecordAsync(TenantA, ConnectionFor(DbA), TestContext.Current.CancellationToken);
        await tenancy.AddDatabaseRecordAsync(TenantB, ConnectionFor(DbB), TestContext.Current.CancellationToken);

        return store;
    }

    private static async Task<DocumentStore> BuildStoreAsync(bool masterTable)
    {
        var schema = NewSchema();
        return masterTable ? await MasterTableTenancyAsync(schema) : StaticTenancy(schema);
    }

    private static DeadLetterEvent DeadLetterFor(ShardName shard)
    {
        return new DeadLetterEvent
        {
            Id = Guid.NewGuid(),
            ProjectionName = shard.Name,
            ShardName = shard.ShardKey,
            EventSequence = 4,
            Timestamp = DateTimeOffset.UtcNow,
            ExceptionType = nameof(InvalidOperationException),
            ExceptionMessage = "boom"
        };
    }

    /// <summary>
    ///     #871 item 2. Only the store's own database had its back-reference wired, and the sessions were
    ///     opened with no database at all, so a tenant database could neither record a dead letter nor count
    ///     one. Each database has to answer for itself.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task a_tenant_database_keeps_its_own_dead_letters(bool masterTable)
    {
        var token = TestContext.Current.CancellationToken;
        using var store = await BuildStoreAsync(masterTable);

        var shard = new ShardName("Alpha");
        IEventDatabase databaseA = store.Options.Tenancy!.GetDatabase(TenantA);
        IEventDatabase databaseB = store.Options.Tenancy!.GetDatabase(TenantB);

        await databaseB.EnsureStorageExistsAsync(typeof(IEvent), token);
        await databaseA.StoreDeadLetterEventAsync(null!, DeadLetterFor(shard), token);

        (await databaseA.CountDeadLetterEventsAsync(shard, token)).ShouldBe(1);
        (await databaseA.FetchDeadLetterCountsAsync(token)).ShouldHaveSingleItem().Count.ShouldBe(1);
        (await databaseA.QueryDeadLetterEventsAsync(shard, null, 0, 10, token)).ShouldHaveSingleItem();

        (await databaseB.CountDeadLetterEventsAsync(shard, token)).ShouldBe(0);
        (await databaseB.FetchDeadLetterCountsAsync(token)).ShouldBeEmpty();
        (await databaseB.QueryDeadLetterEventsAsync(shard, null, 0, 10, token)).ShouldBeEmpty();
    }

    [Fact]
    public async Task a_daemon_on_a_tenant_database_records_its_dead_letters_there()
    {
        var token = TestContext.Current.CancellationToken;
        using var store = StaticTenancy(NewSchema());

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
        {
            session.Events.StartStream(Guid.NewGuid(), new DeadLetterThing(Guid.NewGuid(), "boom", Poison: true));
            await session.SaveChangesAsync(token);
        }

        IEventDatabase database = store.Options.Tenancy!.GetDatabase(TenantA);

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync(TenantA);
        await daemon.StartAllAsync();
        var shard = daemon.CurrentAgents().Single().Name;

        var deadline = DateTimeOffset.UtcNow.Add(15.Seconds());
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await database.CountDeadLetterEventsAsync(shard, token) > 0)
            {
                await daemon.StopAllAsync();
                return;
            }

            await Task.Delay(100, token);
        }

        throw new TimeoutException("The dead letter never reached the tenant's database");
    }

    /// <summary>
    ///     #871 item 4. Wolverine's agent family hands this overload the id of the database it wants, and
    ///     Polecat threw the id away and built the default daemon, which under database-per-tenant refuses.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task a_daemon_can_be_built_for_a_database_by_its_id(bool masterTable)
    {
        using var store = await BuildStoreAsync(masterTable);

        foreach (var (tenantId, catalog) in new[] { (TenantA, DbA), (TenantB, DbB) })
        {
            var descriptor = store.Options.Tenancy!.GetDatabase(tenantId).Describe();
            descriptor.DatabaseName.ShouldBe(catalog);

            using var daemon = await ((IEventStore)store).BuildProjectionDaemonAsync(
                new DatabaseId(descriptor.ServerName, descriptor.DatabaseName));

            ((PolecatProjectionDaemon)daemon).Database.Identifier.ShouldBe($"Polecat_{tenantId}");
        }
    }

    [Fact]
    public async Task a_database_id_the_tenancy_does_not_know_is_refused()
    {
        using var store = StaticTenancy(NewSchema());

        var refused = await Should.ThrowAsync<ArgumentOutOfRangeException>(async () =>
            await ((IEventStore)store).BuildProjectionDaemonAsync(new DatabaseId("nowhere", "nothing")));

        refused.Message.ShouldContain("nowhere");
    }

    /// <summary>
    ///     One database, so nothing to choose between, and the id is what the store reported about itself.
    ///     Matches Marten's <c>DefaultTenancy.FindDatabase</c>.
    /// </summary>
    [Fact]
    public async Task a_single_database_store_answers_with_its_one_database()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionFor(DbA);
            ConfigureBasics(opts, NewSchema());
        });

        var descriptor = store.Database.Describe();

        using var daemon = await ((IEventStore)store).BuildProjectionDaemonAsync(
            new DatabaseId(descriptor.ServerName, descriptor.DatabaseName));

        ((PolecatProjectionDaemon)daemon).Database.ShouldBeSameAs(store.Database);
    }
}

public class TenantDaemonDatabasesFixture : IAsyncLifetime
{
    private static readonly string[] Databases = [tenant_database_daemon_tests.DbA, tenant_database_daemon_tests.DbB];

    public async ValueTask InitializeAsync()
    {
        await DropAsync();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in Databases)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync();
        }

        SqlConnection.ClearAllPools();
    }

    public ValueTask DisposeAsync() => new(DropAsync());

    private static async Task DropAsync()
    {
        SqlConnection.ClearAllPools();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in Databases)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF DB_ID('{db}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{db}];
                END
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
