using JasperFx;
using JasperFx.Events;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     polecat#1048 (port of marten#6011 and marten#6070): <c>QueryAcrossAllDatabasesAsync</c> over a
///     database per tenant.
/// </summary>
public class query_across_all_databases : IAsyncLifetime
{
    private static readonly string[] Databases =
        [ConnectionSource.Scoped("query_all_1"), ConnectionSource.Scoped("query_all_2"), ConnectionSource.Scoped("query_all_3")];

    private static readonly string[] Tenants = ["query_all_1", "query_all_2", "query_all_3"];

    private DocumentStore theStore = null!;

    public async ValueTask InitializeAsync()
    {
        // A clean slate, whatever an earlier run left
        await TenantDatabases.DropAsync(Databases);
        await TenantDatabases.CreateAsync(Databases);

        theStore = DocumentStore.For(opts =>
        {
            opts.DatabaseSchemaName = "query_all";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.MultiTenantedDatabases(x =>
            {
                for (var i = 0; i < Tenants.Length; i++)
                {
                    x.AddTenant(Tenants[i], ConnectionSource.ConnectionStringFor(Databases[i]));
                }
            });
        });

        var tenantNumber = 0;
        foreach (var tenant in Tenants)
        {
            tenantNumber++;
            await using var session = theStore.LightweightSession(tenant);
            for (var i = 0; i < tenantNumber; i++)
            {
                session.Store(new User { FirstName = $"{tenant}-{i}" });
            }

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        theStore?.Dispose();
        await TenantDatabases.DropAsync(Databases);
    }

    [Fact]
    public async Task flattens_the_results_of_every_database()
    {
        var users = await theStore.Advanced.QueryAcrossAllDatabasesAsync(
            async (session, ct) => await session.Query<User>().ToListAsync(ct), 2);

        users.Count.ShouldBe(6);
        users.Select(x => x.FirstName).Distinct().Count().ShouldBe(6);
    }

    [Fact]
    public async Task keeps_each_databases_results_together()
    {
        var users = await theStore.Advanced.QueryAcrossAllDatabasesAsync(
            async (session, ct) => await session.Query<User>().ToListAsync(ct), 3);

        // The names are "<tenant>-<n>": once a tenant's run ends, it must not come back
        var tenants = users.Select(x => x.FirstName[..x.FirstName.LastIndexOf('-')]).ToList();
        var runs = tenants.Where((tenant, i) => i == 0 || tenants[i - 1] != tenant).ToList();
        runs.ShouldBe(runs.Distinct().ToList());
        runs.Count.ShouldBe(3);
    }

    [Fact]
    public async Task hands_each_database_to_the_query_once()
    {
        var seen = await theStore.Advanced.QueryAcrossAllDatabasesAsync<string>(
            (session, database, ct) => Task.FromResult<IReadOnlyList<string>>([database.Identifier]));

        seen.ShouldBe(theStore.Options.Tenancy!.AllDatabases().Select(x => x.Identifier), ignoreOrder: true);
        seen.Count.ShouldBe(3);
    }

    [Fact]
    public async Task tenant_ids_visit_only_the_databases_of_those_tenants()
    {
        var seen = await theStore.Advanced.QueryAcrossAllDatabasesAsync<string>(
            (session, database, ct) => Task.FromResult<IReadOnlyList<string>>([database.Identifier]),
            8, CancellationToken.None, "query_all_2");

        seen.ShouldBe(["Polecat_query_all_2"]);

        var users = await theStore.Advanced.QueryAcrossAllDatabasesAsync(
            async (session, ct) => await session.Query<User>().ToListAsync(ct),
            8, CancellationToken.None, "query_all_2", "query_all_3");

        users.Select(x => x.FirstName).OrderBy(x => x)
            .ShouldBe(["query_all_2-0", "query_all_2-1", "query_all_3-0", "query_all_3-1", "query_all_3-2"]);
    }

    [Fact]
    public async Task never_runs_more_queries_at_once_than_the_limit()
    {
        var running = 0;
        var peak = 0;

        await theStore.Advanced.QueryAcrossAllDatabasesAsync<int>(async (_, ct) =>
        {
            var now = Interlocked.Increment(ref running);
            InterlockedMax(ref peak, now);
            await Task.Delay(100, ct);
            Interlocked.Decrement(ref running);
            return [];
        }, 2);

        peak.ShouldBe(2);
    }

    [Fact]
    public async Task a_failing_database_fails_the_whole_query()
    {
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await theStore.Advanced.QueryAcrossAllDatabasesAsync<int>(
                (_, _) => throw new InvalidOperationException("boom")));
    }

    [Fact]
    public async Task rejects_a_parallelism_below_one()
    {
        await Should.ThrowAsync<ArgumentOutOfRangeException>(async () =>
            await theStore.Advanced.QueryAcrossAllDatabasesAsync<int>(
                (_, _) => Task.FromResult<IReadOnlyList<int>>([]), 0));
    }

    [Fact]
    public async Task an_unknown_tenant_id_is_refused_before_any_database_is_queried()
    {
        var queried = 0;

        var ex = await Should.ThrowAsync<UnknownTenantIdException>(async () =>
            await theStore.Advanced.QueryAcrossAllDatabasesAsync<User>((_, _) =>
            {
                Interlocked.Increment(ref queried);
                return Task.FromResult<IReadOnlyList<User>>([]);
            }, 8, CancellationToken.None, "query_all_1", "query_all_nobody"));

        ex.Message.ShouldContain("query_all_nobody");
        queried.ShouldBe(0);
    }

    [Fact]
    public async Task no_tenant_ids_is_an_empty_result()
    {
        var users = await theStore.Advanced.QueryAcrossAllDatabasesAsync<User>(
            (_, _) => throw new InvalidOperationException("never runs"), 8, CancellationToken.None, []);

        users.ShouldBeEmpty();
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref target)))
        {
            if (Interlocked.CompareExchange(ref target, value, current) == current) break;
        }
    }
}

/// <summary>
///     Two tenants in one database and one in another, every document conjoined. A query across all databases
///     reads every tenant, and reads the shared database once; the tenant id overload visits only the
///     databases that hold those tenants.
/// </summary>
public class query_across_all_conjoined_databases : IAsyncLifetime
{
    private static readonly string[] Databases =
        [ConnectionSource.Scoped("query_conj_1"), ConnectionSource.Scoped("query_conj_2")];

    private DocumentStore theStore = null!;

    public async ValueTask InitializeAsync()
    {
        // A clean slate, whatever an earlier run left
        await TenantDatabases.DropAsync(Databases);
        await TenantDatabases.CreateAsync(Databases);

        theStore = DocumentStore.For(opts =>
        {
            opts.DatabaseSchemaName = "query_conj";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.MultiTenantedDatabases(x =>
            {
                x.AddTenant("red", ConnectionSource.ConnectionStringFor(Databases[0]));
                x.AddTenant("blue", ConnectionSource.ConnectionStringFor(Databases[0]));
                x.AddTenant("green", ConnectionSource.ConnectionStringFor(Databases[1]));
            });
            opts.Policies.AllDocumentsAreMultiTenanted();
        });

        foreach (var tenant in new[] { "red", "blue", "green" })
        {
            await using var session = theStore.LightweightSession(tenant);
            session.Store(new User { FirstName = tenant });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        theStore?.Dispose();
        await TenantDatabases.DropAsync(Databases);
    }

    [Fact]
    public async Task a_plain_query_reads_every_tenant_in_every_database_once()
    {
        var users = await theStore.Advanced.QueryAcrossAllDatabasesAsync(
            async (session, ct) => await session.Query<User>().ToListAsync(ct));

        users.Select(x => x.FirstName).OrderBy(x => x).ShouldBe(["blue", "green", "red"]);
    }

    [Fact]
    public async Task tenant_ids_read_only_those_tenants_in_only_their_databases()
    {
        var visited = new List<string>();
        var users = await theStore.Advanced.QueryAcrossAllDatabasesAsync(
            async (session, database, ct) =>
            {
                lock (visited) visited.Add(database.ConnectionString);
                return await session.Query<User>().ToListAsync(ct);
            },
            8, CancellationToken.None, "blue");

        visited.Count.ShouldBe(1);
        visited[0].ShouldContain(Databases[0]);
        users.Select(x => x.FirstName).ShouldBe(["blue"]);

        // Two tenants of one database are read in one visit to it
        visited.Clear();
        users = await theStore.Advanced.QueryAcrossAllDatabasesAsync(
            async (session, database, ct) =>
            {
                lock (visited) visited.Add(database.ConnectionString);
                return await session.Query<User>().ToListAsync(ct);
            },
            8, CancellationToken.None, "red", "blue", "green");

        visited.Count.ShouldBe(2);
        users.Select(x => x.FirstName).OrderBy(x => x).ShouldBe(["blue", "green", "red"]);
    }
}

public record TenantGreeted(string TenantId);

/// <summary>
///     Several tenants in one database, documents and events conjoined. A query across all databases reads every
///     tenant through LINQ, Load and event queries alike; the tenant id overload limits the tenants it reads.
///     Ordinary sessions keep reading their own tenant.
/// </summary>
public class query_across_all_tenants_of_one_database : OneOffConfigurationsContext
{
    private static readonly string[] Tenants = ["q_alpha", "q_beta", "q_gamma", "q_delta"];

    private readonly Dictionary<string, Guid> _userIds = new();
    private readonly Dictionary<string, Guid> _streamIds = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public override async ValueTask InitializeAsync()
    {
        ConfigureStore(opts =>
        {
            opts.Policies.AllDocumentsAreMultiTenanted();
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.AddEventType<TenantGreeted>();
        });

        await base.InitializeAsync();

        foreach (var tenantId in Tenants)
        {
            await using var session = theStore.LightweightSession(tenantId);
            var user = new User { FirstName = tenantId };
            session.Store(user);
            _userIds[tenantId] = user.Id;
            _streamIds[tenantId] = session.Events.StartStream(new TenantGreeted(tenantId)).Id;
            await session.SaveChangesAsync(Ct);
        }
    }

    private Task<IReadOnlyList<T>> queryAll<T>(Func<IQuerySession, CancellationToken, Task<IReadOnlyList<T>>> query)
        => theStore.Advanced.QueryAcrossAllDatabasesAsync(query);

    private Task<IReadOnlyList<T>> queryTenants<T>(Func<IQuerySession, CancellationToken, Task<IReadOnlyList<T>>> query,
        params string[] tenantIds)
        => theStore.Advanced.QueryAcrossAllDatabasesAsync(query, 8, CancellationToken.None, tenantIds);

    [Fact]
    public async Task a_plain_query_reads_every_tenant()
    {
        var users = await queryAll(async (session, ct) => await session.Query<User>().ToListAsync(ct));

        users.Select(x => x.FirstName).OrderBy(x => x).ShouldBe(["q_alpha", "q_beta", "q_delta", "q_gamma"]);
    }

    [Fact]
    public async Task any_tenant_finds_every_tenants_rows()
    {
        var users = await queryAll(async (session, ct) => await session.Query<User>().AnyTenant().ToListAsync(ct));

        users.Select(x => x.FirstName).OrderBy(x => x).ShouldBe(["q_alpha", "q_beta", "q_delta", "q_gamma"]);
    }

    [Fact]
    public async Task tenant_is_one_of_in_the_query_still_applies()
    {
        var users = await queryAll(async (session, ct) =>
            await session.Query<User>().TenantIsOneOf("q_beta", "q_delta").ToListAsync(ct));

        users.Select(x => x.FirstName).OrderBy(x => x).ShouldBe(["q_beta", "q_delta"]);
    }

    [Fact]
    public async Task load_by_id_reads_every_tenant()
    {
        var one = await queryAll<User>(async (session, ct) =>
            await session.LoadAsync<User>(_userIds["q_gamma"], ct) is { } user ? [user] : []);

        one.Single().FirstName.ShouldBe("q_gamma");

        var many = await queryAll(async (session, ct) => await session.LoadManyAsync<User>(_userIds.Values, ct));

        many.Select(x => x.FirstName).OrderBy(x => x).ShouldBe(["q_alpha", "q_beta", "q_delta", "q_gamma"]);

        var json = await queryAll<string>(async (session, ct) =>
            await session.LoadJsonAsync<User>(_userIds["q_delta"], ct) is { } found ? [found] : []);

        json.Single().ShouldContain("q_delta");

        var exists = await queryAll<bool>(async (session, ct) =>
            [await session.CheckExistsAsync<User>(_userIds["q_beta"], ct)]);

        exists.ShouldBe([true]);
    }

    [Fact]
    public async Task event_queries_read_every_tenant()
    {
        var greeted = await queryAll(async (session, ct) =>
            await session.Events.QueryRawEventDataOnly<TenantGreeted>().ToListAsync(ct));

        greeted.Select(x => x.TenantId).OrderBy(x => x).ShouldBe(["q_alpha", "q_beta", "q_delta", "q_gamma"]);

        var events = await queryAll(async (session, ct) => await session.Events.QueryAllRawEvents().ToListAsync(ct));

        events.Select(x => x.TenantId).OrderBy(x => x).ShouldBe(["q_alpha", "q_beta", "q_delta", "q_gamma"]);

        var stream = await queryAll(async (session, ct) =>
            await session.Events.FetchStreamAsync(_streamIds["q_delta"], token: ct));

        stream.Single().Data.ShouldBe(new TenantGreeted("q_delta"));
    }

    [Fact]
    public async Task tenant_ids_read_only_those_tenants()
    {
        var users = await queryTenants(async (session, ct) => await session.Query<User>().ToListAsync(ct),
            "q_alpha", "q_gamma");

        users.Select(x => x.FirstName).OrderBy(x => x).ShouldBe(["q_alpha", "q_gamma"]);

        var loaded = await queryTenants(async (session, ct) =>
            await session.LoadManyAsync<User>(_userIds.Values, ct), "q_alpha", "q_gamma");

        loaded.Select(x => x.FirstName).OrderBy(x => x).ShouldBe(["q_alpha", "q_gamma"]);

        var beta = await queryTenants<User>(async (session, ct) =>
            await session.LoadAsync<User>(_userIds["q_beta"], ct) is { } user ? [user] : [], "q_alpha", "q_gamma");

        beta.ShouldBeEmpty();

        var greeted = await queryTenants(async (session, ct) =>
            await session.Events.QueryRawEventDataOnly<TenantGreeted>().ToListAsync(ct), "q_alpha", "q_gamma");

        greeted.Select(x => x.TenantId).OrderBy(x => x).ShouldBe(["q_alpha", "q_gamma"]);

        var betaStream = await queryTenants(async (session, ct) =>
            await session.Events.FetchStreamAsync(_streamIds["q_beta"], token: ct), "q_alpha", "q_gamma");

        betaStream.ShouldBeEmpty();
    }

    [Fact]
    public async Task ordinary_sessions_still_read_their_own_tenant()
    {
        await queryAll(async (session, ct) => await session.Query<User>().ToListAsync(ct));

        await using var alpha = theStore.QuerySession("q_alpha");
        (await alpha.Query<User>().ToListAsync(Ct)).Select(x => x.FirstName).ShouldBe(["q_alpha"]);
        (await alpha.LoadAsync<User>(_userIds["q_delta"], Ct)).ShouldBeNull();
        (await alpha.Events.FetchStreamAsync(_streamIds["q_delta"], token: Ct)).ShouldBeEmpty();
    }
}

/// <summary>Creates and drops the databases a database-per-tenant test uses.</summary>
internal static class TenantDatabases
{
    public static async Task CreateAsync(string[] databases)
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in databases)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public static async Task DropAsync(string[] databases)
    {
        SqlConnection.ClearAllPools();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in databases)
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
