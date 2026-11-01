using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.TestUtils;

namespace Polecat.Tests.Schema;

public record FirstUseTick;

public class FirstUseDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}

public class FirstUseOrder { public Guid Id { get; set; } }
public class FirstUseInvoice { public Guid Id { get; set; } }
public class FirstUseShipment { public Guid Id { get; set; } }
public class FirstUseCustomer { public Guid Id { get; set; } }
public class FirstUseProduct { public Guid Id { get; set; } }
public class FirstUseSupplier { public Guid Id { get; set; } }

/// <summary>
///     #868: everything that provisions a schema on first use has to serialize on one lock, or two of
///     them meet on a fresh schema and the loser fails with
///     <c>There is already an object named 'fk_pc_events_stream_id' in the database</c>. The event store
///     ensure had a <c>SemaphoreSlim</c>, which covers one ensurer in one process, and the daemon's
///     <c>EnsureStorageExistsAsync</c> took nothing at all. That reached CI on
///     <c>ancillary_store_coordinator_tests.a_marked_coordinator_runs_its_own_stores_projections</c>, whose
///     session makes its first write while the coordinator is starting.
/// </summary>
/// <remarks>
///     Each round is a brand new schema, because a race on a schema only happens once: whoever comes second
///     to a finished schema has nothing to create. A single round can pass by luck, so the assertion is on
///     the whole run, and the message says how many rounds failed.
/// </remarks>
public class first_use_schema_race_tests
{
    private const int Rounds = 20;

    /// <summary>
    ///     The document races are the least reliable without the lock: measured as low as one round in twenty,
    ///     where the event store races fail better than one in two, so they need more rounds to fail every
    ///     run rather than most of them.
    /// </summary>
    private const int DocumentRounds = 40;

    /// <summary>
    ///     Far fewer, because the daemon's ensure applies every object over seven document tables, which is
    ///     about eight seconds a round on a CI runner, and 40 of those went past the five minute stall
    ///     report. Without the lock this fails about one round in fifteen, so a single run can miss it. It
    ///     earns its place across many runs, and <see cref="two_independent_stores_making_their_first_document_write" />
    ///     is the one that fails every time.
    /// </summary>
    private const int DocumentAndDaemonRounds = 12;

    private static string NewSchema() => "fsr_" + Guid.NewGuid().ToString("N")[..10];

    private static DocumentStore BuildStore(string schema, bool manyDocumentTypes = false) => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Schema.For<FirstUseDoc>();

        if (!manyDocumentTypes) return;

        opts.Schema.For<FirstUseOrder>();
        opts.Schema.For<FirstUseInvoice>();
        opts.Schema.For<FirstUseShipment>();
        opts.Schema.For<FirstUseCustomer>();
        opts.Schema.For<FirstUseProduct>();
        opts.Schema.For<FirstUseSupplier>();
    });

    private static async Task FirstAppendAsync(IDocumentStore store, CancellationToken token)
    {
        await using var session = store.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new FirstUseTick());
        await session.SaveChangesAsync(token);
    }

    private static async Task FirstDocumentWriteAsync(IDocumentStore store, CancellationToken token)
    {
        await using var session = store.LightweightSession();
        session.Store(new FirstUseDoc { Id = Guid.NewGuid(), Name = "first" });
        await session.SaveChangesAsync(token);
    }

    private static async Task FirstWriteToEveryDocumentTableAsync(IDocumentStore store, CancellationToken token)
    {
        await using var session = store.LightweightSession();
        session.Store(new FirstUseDoc { Id = Guid.NewGuid() });
        session.Store(new FirstUseOrder { Id = Guid.NewGuid() });
        session.Store(new FirstUseInvoice { Id = Guid.NewGuid() });
        session.Store(new FirstUseShipment { Id = Guid.NewGuid() });
        session.Store(new FirstUseCustomer { Id = Guid.NewGuid() });
        session.Store(new FirstUseProduct { Id = Guid.NewGuid() });
        session.Store(new FirstUseSupplier { Id = Guid.NewGuid() });
        await session.SaveChangesAsync(token);
    }

    /// <summary>
    ///     Releases the contenders together and reports what they threw, rather than the first of it.
    /// </summary>
    private static async Task<IReadOnlyList<Exception>> ReleaseTogetherAsync(params Func<Task>[] contenders)
    {
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var running = contenders
            .Select(contender => Task.Run(async () =>
            {
                await start.Task;
                await contender();
            }))
            .ToArray();

        start.SetResult();

        await Task.WhenAll(running.Select(x => x.ContinueWith(_ => { }, TaskScheduler.Default)));

        return running
            .Where(x => x.IsFaulted)
            .SelectMany(x => x.Exception!.InnerExceptions)
            .ToList();
    }

    private static void AssertNoRoundFailed(List<Exception> failures, int rounds = Rounds)
    {
        var message = $"{failures.Count} failure(s) over {rounds} fresh schemas: "
                      + string.Join(" | ", failures
                          .GroupBy(x => $"{x.GetType().Name}: {x.Message}")
                          .Select(x => $"{x.Count()}x {x.Key}"))
                      + Environment.NewLine
                      + string.Join(Environment.NewLine, failures
                          .GroupBy(x => x.GetType())
                          .Select(x => x.First().ToString()));

        failures.Count.ShouldBe(0, message);
    }

    private static async Task DropAsync(string schema, DocumentStore store, CancellationToken token,
        string? connectionString = null)
    {
        await store.Advanced.CompletelyRemoveAllAsync(token);

        await using var conn = new SqlConnection(connectionString ?? ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DROP SCHEMA IF EXISTS [{schema}];";
        await cmd.ExecuteNonQueryAsync(token);
    }

    [Fact]
    public async Task a_sessions_first_append_and_the_daemons_ensure_on_one_store()
    {
        var token = TestContext.Current.CancellationToken;
        var failures = new List<Exception>();

        for (var round = 0; round < Rounds; round++)
        {
            var schema = NewSchema();
            using var store = BuildStore(schema);

            failures.AddRange(await ReleaseTogetherAsync(
                () => store.Database.EnsureStorageExistsAsync(typeof(IEvent), token),
                () => FirstAppendAsync(store, token)));

            await DropAsync(schema, store, token);
        }

        AssertNoRoundFailed(failures);
    }

    /// <summary>
    ///     Two processes, as far as anything in-memory can tell: the stores share neither an ensurer nor a
    ///     semaphore, so only something held on the server can serialize them.
    /// </summary>
    [Fact]
    public async Task two_independent_stores_making_their_first_append()
    {
        var token = TestContext.Current.CancellationToken;
        var failures = new List<Exception>();

        for (var round = 0; round < Rounds; round++)
        {
            var schema = NewSchema();
            using var first = BuildStore(schema);
            using var second = BuildStore(schema);

            failures.AddRange(await ReleaseTogetherAsync(
                () => FirstAppendAsync(first, token),
                () => FirstAppendAsync(second, token)));

            await DropAsync(schema, first, token);
        }

        AssertNoRoundFailed(failures);
    }

    [Fact]
    public async Task a_sessions_first_append_and_another_stores_daemon_ensure()
    {
        var token = TestContext.Current.CancellationToken;
        var failures = new List<Exception>();

        for (var round = 0; round < Rounds; round++)
        {
            var schema = NewSchema();
            using var hostingTheDaemon = BuildStore(schema);
            using var hostingTheSession = BuildStore(schema);

            failures.AddRange(await ReleaseTogetherAsync(
                () => hostingTheDaemon.Database.EnsureStorageExistsAsync(typeof(IEvent), token),
                () => FirstAppendAsync(hostingTheSession, token)));

            await DropAsync(schema, hostingTheDaemon, token);
        }

        AssertNoRoundFailed(failures);
    }

    [Fact]
    public async Task two_independent_stores_making_their_first_document_write()
    {
        var token = TestContext.Current.CancellationToken;
        var failures = new List<Exception>();

        for (var round = 0; round < DocumentRounds; round++)
        {
            var schema = NewSchema();
            using var first = BuildStore(schema);
            using var second = BuildStore(schema);

            failures.AddRange(await ReleaseTogetherAsync(
                () => FirstDocumentWriteAsync(first, token),
                () => FirstDocumentWriteAsync(second, token)));

            await DropAsync(schema, first, token);
        }

        AssertNoRoundFailed(failures, DocumentRounds);
    }

    /// <summary>
    ///     The document half of the same exposure: the daemon's ensure applies EVERY configured object, so
    ///     it creates the document table as well, and the session's own ensure for that table is not
    ///     protected by anything the event store path takes.
    /// </summary>
    /// <remarks>
    ///     Several tables on purpose. Weasel guards a table's create, so these two only collide in the
    ///     instant both are creating the SAME table, which a single table almost never hits. A session
    ///     working through seven of them, while the daemon does the same, overlaps by construction.
    /// </remarks>
    [Fact]
    public async Task a_sessions_first_document_write_and_the_daemons_ensure()
    {
        var token = TestContext.Current.CancellationToken;
        var failures = new List<Exception>();

        for (var round = 0; round < DocumentAndDaemonRounds; round++)
        {
            var schema = NewSchema();
            using var store = BuildStore(schema, manyDocumentTypes: true);

            failures.AddRange(await ReleaseTogetherAsync(
                () => store.Database.EnsureStorageExistsAsync(typeof(IEvent), token),
                () => FirstWriteToEveryDocumentTableAsync(store, token)));

            await DropAsync(schema, store, token);
        }

        AssertNoRoundFailed(failures, DocumentAndDaemonRounds);
    }

    /// <summary>
    ///     The shape the issue was filed against: the daemon provisioning a tenant's database while a session
    ///     writes to that tenant. The lock has to work through the per-tenant ensurer and the per-tenant
    ///     <c>PolecatDatabase</c>, neither of which is the store's own.
    /// </summary>
    [Fact]
    public async Task a_tenants_first_append_and_the_daemons_ensure_on_that_database()
    {
        var token = TestContext.Current.CancellationToken;

        var database = ConnectionSource.Scoped("fsr_tenant");
        var connectionString = ConnectionSource.ConnectionStringFor(database);

        await using (var master = new SqlConnection(ConnectionSource.MasterConnectionString))
        {
            await master.OpenAsync(token);
            await using var cmd = master.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{database}') IS NULL CREATE DATABASE [{database}];";
            await cmd.ExecuteNonQueryAsync(token);
        }

        var failures = new List<Exception>();

        try
        {
            for (var round = 0; round < Rounds; round++)
            {
                var schema = NewSchema();
                using var store = DocumentStore.For(opts =>
                {
                    opts.ConnectionString = connectionString;
                    opts.DatabaseSchemaName = schema;
                    opts.AutoCreateSchemaObjects = AutoCreate.All;
                    opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
                    opts.MultiTenantedDatabases(tenancy => tenancy.AddTenant("fsr", connectionString));
                });

                var tenantDatabase = store.Options.Tenancy!.AllDatabases().Single();

                failures.AddRange(await ReleaseTogetherAsync(
                    () => tenantDatabase.EnsureStorageExistsAsync(typeof(IEvent), token),
                    async () =>
                    {
                        await using var session = store.LightweightSession("fsr");
                        session.Events.StartStream(Guid.NewGuid(), new FirstUseTick());
                        await session.SaveChangesAsync(token);
                    }));

                await DropAsync(schema, store, token, connectionString);
            }
        }
        finally
        {
            SqlConnection.ClearAllPools();

            await using var master = new SqlConnection(ConnectionSource.MasterConnectionString);
            await master.OpenAsync(CancellationToken.None);
            await using var cmd = master.CreateCommand();
            cmd.CommandText =
                $"IF DB_ID('{database}') IS NOT NULL BEGIN "
                + $"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                + $"DROP DATABASE [{database}]; END";
            await cmd.ExecuteNonQueryAsync(CancellationToken.None);
        }

        AssertNoRoundFailed(failures);
    }
}
