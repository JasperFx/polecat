using System.Diagnostics;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Projections;
using Polecat.TestUtils;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Tests.Schema;

public class FirstUseTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class FirstUseTallyProjection : SingleStreamProjection<FirstUseTally, Guid>
{
    public override FirstUseTally? Evolve(FirstUseTally? snapshot, Guid id, IEvent e)
    {
        snapshot ??= new FirstUseTally { Id = id };
        snapshot.Count++;
        return snapshot;
    }
}

/// <summary>
///     #868: what the lock has to guarantee, one guarantee at a time, by holding it from outside.
///     <see cref="first_use_schema_race_tests" /> shows the collisions are gone; these show why, and pin
///     the constraints that a lock could otherwise break on its way to fixing them: the same resource on
///     every path, a loser that waits and then carries on, nothing asked for once a process knows, and
///     no wait at all inside an open transaction.
/// </summary>
public class first_use_schema_lock_tests
{
    private static string NewSchema() => "fsl_" + Guid.NewGuid().ToString("N")[..10];

    private static string LockFor(string schema) => $"polecat:migrate:{schema}";

    private static DocumentStore BuildStore(string schema, TimeSpan? lockTimeout = null,
        AutoCreate autoCreate = AutoCreate.All, Action<StoreOptions>? configure = null) => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = schema;
        opts.AutoCreateSchemaObjects = autoCreate;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.StartupMigrationLockTimeout = lockTimeout ?? TimeSpan.FromSeconds(30);
        opts.Schema.For<FirstUseDoc>();
        configure?.Invoke(opts);
    });

    /// <summary>
    ///     Holds the migration lock from outside, the way another process would, on a connection of its own.
    /// </summary>
    private sealed class LockHolder : IAsyncDisposable
    {
        private readonly string _resource;
        private bool _held;

        private LockHolder(SqlConnection connection, string resource)
        {
            Connection = connection;
            _resource = resource;
        }

        public SqlConnection Connection { get; }

        public static async Task<LockHolder> TakeAsync(string schema, CancellationToken token)
        {
            var connection = new SqlConnection(ConnectionSource.ConnectionString);
            await connection.OpenAsync(token);

            var holder = new LockHolder(connection, LockFor(schema));
            (await connection.TryGetGlobalLock(holder._resource, cancellation: token)).ShouldBeTrue();
            holder._held = true;

            return holder;
        }

        public async Task ReleaseAsync()
        {
            if (!_held) return;
            _held = false;
            await Connection.ReleaseGlobalLock(_resource, CancellationToken.None);
        }

        public async ValueTask DisposeAsync()
        {
            await ReleaseAsync();
            await Connection.DisposeAsync();
        }
    }

    /// <summary>
    ///     Asks for the lock with no wait at all, from a third party. A lock left behind by a failure shows
    ///     up here as a refusal.
    /// </summary>
    /// <remarks>
    ///     ⚠️ A connection string of its own, which is a connection pool of its own, on purpose. A leaked lock
    ///     sits on a connection that has gone back to the pool, and opening from that same pool hands that
    ///     very connection back, resets it, and frees the lock, so this would report the lock free BECAUSE it
    ///     had leaked. Another process is what is being modelled here, and it would have its own pool.
    /// </remarks>
    private static async Task<bool> LockIsFreeAsync(string schema, CancellationToken token)
    {
        var separatePool = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            ApplicationName = "first_use_schema_lock_probe"
        };

        await using var probe = new SqlConnection(separatePool.ConnectionString);
        await probe.OpenAsync(token);

        var free = await probe.TryGetGlobalLock(LockFor(schema), cancellation: token, lockTimeoutMs: 0);
        if (free) await probe.ReleaseGlobalLock(LockFor(schema), token);

        return free;
    }

    private static async Task<bool> TableExistsAsync(string schema, string table, CancellationToken token)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT OBJECT_ID('[{schema}].[{table}]', 'U')";
        return await cmd.ExecuteScalarAsync(token) is not (null or DBNull);
    }

    /// <summary>
    ///     Whether anything in this database is queued for an application lock right now. Tests in this
    ///     assembly run one collection at a time, and parallel workers get a catalog each, so the only
    ///     thing that can be queued here is what the test started.
    /// </summary>
    private static async Task<bool> SomethingIsQueuedForTheLockAsync(SqlConnection conn, CancellationToken token)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          SELECT COUNT(*) FROM sys.dm_tran_locks
                          WHERE resource_type = 'APPLICATION'
                            AND resource_database_id = DB_ID()
                            AND request_status = 'WAIT'
                          """;
        return (int)(await cmd.ExecuteScalarAsync(token))! > 0;
    }

    /// <summary>
    ///     The waiter has to be seen queued for the lock, and only finish once the lock is given back. Then
    ///     it has to succeed, which is the half that matters: a loser finds the schema the winner left and
    ///     carries on rather than failing.
    /// </summary>
    /// <remarks>
    ///     Asks the server rather than checking that the work is still running after a while. An apply on a
    ///     fresh schema takes anything from 50ms to 4 seconds with nothing contending for it, so "still
    ///     running after 1.5 seconds" passed by luck against code with no lock at all.
    /// </remarks>
    private static async Task AssertWaitsThenCarriesOnAsync(LockHolder holder, Func<Task> firstUse)
    {
        var token = TestContext.Current.CancellationToken;
        var running = Task.Run(firstUse, token);

        var patience = Stopwatch.StartNew();
        while (!await SomethingIsQueuedForTheLockAsync(holder.Connection, token))
        {
            running.IsCompleted.ShouldBeFalse("this ran while another process held the migration lock");
            patience.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(20), "nothing ever asked for the migration lock");

            await Task.Delay(25, token);
        }

        running.IsCompleted.ShouldBeFalse("this finished while it was queued for the migration lock");

        await holder.ReleaseAsync();
        await running;
    }

    // -------------------------------------------------------------------------------------
    // Every path asks for the same lock, and a loser waits and then carries on
    // -------------------------------------------------------------------------------------

    [Fact]
    public async Task a_sessions_first_append_waits_for_the_lock_and_then_succeeds()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        using var store = BuildStore(schema);
        await using var holder = await LockHolder.TakeAsync(schema, token);

        await AssertWaitsThenCarriesOnAsync(holder, async () =>
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new FirstUseTick());
            await session.SaveChangesAsync(token);
        });

        (await TableExistsAsync(schema, "pc_events", token)).ShouldBeTrue();
    }

    [Fact]
    public async Task a_sessions_first_read_of_the_event_store_waits_for_the_lock_and_then_succeeds()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        using var store = BuildStore(schema);
        await using var holder = await LockHolder.TakeAsync(schema, token);

        await AssertWaitsThenCarriesOnAsync(holder, async () =>
        {
            await using var session = store.QuerySession();
            (await session.Events.FetchStreamStateAsync(Guid.NewGuid(), token)).ShouldBeNull();
        });
    }

    [Fact]
    public async Task the_daemons_ensure_waits_for_the_lock_and_then_succeeds()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        using var store = BuildStore(schema);
        await using var holder = await LockHolder.TakeAsync(schema, token);

        await AssertWaitsThenCarriesOnAsync(holder,
            () => store.Database.EnsureStorageExistsAsync(typeof(IEvent), token));

        (await TableExistsAsync(schema, "pc_events", token)).ShouldBeTrue();
    }

    [Fact]
    public async Task a_first_document_write_waits_for_the_lock_and_then_succeeds()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        using var store = BuildStore(schema);
        await using var holder = await LockHolder.TakeAsync(schema, token);

        await AssertWaitsThenCarriesOnAsync(holder, async () =>
        {
            await using var session = store.LightweightSession();
            session.Store(new FirstUseDoc { Id = Guid.NewGuid() });
            await session.SaveChangesAsync(token);
        });

        (await TableExistsAsync(schema, "pc_doc_firstusedoc", token)).ShouldBeTrue();
    }

    [Fact]
    public async Task an_inline_projections_table_is_ensured_before_the_transaction_so_it_can_wait()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();

        // The event store is already there, so the only thing a save has left to ensure is the projected
        // table, which used to happen with the appends already sitting in an open transaction.
        using (var existing = BuildStore(schema))
        {
            await using var session = existing.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new FirstUseTick());
            await session.SaveChangesAsync(token);
        }

        using var store = BuildStore(schema,
            configure: opts => opts.Projections.Add<FirstUseTallyProjection>(ProjectionLifecycle.Inline));

        await using (var warmUp = store.LightweightSession())
        {
            await warmUp.Events.FetchStreamStateAsync(Guid.NewGuid(), token);
        }

        await using var holder = await LockHolder.TakeAsync(schema, token);

        var id = Guid.NewGuid();
        await AssertWaitsThenCarriesOnAsync(holder, async () =>
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(id, new FirstUseTick());
            await session.SaveChangesAsync(token);
        });

        await using var query = store.QuerySession();
        (await query.LoadAsync<FirstUseTally>(id, token))!.Count.ShouldBe(1);
    }

    /// <summary>
    ///     #664's lock moved out of the activator. A startup migration has to keep asking for the resource
    ///     the first-use paths ask for, or the two would run straight past each other.
    /// </summary>
    [Fact]
    public async Task a_startup_migration_asks_for_the_same_lock()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        using var store = BuildStore(schema);
        store.Options.ShouldApplyChangesOnStartup = true;
        await using var holder = await LockHolder.TakeAsync(schema, token);

        await AssertWaitsThenCarriesOnAsync(holder, () => new PolecatActivator(store).StartAsync(token));

        (await TableExistsAsync(schema, "pc_events", token)).ShouldBeTrue();
    }

    // -------------------------------------------------------------------------------------
    // Losing the lock
    // -------------------------------------------------------------------------------------

    [Fact]
    public async Task a_session_that_loses_the_lock_says_why_and_the_next_use_tries_again()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        using var store = BuildStore(schema, lockTimeout: TimeSpan.FromMilliseconds(300));
        await using var holder = await LockHolder.TakeAsync(schema, token);

        async Task FirstAppendAsync()
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new FirstUseTick());
            await session.SaveChangesAsync(token);
        }

        var failure = await Should.ThrowAsync<InvalidOperationException>(FirstAppendAsync);
        failure.Message.ShouldContain(LockFor(schema));
        failure.Message.ShouldContain(nameof(StoreOptions.StartupMigrationLockTimeout));

        await holder.ReleaseAsync();

        await FirstAppendAsync();
    }

    /// <summary>
    ///     The lock request is an ordinary command, and on default settings the 30 second lock timeout and
    ///     the 30 second command timeout expire together, with the client winning. A raw "Execution Timeout
    ///     Expired" names nothing, and it bypasses <c>ResourceMigrationFailureMode</c> altogether, so
    ///     <c>ContinueOnFailures</c> never engaged. A one second command timeout gets to that same place
    ///     without taking 30 seconds over it.
    /// </summary>
    [Fact]
    public async Task a_lock_wait_cut_short_by_the_command_timeout_is_still_a_lost_lock()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        var shortCommandTimeout = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            CommandTimeout = 1
        };

        using var store = BuildStore(schema, configure: opts => opts.ConnectionString = shortCommandTimeout.ConnectionString);
        await using var holder = await LockHolder.TakeAsync(schema, token);

        var failure = await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new FirstUseTick());
            await session.SaveChangesAsync(token);
        });
        failure.Message.ShouldContain(LockFor(schema));

        store.Database.ResourceMigrationFailureMode = ResourceMigrationFailureMode.ContinueOnFailures;
        await store.Database.EnsureStorageExistsAsync(typeof(IEvent), token);
    }

    /// <summary>
    ///     Under <c>ContinueOnFailures</c> the apply returns as though nothing needed doing, having looked at
    ///     nothing. Remembering that as "ensured" would leave this process never creating the schema.
    /// </summary>
    [Fact]
    public async Task a_daemon_that_continues_past_a_lost_lock_does_not_remember_the_schema_as_ensured()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        using var store = BuildStore(schema, lockTimeout: TimeSpan.FromMilliseconds(300));
        store.Database.ResourceMigrationFailureMode = ResourceMigrationFailureMode.ContinueOnFailures;
        await using var holder = await LockHolder.TakeAsync(schema, token);

        await store.Database.EnsureStorageExistsAsync(typeof(IEvent), token);
        (await TableExistsAsync(schema, "pc_events", token)).ShouldBeFalse();

        await holder.ReleaseAsync();

        await store.Database.EnsureStorageExistsAsync(typeof(IEvent), token);
        (await TableExistsAsync(schema, "pc_events", token)).ShouldBeTrue();
    }

    /// <summary>
    ///     The lock is held by the session on the connection, and a connection handed back to the pool keeps
    ///     its session, so a failure that skipped the release would lock every other replica out of
    ///     migrating this schema until the pool happened to reuse that connection.
    /// </summary>
    [Fact]
    public async Task a_failed_ensure_gives_the_lock_back()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        using var store = BuildStore(schema);

        // pc_streams as a view: the catalog says the table is missing, and the create cannot succeed.
        await using (var conn = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(token);
            await using var createSchema = conn.CreateCommand();
            createSchema.CommandText = $"CREATE SCHEMA [{schema}];";
            await createSchema.ExecuteNonQueryAsync(token);

            await using var createView = conn.CreateCommand();
            createView.CommandText = $"CREATE VIEW [{schema}].[pc_streams] AS SELECT 1 AS id;";
            await createView.ExecuteNonQueryAsync(token);
        }

        await Should.ThrowAsync<Exception>(async () =>
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new FirstUseTick());
            await session.SaveChangesAsync(token);
        });

        (await LockIsFreeAsync(schema, token)).ShouldBeTrue();
    }

    // -------------------------------------------------------------------------------------
    // Nothing is asked for once a process knows, and nothing at all under AutoCreate.None
    // -------------------------------------------------------------------------------------

    /// <summary>
    ///     Each of these would time out against the held lock, in 300ms, if it asked for it. The high water
    ///     health check calls <c>EnsureStorageExistsAsync</c> on every probe, so the hot path staying
    ///     lock-free is not hypothetical.
    /// </summary>
    [Fact]
    public async Task a_process_that_knows_the_schema_is_ensured_asks_for_no_lock()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        using var store = BuildStore(schema, lockTimeout: TimeSpan.FromMilliseconds(300));

        await store.Database.EnsureStorageExistsAsync(typeof(IEvent), token);

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new FirstUseTick());
            session.Store(new FirstUseDoc { Id = Guid.NewGuid() });
            await session.SaveChangesAsync(token);
        }

        await using var holder = await LockHolder.TakeAsync(schema, token);

        await store.Database.EnsureStorageExistsAsync(typeof(IEvent), token);

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new FirstUseTick());
            session.Store(new FirstUseDoc { Id = Guid.NewGuid() });
            await session.SaveChangesAsync(token);
        }
    }

    /// <summary>
    ///     The memo must not outlive the schema it remembers: a daemon built after a wipe has always
    ///     rebuilt the tables, and the suite leans on that.
    /// </summary>
    [Fact]
    public async Task removing_everything_forgets_that_the_schema_was_ensured()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        using var store = BuildStore(schema);

        await store.Database.EnsureStorageExistsAsync(typeof(IEvent), token);
        await store.Advanced.CompletelyRemoveAllAsync(token);
        (await TableExistsAsync(schema, "pc_events", token)).ShouldBeFalse();

        await store.Database.EnsureStorageExistsAsync(typeof(IEvent), token);

        (await TableExistsAsync(schema, "pc_events", token)).ShouldBeTrue();
    }

    [Fact]
    public async Task auto_create_none_asks_for_no_lock_on_any_path()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();

        using (var provisioning = BuildStore(schema))
        {
            await provisioning.Database.EnsureStorageExistsAsync(typeof(IEvent), token);
        }

        using var store = BuildStore(schema, lockTimeout: TimeSpan.FromMilliseconds(300),
            autoCreate: AutoCreate.None);

        await using var holder = await LockHolder.TakeAsync(schema, token);

        await store.Database.EnsureStorageExistsAsync(typeof(IEvent), token);

        await using var session = store.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new FirstUseTick());
        session.Store(new FirstUseDoc { Id = Guid.NewGuid() });
        await session.SaveChangesAsync(token);
    }

    // -------------------------------------------------------------------------------------
    // Never wait while holding a transaction
    // -------------------------------------------------------------------------------------

    /// <summary>
    ///     The cycle the lock could have introduced, built for real. The session takes a stream lock in a
    ///     transaction, and its first save then ensures the event store on a SECOND connection. Meanwhile a
    ///     migration holds the lock, and its ALTER needs a schema lock on the table the transaction has
    ///     touched. Had the ensure waited for the lock, the transaction could never finish, the ALTER could
    ///     never finish, and the lock would never come free, and because the ensure is on another connection
    ///     SQL Server sees no cycle and picks no victim. It only ends when the lock times out and fails the
    ///     save.
    /// </summary>
    [Fact]
    public async Task an_exclusive_append_is_not_deadlocked_by_a_migration_waiting_on_its_transaction()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        var id = Guid.NewGuid();

        using (var existing = BuildStore(schema))
        {
            await using var seed = existing.LightweightSession();
            seed.Events.StartStream(id, new FirstUseTick());
            await seed.SaveChangesAsync(token);
        }

        // A second process: it has never ensured anything, so its save has to.
        using var store = BuildStore(schema, lockTimeout: TimeSpan.FromSeconds(10));

        await using var session = store.LightweightSession();
        await session.Events.AppendExclusive(id, token, new FirstUseTick());

        await using var migration = await LockHolder.TakeAsync(schema, token);
        await using var alter = migration.Connection.CreateCommand();
        alter.CommandText =
            $"""
             SET DEADLOCK_PRIORITY LOW;
             ALTER TABLE [{schema}].[pc_streams] ADD migrated_by_someone_else int NULL;
             """;
        var altering = alter.ExecuteNonQueryAsync(token);

        var blocked = await Task.WhenAny(altering, Task.Delay(500, token));
        blocked.ShouldNotBe(altering, "the migration should be blocked behind the session's transaction");

        var stopwatch = Stopwatch.StartNew();
        await session.SaveChangesAsync(token);
        stopwatch.Stop();

        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5),
            "a session holding a transaction must never wait on the migration lock");

        // The session's own statements meet the pending schema lock head on, on connections SQL Server
        // can see, so THAT cycle gets a victim. Either outcome is the migration resolving, and which one
        // it gets depends on timing. The low priority above only keeps the victim from being the save.
        try
        {
            await altering;
        }
        catch (SqlException e) when (e.Number == 1205)
        {
        }

        await migration.ReleaseAsync();

        await using var query = store.QuerySession();
        (await query.Events.FetchStreamStateAsync(id, token))!.Version.ShouldBe(2);
    }

    /// <summary>
    ///     The same guarantee for the document ensure, on the path that cannot be moved ahead of the
    ///     transaction: a type this process has not touched yet, with a stream lock already held.
    /// </summary>
    [Fact]
    public async Task a_document_read_inside_a_transaction_does_not_wait_for_the_lock()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        var id = Guid.NewGuid();

        using (var existing = BuildStore(schema))
        {
            await using var seed = existing.LightweightSession();
            seed.Events.StartStream(id, new FirstUseTick());
            seed.Store(new FirstUseDoc { Id = Guid.NewGuid() });
            await seed.SaveChangesAsync(token);
        }

        using var store = BuildStore(schema, lockTimeout: TimeSpan.FromSeconds(10));

        await using var session = store.LightweightSession();
        await session.Events.AppendExclusive(id, token, new FirstUseTick());

        await using var holder = await LockHolder.TakeAsync(schema, token);

        var stopwatch = Stopwatch.StartNew();
        (await session.LoadAsync<FirstUseDoc>(Guid.NewGuid(), token)).ShouldBeNull();
        stopwatch.Stop();

        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5),
            "a session holding a transaction must never wait on the migration lock");
    }

    /// <summary>
    ///     What not waiting costs. A table that genuinely is not there yet fails its statement, straight
    ///     away and with no wait, while somebody else is migrating. It must not be remembered as ensured,
    ///     or that table would never be created by this process.
    /// </summary>
    [Fact]
    public async Task a_table_missing_inside_a_transaction_fails_at_once_and_the_next_use_creates_it()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        var id = Guid.NewGuid();

        using (var existing = BuildStore(schema))
        {
            await using var seed = existing.LightweightSession();
            seed.Events.StartStream(id, new FirstUseTick());
            await seed.SaveChangesAsync(token);
        }

        using var store = BuildStore(schema, lockTimeout: TimeSpan.FromSeconds(10));

        await using (var holder = await LockHolder.TakeAsync(schema, token))
        {
            await using var session = store.LightweightSession();
            await session.Events.AppendExclusive(id, token, new FirstUseTick());

            var stopwatch = Stopwatch.StartNew();
            await Should.ThrowAsync<SqlException>(() => session.LoadAsync<FirstUseDoc>(Guid.NewGuid(), token));
            stopwatch.Stop();

            stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5),
                "a session holding a transaction must never wait on the migration lock");
        }

        await using var next = store.LightweightSession();
        (await next.LoadAsync<FirstUseDoc>(Guid.NewGuid(), token)).ShouldBeNull();
    }

    /// <summary>
    ///     A session that has opened a transaction still takes the lock when it is free, because the ensure
    ///     then runs like any other. Only a lock somebody else is holding changes what happens.
    /// </summary>
    [Fact]
    public async Task inside_a_transaction_the_lock_is_still_taken_when_it_is_free()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);

        await using (await SchemaMigrationLock.HoldForFirstUseAsync(
                         new StoreOptions { DatabaseSchemaName = schema }, conn, insideTransaction: true, token))
        {
            (await LockIsFreeAsync(schema, token)).ShouldBeFalse();
        }

        (await LockIsFreeAsync(schema, token)).ShouldBeTrue();
    }
}
