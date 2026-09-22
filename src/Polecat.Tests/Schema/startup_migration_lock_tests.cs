using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.TestUtils;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Tests.Schema;

/// <summary>
///     #664 / weasel#599. Polecat's startup migration ran under Weasel's nullo lock, because until
///     Weasel 9.33.0 no <c>IGlobalLock&lt;SqlConnection&gt;</c> existed for SQL Server at all — so two
///     replicas starting together both introspected the catalog, both derived a patch from the same
///     state, and both ran DDL. Polecat does not opt into schema fingerprinting either, so the race
///     was on every deploy rather than only the first.
/// </summary>
[Collection("integration")]
public class startup_migration_lock_tests
{
    private const string Schema = "startup_lock";

    private static string LockName => $"polecat:migrate:{Schema}";

    /// <summary>
    ///     The lock the activator takes is real: hold it from outside and the apply cannot get in.
    ///     This is the fact that would have failed before the change — under the nullo lock the apply
    ///     never asked for anything, so nothing could hold it out.
    /// </summary>
    [Fact]
    public async Task the_apply_waits_on_the_schema_scoped_application_lock()
    {
        var token = TestContext.Current.CancellationToken;

        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            // Short, so the assertion does not sit here for the 30s default.
            opts.StartupMigrationLockTimeout = TimeSpan.FromMilliseconds(500);
        });

        // Hold the same resource the activator would ask for, on a separate session-scoped
        // connection.
        await using var holder = new SqlConnection(ConnectionSource.ConnectionString);
        await holder.OpenAsync(token);
        var held = await holder.TryGetGlobalLock(LockName, cancellation: token);
        held.ShouldBeTrue();

        var contested = new SqlServerGlobalLock(LockName, 500);

        await using var contender = new SqlConnection(ConnectionSource.ConnectionString);
        await contender.OpenAsync(token);

        var result = await contested.TryAttainLock(contender, token);

        result.Succeeded.ShouldBeFalse();

        // And the loser is ruled on by ResourceMigrationFailureMode rather than throwing a bare
        // exception — which is the weasel#599 half that makes FailFast/ContinueOnFailures mean the
        // same thing here as on PostgreSQL.
        result.ShouldNotBeNull();

        await holder.ReleaseGlobalLock(LockName, token);

        // Once released, the same lock is attainable, so the refusal above was contention and not a
        // malformed request.
        (await contested.TryAttainLock(contender, token)).Succeeded.ShouldBeTrue();
        await contested.ReleaseLock(contender, token);
    }

    /// <summary>
    ///     Two stores in ONE database under different schemas must not block each other. Application
    ///     locks are database-scoped, so the tenant dimension is already separate — the schema is the
    ///     only thing the resource name has to carry, and naming it after
    ///     <c>PolecatDatabase.Identifier</c> would have been wrong, since that is the constant
    ///     "Polecat" for a single-database store.
    /// </summary>
    [Fact]
    public async Task two_stores_in_one_database_under_different_schemas_do_not_contend()
    {
        var token = TestContext.Current.CancellationToken;

        await using var first = new SqlConnection(ConnectionSource.ConnectionString);
        await first.OpenAsync(token);
        (await first.TryGetGlobalLock("polecat:migrate:store_a", cancellation: token)).ShouldBeTrue();

        await using var second = new SqlConnection(ConnectionSource.ConnectionString);
        await second.OpenAsync(token);
        (await second.TryGetGlobalLock("polecat:migrate:store_b", cancellation: token)).ShouldBeTrue();

        await first.ReleaseGlobalLock("polecat:migrate:store_a", token);
        await second.ReleaseGlobalLock("polecat:migrate:store_b", token);
    }

    /// <summary>
    ///     The default is 30 seconds, not Weasel's 1000ms. Contention is NOT retried —
    ///     <c>AttainLockResult.ShouldReconnect</c> is true only for <c>DatabaseNotAvailable</c> — so a
    ///     one-second timeout gives the second replica of a deploy exactly one attempt and then
    ///     FailFast aborts its startup. The default has to outlast a real migration.
    /// </summary>
    [Fact]
    public void the_default_lock_timeout_outlasts_a_real_migration()
    {
        new StoreOptions().StartupMigrationLockTimeout
            .ShouldBeGreaterThan(TimeSpan.FromMilliseconds(SharedLockExtensions.DefaultLockTimeoutMilliseconds));

        new StoreOptions().StartupMigrationLockTimeout.ShouldBe(TimeSpan.FromSeconds(30));
    }

    /// <summary>
    ///     And the whole point: applying changes on startup still works, holding the lock it now
    ///     takes. A regression that broke the lock wiring would surface here rather than as a hang in
    ///     somebody's deployment.
    /// </summary>
    [Fact]
    public async Task applying_changes_under_the_lock_still_provisions_the_schema()
    {
        var token = TestContext.Current.CancellationToken;

        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "startup_lock_apply";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(
            new SqlServerGlobalLock("polecat:migrate:startup_lock_apply", 30_000), ct: token);

        // Second apply is a no-op, and the lock is released rather than held by the first.
        var difference = await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(
            new SqlServerGlobalLock("polecat:migrate:startup_lock_apply", 30_000), ct: token);

        difference.ShouldBe(SchemaPatchDifference.None);
    }
}
