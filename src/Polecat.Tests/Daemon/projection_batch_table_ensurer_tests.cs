using Polecat.Events.Daemon;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     Regression coverage for [polecat#538](https://github.com/JasperFx/polecat/issues/538):
///     <see cref="PolecatProjectionBatch.ExecuteAsync"/> constructed a fresh
///     <see cref="Polecat.Internal.DocumentTableEnsurer"/> per projection batch. The ensurer
///     memoizes on instance state, so every daemon batch re-ran the full schema-ensure work
///     (SchemaMigration diff, version-column widening, index DDL) per projected document type —
///     the same bug class as marten#4946. The daemon must reuse the store's long-lived
///     per-database ensurer so the work runs once per database per type, not per batch.
/// </summary>
public class projection_batch_table_ensurer_tests : OneOffConfigurationsContext
{
    [Fact]
    public void store_hands_out_the_same_ensurer_per_database()
    {
        var defaultCs = theStore.Database.ConnectionString;

        // Stable identity per database — this is what makes the memoization effective.
        theStore.EnsurerFor(defaultCs).ShouldBeSameAs(theStore.EnsurerFor(defaultCs));

        // A different database gets its own (but equally stable) ensurer, so the scoping is
        // per database rather than blindly global — required for database-per-tenant tenancy.
        var otherCs = ConnectionSource.ConnectionStringFor("polecat_ensurer_identity_probe");
        var other = theStore.EnsurerFor(otherCs);
        other.ShouldNotBeSameAs(theStore.EnsurerFor(defaultCs));
        other.ShouldBeSameAs(theStore.EnsurerFor(otherCs));
    }

    [Fact]
    public async Task schema_ensure_work_runs_once_per_document_type_across_daemon_batches()
    {
        ConfigureStore(opts => opts.DatabaseSchemaName = "batch_ensurer_reuse");
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(
            ct: TestContext.Current.CancellationToken);

        var ensurer = theStore.EnsurerFor(theStore.Database.ConnectionString);
        ensurer.SchemaEnsureExecutions.ShouldBe(0);

        // Simulate the daemon: one PolecatProjectionBatch per page of events, each writing the
        // same projected document type.
        for (var i = 0; i < 3; i++)
        {
            var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph,
                theStore.Database);
            var session = batch.SessionForTenant(theStore.Options.Tenancy!.DefaultTenantId);
            session.Store(new BatchedDoc { Id = Guid.NewGuid() });

            await batch.ExecuteAsync(TestContext.Current.CancellationToken);
            await batch.DisposeAsync();
        }

        // The regression signature: with a per-batch ensurer the store's shared instance never
        // ran (0) while three throwaway instances each ran the full schema check. With the fix,
        // the shared ensurer ran the work exactly once for BatchedDoc across all three batches.
        ensurer.SchemaEnsureExecutions.ShouldBe(1);
    }

    public class BatchedDoc
    {
        public Guid Id { get; set; }
    }
}
