using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Weasel.SqlServer;

namespace Polecat.Internal;

/// <summary>
///     Always-on hosted service (registered unconditionally by <c>AddPolecat</c>, #219) that surfaces the
///     #345 application-assembly-reuse warning, applies database schema changes when
///     <c>ApplyAllDatabaseChangesOnStartup()</c> opted in, and runs InitialData seeders on startup.
/// </summary>
internal class PolecatActivator : IHostedService
{
    private readonly IDocumentStore _store;
    private readonly ILogger<PolecatActivator> _logger;

    public PolecatActivator(IDocumentStore store, ILogger<PolecatActivator>? logger = null)
    {
        _store = store;
        _logger = logger ?? NullLogger<PolecatActivator>.Instance;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // #345: surface JasperFx's GH-3521 application-assembly-reuse warning (jasperfx#543) once, early,
        // so it is logged even if the schema migration below later throws. JasperFx only detects the
        // condition — consumers surface it, and Polecat has no other always-on emit point.
        if (_store.Options.ApplicationAssemblyReuseWarning is { } reuseWarning)
        {
            _logger.LogWarning("{Warning}", reuseWarning);
        }

        if (_store.Options.ShouldApplyChangesOnStartup)
        {
            var documentStore = (DocumentStore)_store;

            // #514: migrate EVERY tenant database, not just the one behind
            // StoreOptions.ConnectionString. Under database-per-tenant tenancy the other tenants'
            // databases were silently left unprovisioned, so the first write to them failed at
            // runtime with a missing-table error long after startup had reported success. Mirrors
            // Marten's MartenActivator, which iterates Store.Tenancy.BuildDatabases(). Resolved
            // asynchronously so a dynamic tenancy (MasterTableTenancy) reads its control table here.
            var tenantDatabases = _store.Options.Tenancy is { } tenancy
                ? await tenancy.BuildDatabasesAsync(cancellationToken)
                : [documentStore.Database];

            foreach (var database in tenantDatabases)
            {
                await database.ApplyAllConfiguredChangesToDatabaseAsync(
                    BuildMigrationLock(), ct: cancellationToken);
            }

            // #386: roll every configured rolling-window RANGE partition forward and retire the aged
            // ones. The migration above already provisions the leading edge — with a rolling-window
            // manager attached the delta is additive, a SPLIT rather than a rebuild — but migration
            // never removes data, so the retention half has to be driven separately. Gated on the same
            // opt-in as the migration itself: applying changes on startup is how a host says "Polecat
            // owns this schema", and retiring a partition is emphatically a schema change.
            await Storage.RollingPartitions.ApplyAsync(tenantDatabases, _logger, rollForward: true, dropAged: true,
                cancellationToken);
        }

        // Run initial data seeders after schema migration
        foreach (var initialData in _store.Options.InitialData)
        {
            await initialData.Populate(_store, cancellationToken);
        }
    }

    /// <summary>
    ///     The <c>sp_getapplock</c> lock a startup migration holds while it applies.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     #664 / weasel#599. Until Weasel 9.33.0 there was no <c>IGlobalLock&lt;SqlConnection&gt;</c>
    ///     at all, so this call passed Weasel's nullo lock and two replicas starting together both
    ///     introspected the catalog, both derived a patch from the same state, and both ran DDL.
    ///     Polecat does not opt into <c>Migrator.UseSchemaFingerprinting</c>, so there is no stamp
    ///     short-circuit either — the race was on every deploy, not only the first.
    ///     </para>
    ///     <para>
    ///     Application locks are scoped to the CURRENT DATABASE, and the apply opens its own
    ///     connection per tenant database, so per-database isolation falls out for free: one tenant's
    ///     migration must not block another's, and it does not. What does not fall out is two Polecat
    ///     stores inside one database under different schemas — the ancillary-store pattern, and every
    ///     test in this repo — so the resource name is the schema.
    ///     </para>
    ///     <para>
    ///     The timeout is <see cref="StoreOptions.StartupMigrationLockTimeout" /> rather than
    ///     Weasel's 1000ms default, because contention is not retried and the loser would otherwise
    ///     abort startup after one second of waiting on a migration that takes longer than that. See
    ///     that property for the whole reasoning.
    ///     </para>
    /// </remarks>
    private SqlServerGlobalLock BuildMigrationLock()
    {
        var timeout = _store.Options.StartupMigrationLockTimeout;
        var timeoutMs = (int)Math.Clamp(timeout.TotalMilliseconds, 0, int.MaxValue);

        // The schema and nothing else. sp_getapplock resources are database-scoped and the apply
        // opens its own connection per tenant database, so the tenant dimension is already
        // separate; naming the database as well would be redundant, and naming it via
        // PolecatDatabase.Identifier would be wrong — that is the constant "Polecat" for a
        // single-database store, so two stores sharing one database would share one lock.
        return new SqlServerGlobalLock($"polecat:migrate:{_store.Options.DatabaseSchemaName}", timeoutMs);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
