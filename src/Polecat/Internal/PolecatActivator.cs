using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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
                await database.ApplyAllConfiguredChangesToDatabaseAsync(ct: cancellationToken);
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

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
