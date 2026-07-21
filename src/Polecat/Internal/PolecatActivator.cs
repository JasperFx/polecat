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
            await documentStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: cancellationToken);
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
