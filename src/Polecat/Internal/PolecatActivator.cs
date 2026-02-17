using Microsoft.Extensions.Hosting;

namespace Polecat.Internal;

/// <summary>
///     Hosted service that applies database schema changes on startup.
///     Registered by ApplyAllDatabaseChangesOnStartup().
/// </summary>
internal class PolecatActivator : IHostedService
{
    private readonly IDocumentStore _store;

    public PolecatActivator(IDocumentStore store)
    {
        _store = store;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_store.Options.ShouldApplyChangesOnStartup)
        {
            var documentStore = (DocumentStore)_store;
            await documentStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: cancellationToken);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
