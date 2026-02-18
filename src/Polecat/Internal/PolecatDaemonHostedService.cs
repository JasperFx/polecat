using JasperFx.Events.Daemon;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Polecat.Internal;

/// <summary>
///     Hosted service that starts and stops the async projection daemon.
///     Registered by AddAsyncDaemon().
/// </summary>
internal class PolecatDaemonHostedService : IHostedService, IAsyncDisposable
{
    private readonly DocumentStore _store;
    private readonly ILoggerFactory _loggerFactory;
    private IProjectionDaemon? _daemon;

    public PolecatDaemonHostedService(DocumentStore store, ILoggerFactory loggerFactory)
    {
        _store = store;
        _loggerFactory = loggerFactory;
    }

    public async Task StartAsync(CancellationToken token)
    {
        var logger = _loggerFactory.CreateLogger<PolecatDaemonHostedService>();
        _daemon = await _store.BuildProjectionDaemonAsync(logger: logger);
        await _daemon.StartAllAsync();
    }

    public async Task StopAsync(CancellationToken token)
    {
        if (_daemon != null)
        {
            await _daemon.StopAllAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _daemon?.Dispose();
    }
}
