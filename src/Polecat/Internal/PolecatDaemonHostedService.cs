using JasperFx.Events.Daemon;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Polecat.Internal;

/// <summary>
///     Hosted service that starts and stops the async projection daemon.
///     Registered by AddAsyncDaemon().
/// </summary>
public class PolecatDaemonHostedService : IHostedService, IAsyncDisposable
{
    private readonly DocumentStore _store;
    private readonly ILoggerFactory _loggerFactory;

    public PolecatDaemonHostedService(DocumentStore store, ILoggerFactory loggerFactory)
    {
        _store = store;
        _loggerFactory = loggerFactory;
    }

    /// <summary>
    ///     The running projection daemon, if started.
    /// </summary>
    public IProjectionDaemon? Daemon { get; private set; }

    public async Task StartAsync(CancellationToken token)
    {
        var logger = _loggerFactory.CreateLogger<PolecatDaemonHostedService>();
        Daemon = await _store.BuildProjectionDaemonAsync(logger: logger);
        await Daemon.StartAllAsync();
    }

    public async Task StopAsync(CancellationToken token)
    {
        if (Daemon != null)
        {
            await Daemon.StopAllAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        Daemon?.Dispose();
    }
}
