using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Core;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #847, the marten#5761 equivalent: the single-database daemon built its high-water detector with
///     <c>new Logger&lt;PolecatHighWaterDetector&gt;(new LoggerFactory())</c>, a factory with no providers.
///     "Skipping stale gap" is the only trace a skip leaves, and on <c>AddAsyncDaemon</c> over one database
///     it went nowhere. Multi-database and coordinator daemons were fine, which is why nobody noticed: they
///     build their detector through <c>PolecatDatabase.StartProjectionDaemon</c>.
/// </summary>
public class high_water_detector_logging_tests
{
    private const string SkipMessage = "Skipping stale gap";

    private static void ConfigureGappedStore(StoreOptions opts, string schema)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.DaemonSettings.StaleSequenceThreshold = 500.Milliseconds();
        opts.Projections.Add<CoordinatedTallyProjection>(ProjectionLifecycle.Async);
    }

    [Fact]
    public async Task a_skipped_gap_is_logged_by_a_single_database_async_daemon_host()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = "hw_log_" + Guid.NewGuid().ToString("N")[..8];
        var logs = new CapturedLogs();

        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(logs);
        builder.Services.AddPolecat(opts => ConfigureGappedStore(opts, schema))
            .AddAsyncDaemon(DaemonMode.Solo);

        using var host = builder.Build();

        await PunchAHoleAsync((DocumentStore)host.Services.GetRequiredService<IDocumentStore>(), schema);

        await host.StartAsync(token);
        await logs.WaitForAsync(SkipMessage);
        await host.StopAsync(token);
    }

    [Fact]
    public async Task a_skipped_gap_reaches_the_logger_handed_to_the_daemon()
    {
        var schema = "hw_log_" + Guid.NewGuid().ToString("N")[..8];
        var logs = new CapturedLogs();

        using var store = DocumentStore.For(opts => ConfigureGappedStore(opts, schema));
        await PunchAHoleAsync(store, schema);

        using var daemon = await store.BuildProjectionDaemonAsync(logger: logs.CreateLogger("the daemon's logger"));
        await daemon.StartAllAsync();

        await logs.WaitForAsync(SkipMessage);
        await daemon.StopAllAsync();
    }

    // Leaves seq_ids 1 and 3 with no row at 2: a dead hole, so the mark holds at 1 until the threshold.
    private static async Task PunchAHoleAsync(DocumentStore store, string schema)
    {
        var token = TestContext.Current.CancellationToken;

        for (var i = 0; i < 3; i++)
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new CoordinatedTick());
            await session.SaveChangesAsync(token);
        }

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            DELETE FROM [{schema}].[pc_events]
            WHERE seq_id = (SELECT MIN(seq_id) + 1 FROM [{schema}].[pc_events]);
            """;
        (await cmd.ExecuteNonQueryAsync(token)).ShouldBe(1);
    }

    private sealed class CapturedLogs : ILoggerProvider, ILogger
    {
        private readonly ConcurrentQueue<string> _messages = new();

        public async Task WaitForAsync(string fragment)
        {
            var deadline = DateTimeOffset.UtcNow.Add(10.Seconds());
            while (DateTimeOffset.UtcNow < deadline)
            {
                if (_messages.Any(x => x.Contains(fragment))) return;
                await Task.Delay(50, TestContext.Current.CancellationToken);
            }

            throw new TimeoutException(
                $"Nothing logged '{fragment}'. Captured {_messages.Count} other message(s).");
        }

        public ILogger CreateLogger(string categoryName) => this;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => _messages.Enqueue(formatter(state, exception));

        public void Dispose()
        {
        }
    }
}
