using JasperFx;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

public record LoggedTallyStarted;

public record LoggedTallied(bool Poison);

public partial class LoggedTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

/// <summary>Throws on a poisoned <see cref="LoggedTallied" />, so the daemon parks the stream.</summary>
public partial class LoggedTallyProjection: SingleStreamProjection<LoggedTally, Guid>
{
    public static LoggedTally Create(IEvent<LoggedTallyStarted> e) => new() { Id = e.StreamId };

    public void Apply(LoggedTallied e, LoggedTally tally)
    {
        if (e.Poison) throw new InvalidOperationException("Can't tally this one");
        tally.Count++;
    }
}

/// <summary>
///     #1109 (fisher#658): a daemon logs through the host's <c>ILoggerFactory</c> however it was started.
/// </summary>
/// <remarks>
///     Wolverine-managed projection distribution builds each daemon through
///     <c>IEventStore.BuildProjectionDaemonAsync(DatabaseId)</c> and starts one agent at a time with
///     <c>StartAgentAsync(ShardName)</c>. That seam has no logger parameter, and Polecat used to hand the daemon
///     <c>NullLogger</c> there, so the identity access line, every parked identity and
///     <c>ParkedLimitReachedException</c> went nowhere. The first test drives that seam exactly as Wolverine does;
///     Wolverine itself isn't referenced here.
/// </remarks>
public class daemon_logging
{
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(60);

    private readonly CapturingLoggerProvider _log = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static void configure(StoreOptions opts)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = "daemon_log_" + Guid.NewGuid().ToString("N")[..8];
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Projections.Add<LoggedTallyProjection>(ProjectionLifecycle.Async);

        // So the second poisoned identity passes the cap and throws ParkedLimitReachedException
        opts.Projections.Errors.MaxParkedIdentitiesPerShard = 1;
    }

    private ServiceProvider providerWith(Action<PolecatConfigurationExpression>? polecat = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(x => x.AddProvider(_log).SetMinimumLevel(LogLevel.Debug));

        var expression = services.AddPolecat(configure);
        polecat?.Invoke(expression);

        return services.BuildServiceProvider();
    }

    private static async Task<Guid> tallyAsync(IDocumentStore store, bool poison)
    {
        var stream = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream<LoggedTally>(stream, new LoggedTallyStarted(), new LoggedTallied(poison));
        await session.SaveChangesAsync(Ct);
        return stream;
    }

    private static async Task eventuallyAsync(Func<Task<bool>> condition, string description)
    {
        var deadline = DateTimeOffset.UtcNow.Add(Generous);
        while (!await condition())
        {
            DateTimeOffset.UtcNow.ShouldBeLessThan(deadline, $"Timed out waiting until {description}");
            await Task.Delay(100, Ct);
        }
    }

    private static async Task<bool> parkedCountIsAsync(DocumentStore store, int count)
        => (await ((IEventDatabase)store.Database).FetchParkedIdentitiesAsync(
            new ParkedIdentityQuery { ProjectionName = nameof(LoggedTally) }, Ct)).Count == count;

    private static async Task<DatabaseId> mainDatabaseIdAsync(IEventStore store)
    {
        // The same descriptor and the same id Wolverine's EventStoreAgents builds its agent URIs from
        var usage = (await store.TryDescribeDatabasesAsync(Ct)).ShouldNotBeNull();
        var database = usage.MainDatabase ?? usage.Databases.ShouldHaveSingleItem();
        return new DatabaseId(database.ServerName, database.DatabaseName);
    }

    private static bool isTheIdentityAccessLine(LogEntry x)
        => x.Level == LogLevel.Information
           && x.Message.StartsWith($"Projection {nameof(LoggedTally)} is ")
           && x.Message.Contains("a failing identity parks");

    private void theIdentityAccessLineWasLogged()
    {
        _log.Entries.ShouldContain(x => isTheIdentityAccessLine(x),
            $"No identity access line was logged for {nameof(LoggedTally)}. Logged:\n" + _log.Dump());
    }

    private void theParkWasLogged(Guid stream)
    {
        _log.Entries.ShouldContain(x => x.Level == LogLevel.Warning
                                        && x.Message.Contains("parked " + ParkedIdentityText.Format(stream))
                                        && x.Exception != null,
            "No parked identity was logged. Logged:\n" + _log.Dump());
    }

    [Fact]
    public async Task a_daemon_built_and_started_the_way_wolverine_does_it_logs_its_parking()
    {
        await using var provider = providerWith();
        var store = (DocumentStore)provider.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        IEventStore eventStore = store;
        var databaseId = await mainDatabaseIdAsync(eventStore);
        var shard = eventStore.RegisteredShardNames().Single();

        var daemon = await eventStore.BuildProjectionDaemonAsync(databaseId);
        try
        {
            theIdentityAccessLineWasLogged();

            await daemon.StartAgentAsync(shard, Ct);

            var first = await tallyAsync(store, poison: true);
            await eventuallyAsync(() => parkedCountIsAsync(store, 1), "the first identity is parked");

            theParkWasLogged(first);

            // Over the cap of 1, so the shard pauses on ParkedLimitReachedException
            await tallyAsync(store, poison: true);
            await eventuallyAsync(() => Task.FromResult(daemon.StatusFor(shard.Identity) == AgentStatus.Paused),
                "the shard pauses");

            _log.Entries.ShouldContain(x => x.Exception is ParkedLimitReachedException,
                "ParkedLimitReachedException was not logged. Logged:\n" + _log.Dump());
        }
        finally
        {
            await daemon.StopAllAsync();
            daemon.Dispose();
        }
    }

    [Fact]
    public async Task the_hosted_daemon_logs_its_parking()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(_log).SetMinimumLevel(LogLevel.Debug);
        builder.Services.AddPolecat(configure).AddAsyncDaemon(DaemonMode.Solo);

        using var host = builder.Build();
        var store = (DocumentStore)host.Services.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await host.StartAsync(Ct);
        try
        {
            await eventuallyAsync(() => Task.FromResult(_log.Entries.Any(isTheIdentityAccessLine)),
                "the identity access line is logged");

            var stream = await tallyAsync(store, poison: true);
            await eventuallyAsync(() => parkedCountIsAsync(store, 1), "the identity is parked");

            theParkWasLogged(stream);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task a_daemon_built_by_hand_from_a_registered_store_logs_through_the_host()
    {
        await using var provider = providerWith();
        var store = (DocumentStore)provider.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        using var daemon = await store.BuildProjectionDaemonAsync();

        theIdentityAccessLineWasLogged();
        _log.Entries.ShouldContain(x => x.Category == typeof(Polecat.Events.Daemon.PolecatProjectionDaemon).FullName);
    }

    [Fact]
    public async Task a_daemon_handed_a_logger_uses_that_one()
    {
        await using var provider = providerWith();
        var store = (DocumentStore)provider.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var own = new CapturingLoggerProvider();
        using var daemon = await store.BuildProjectionDaemonAsync(logger: own.CreateLogger("the daemon's own logger"));

        own.Entries.ShouldContain(x => isTheIdentityAccessLine(x),
            "The logger handed to the daemon got no identity access line. Logged:\n" + own.Dump());
        _log.Entries.ShouldNotContain(x => isTheIdentityAccessLine(x),
            "The host's logger was used instead of the one handed to the daemon. Logged:\n" + _log.Dump());
    }

    [Fact]
    public void a_store_built_without_a_container_has_no_logger_factory()
    {
        using var store = DocumentStore.For(configure);

        store.Options.LogFactory.ShouldBeNull();
        store.DefaultDaemonLogger().ShouldBeSameAs(NullLogger.Instance);
    }

    private sealed record LogEntry(string Category, LogLevel Level, string Message, Exception? Exception);

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        private readonly List<LogEntry> _entries = [];

        public IReadOnlyList<LogEntry> Entries
        {
            get
            {
                lock (_entries) return [.._entries];
            }
        }

        public string Dump()
            => string.Join("\n",
                Entries.Select(x => $"[{x.Level}] {x.Category}: {x.Message} {x.Exception?.GetType().Name}"));

        public ILogger CreateLogger(string categoryName) => new Capturing(categoryName, _entries);

        public void Dispose()
        {
        }

        private sealed class Capturing(string category, List<LogEntry> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                lock (entries) entries.Add(new LogEntry(category, logLevel, formatter(state, exception), exception));
            }
        }
    }
}
