using JasperFx;
using JasperFx.Events;
using JasperFx.Events.ComplianceTests;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.Hosting;
using Polecat.Tests.Harness;
using Spectre.Console;

namespace Polecat.Tests.CommandLine;

/// <summary>
///     #1105 (marten#6122): <c>projections parked</c> on the command line against a real Polecat store. Listing reads
///     <c>pc_parked_identities</c>. A replay or a discard writes a durable request on the parked row from a process
///     that runs no daemon, and the daemon that runs the shard picks it up. The command runs through
///     <c>RunJasperFxCommands</c>, the way a developer runs it, against a host of its own.
/// </summary>
/// <remarks>
///     One store, the daemon's, stands in for the node that runs the shard. The command builds its own store from
///     the same options, as a separate process would.
/// </remarks>
public class projections_parked_command_end_to_end : IAsyncLifetime
{
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(60);
    private static readonly string Projection = nameof(ParkedGuidCounter);

    private readonly List<Guid> _poisoned = new();
    private readonly List<IAsyncDisposable> _disposables = new();
    private string _schema = null!;
    private bool _extendedProgression;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var stream in _poisoned) ParkingPoison.Off(stream);
        for (var i = _disposables.Count - 1; i >= 0; i--) await _disposables[i].DisposeAsync();
    }

    private void configure(StoreOptions opts)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = _schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Events.AddEventType<ParkStarted>();
        opts.Events.AddEventType<ParkTicked>();
        opts.Events.AddEventType<ParkPoisoned>();
        opts.Projections.Snapshot<ParkedGuidCounter>(SnapshotLifecycle.Async);
        opts.Events.EnableExtendedProgressionTracking = _extendedProgression;
    }

    private async Task<DocumentStore> aStoreAsync(string schema)
    {
        _schema = schema;
        await TestSchema.DropSchemaTablesAsync(schema);

        var store = DocumentStore.For(configure);
        _disposables.Add(store);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        return store;
    }

    private async Task<IProjectionDaemon> startDaemonAsync(DocumentStore store)
    {
        var daemon = await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        _disposables.Add(new DaemonStopper(daemon));
        return daemon;
    }

    private sealed class DaemonStopper(IProjectionDaemon daemon): IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await daemon.StopAllAsync();
            daemon.Dispose();
        }
    }

    /// <summary>A stream of three events whose second one throws while the stream is poisoned.</summary>
    private async Task<Guid> aPoisonedStreamAsync(DocumentStore store)
    {
        var stream = Guid.NewGuid();
        ParkingPoison.On(stream);
        _poisoned.Add(stream);

        await using var session = store.LightweightSession();
        session.Events.StartStream<ParkedGuidCounter>(stream, new ParkStarted(), new ParkPoisoned(), new ParkTicked());
        await session.SaveChangesAsync(Ct);
        return stream;
    }

    private static async Task<IReadOnlyList<ParkedIdentity>> parkedAsync(DocumentStore store)
        => await ((IEventDatabase)store.Database).FetchParkedIdentitiesAsync(
            new ParkedIdentityQuery { ProjectionName = Projection }, Ct);

    private static async Task<ParkedGuidCounter?> counterAsync(DocumentStore store, Guid stream)
    {
        await using var query = store.QuerySession();
        return await query.LoadAsync<ParkedGuidCounter>(stream, Ct);
    }

    /// <summary>Run <c>projections parked ...</c> the way the command line does, and capture what it writes.</summary>
    private async Task<(int ExitCode, string Output)> runAsync(params string[] arguments)
    {
        var writer = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Out = new AnsiConsoleOutput(writer)
        });

        // Wide enough that the table never wraps a GUID across two lines
        console.Profile.Width = 500;

        var original = AnsiConsole.Console;
        AnsiConsole.Console = console;
        try
        {
            var exitCode = await Host.CreateDefaultBuilder()
                .ConfigureServices(services => services.AddPolecat(configure))
                .RunJasperFxCommands(["projections", "parked", ..arguments]);

            return (exitCode, writer.ToString());
        }
        finally
        {
            AnsiConsole.Console = original;
        }
    }

    [Fact]
    public async Task listing_shows_every_parked_identity_with_the_error_that_parked_it()
    {
        var store = await aStoreAsync("parked_cli_list");
        await startDaemonAsync(store);

        var (emptyExit, empty) = await runAsync();
        emptyExit.ShouldBe(0, empty);
        empty.ShouldContain("Nothing is parked.");

        var first = await aPoisonedStreamAsync(store);
        var second = await aPoisonedStreamAsync(store);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);
        (await parkedAsync(store)).Count.ShouldBe(2);

        var (exitCode, output) = await runAsync();

        exitCode.ShouldBe(0, output);
        output.ShouldContain(Projection);
        output.ShouldContain(ParkedIdentityText.Format(first));
        output.ShouldContain(ParkedIdentityText.Format(second));
        output.ShouldContain($"Stream {first:D} is poisoned");
        output.ShouldContain($"Stream {second:D} is poisoned");

        // Filtered to a projection the store doesn't have, there's nothing to list
        var (otherExit, other) = await runAsync("--projection", "NotAProjection");
        otherExit.ShouldBe(0, other);
        other.ShouldNotContain(ParkedIdentityText.Format(first));
    }

    [Fact]
    public async Task replay_is_done_by_the_daemon_that_runs_the_shard()
    {
        var store = await aStoreAsync("parked_cli_replay");
        await startDaemonAsync(store);

        var fixedStream = await aPoisonedStreamAsync(store);
        var stillBroken = await aPoisonedStreamAsync(store);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);
        ((await counterAsync(store, fixedStream))?.Ticks ?? 0).ShouldBe(0);

        ParkingPoison.Off(fixedStream);

        var (exitCode, output) = await runAsync("--projection", Projection, "--replay", "--identity",
            ParkedIdentityText.Format(fixedStream), "--wait", "60");

        exitCode.ShouldBe(0, output);
        output.ShouldContain($"asked for a replay of 1 parked identity of {Projection}");
        output.ShouldContain("Done: nothing requested is still waiting.");

        // The replayed stream folded every event, and the other one is still parked
        (await counterAsync(store, fixedStream))!.Ticks.ShouldBe(2);
        (await parkedAsync(store)).ShouldHaveSingleItem().IdentityText
            .ShouldBe(ParkedIdentityText.Format(stillBroken));
    }

    [Fact]
    public async Task discard_skips_the_event_that_still_fails_and_unparks_the_identity()
    {
        var store = await aStoreAsync("parked_cli_discard");
        await startDaemonAsync(store);

        var stream = await aPoisonedStreamAsync(store);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);
        (await parkedAsync(store)).ShouldHaveSingleItem();

        // A discard is refused without an identity, and changes nothing
        var (refusedExit, refused) = await runAsync("--projection", Projection, "--discard", "--yes");
        refusedExit.ShouldNotBe(0, refused);
        refused.ShouldContain("--identity");
        (await parkedAsync(store)).ShouldHaveSingleItem();

        // Still poisoned: the discard dead-letters the failing event and applies the rest
        var (exitCode, output) = await runAsync("--projection", Projection, "--discard", "--identity",
            ParkedIdentityText.Format(stream), "--yes", "--wait", "60");

        exitCode.ShouldBe(0, output);
        output.ShouldContain($"asked for a discard of 1 parked identity of {Projection}");

        (await parkedAsync(store)).ShouldBeEmpty();
        (await counterAsync(store, stream))!.Ticks.ShouldBe(1);
    }

    [Fact]
    public async Task a_replay_asked_for_while_no_daemon_runs_waits_for_the_shard_to_start()
    {
        // The command tells a stopped shard from a running one by the agent status in pc_event_progression
        _extendedProgression = true;
        var store = await aStoreAsync("parked_cli_offline");
        var daemon = await startDaemonAsync(store);

        var stream = await aPoisonedStreamAsync(store);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);
        await daemon.StopAllAsync();

        var shard = ((IEventStore)store).RegisteredShardNames().Single();
        var stopped = DateTimeOffset.UtcNow.Add(Generous);
        while ((await ((IEventDatabase)store.Database).ReadProjectionProgressAsync(shard, Ct))?.AgentStatus ==
               nameof(AgentStatus.Running))
        {
            DateTimeOffset.UtcNow.ShouldBeLessThan(stopped, "the stopped shard still reads as running");
            await Task.Delay(100, Ct);
        }

        ParkingPoison.Off(stream);

        var (exitCode, output) = await runAsync("--projection", Projection, "--replay", "--all");

        exitCode.ShouldBe(0, output);
        output.ShouldContain($"No node is running {Projection}", customMessage: output);
        (await parkedAsync(store)).ShouldHaveSingleItem().RequestedAction.ShouldBe(ParkedAction.Replay);

        await daemon.StartAllAsync();

        // jasperfx#1263: a restarted shard loads its parked rows with its first batch, so until that is fixed the
        // request waits for the shard's next event rather than its start. Drop this append once it is.
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<ParkedGuidCounter>(Guid.NewGuid(), new ParkStarted());
            await session.SaveChangesAsync(Ct);
        }

        var deadline = DateTimeOffset.UtcNow.Add(Generous);
        while ((await parkedAsync(store)).Count > 0)
        {
            DateTimeOffset.UtcNow.ShouldBeLessThan(deadline, "the shard never ran the requested replay");
            await Task.Delay(100, Ct);
        }

        (await counterAsync(store, stream))!.Ticks.ShouldBe(2);
    }
}
