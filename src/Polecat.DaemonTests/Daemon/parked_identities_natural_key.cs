using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.ComplianceTests;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.Hosting;
using Polecat.Tests.Harness;
using Spectre.Console;

namespace Polecat.Tests.Daemon;

public record ParkKeyAssigned(string Code);

/// <summary>A Guid stream's counter with a natural key, which a poisoned event parks.</summary>
public partial class ParkedKeyedGuidCounter
{
    public Guid Id { get; set; }

    [NaturalKey]
    public string Code { get; set; } = string.Empty;

    public int Ticks { get; set; }

    public static ParkedKeyedGuidCounter Create(IEvent<ParkStarted> e) => new() { Id = e.StreamId };

    [NaturalKeySource]
    public void Apply(ParkKeyAssigned e) => Code = e.Code;

    public void Apply(ParkTicked e) => Ticks++;

    public void Apply(IEvent<ParkPoisoned> e)
    {
        ParkingPoison.ThrowIfPoisoned(e);
        Ticks++;
    }
}

/// <summary>
///     #1131 (marten#6150): an Async read model of the same streams that owns no key, mapped to
///     <see cref="ParkedKeyedGuidCounter" />'s, whose lookup names its parked identities.
/// </summary>
public partial class ParkedMappedGuidCounter
{
    public Guid Id { get; set; }

    [MappedNaturalKey<ParkedKeyedGuidCounter>]
    public string Code { get; set; } = string.Empty;

    public int Ticks { get; set; }

    public static ParkedMappedGuidCounter Create(IEvent<ParkStarted> e) => new() { Id = e.StreamId };

    public void Apply(ParkKeyAssigned e) => Code = e.Code;

    public void Apply(ParkTicked e) => Ticks++;

    public void Apply(IEvent<ParkPoisoned> e)
    {
        ParkingPoison.ThrowIfPoisoned(e);
        Ticks++;
    }
}

/// <summary>
///     A string store refuses a bare <c>string</c> natural key, because fetching by it would fetch by stream key, so the
///     string twin wraps its key.
/// </summary>
public record ParkCode(string Value);

/// <summary>The string stream twin of <see cref="ParkedKeyedGuidCounter" />.</summary>
public partial class ParkedKeyedStringCounter
{
    public string Id { get; set; } = string.Empty;

    [NaturalKey]
    public ParkCode? Code { get; set; }

    public int Ticks { get; set; }

    public static ParkedKeyedStringCounter Create(IEvent<ParkStarted> e) => new() { Id = e.StreamKey! };

    [NaturalKeySource]
    public void Apply(ParkKeyAssigned e) => Code = new ParkCode(e.Code);

    public void Apply(ParkTicked e) => Ticks++;

    public void Apply(IEvent<ParkPoisoned> e)
    {
        ParkingPoison.ThrowIfPoisoned(e);
        Ticks++;
    }
}

/// <summary>
///     #1117 (marten#6132, jasperfx#1273): the parked-identity list shows the natural key of each parked stream whose
///     aggregate has one. <c>FetchParkedIdentitiesAsync</c> fills <see cref="ParkedIdentity.NaturalKey" /> from the
///     aggregate's <c>pc_natural_key_*</c> table, with both stream identities and under conjoined tenancy, and
///     <c>projections parked</c> shows it in its "Natural Key" column.
/// </summary>
public class parked_identities_natural_key : IAsyncLifetime
{
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(60);

    private readonly List<IAsyncDisposable> _disposables = new();
    private readonly List<object> _poisoned = new();
    private Action<StoreOptions> _configure = _ => { };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var stream in _poisoned) ParkingPoison.Off(stream);
        for (var i = _disposables.Count - 1; i >= 0; i--) await _disposables[i].DisposeAsync();
    }

    private async Task<DocumentStore> aStoreAsync(string schema, bool guids, bool conjoined, bool mapped = false)
    {
        await TestSchema.DropSchemaTablesAsync(schema);

        _configure = opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            if (!guids) opts.Events.StreamIdentity = StreamIdentity.AsString;
            if (conjoined) opts.Events.TenancyStyle = TenancyStyle.Conjoined;

            opts.Events.AddEventType<ParkStarted>();
            opts.Events.AddEventType<ParkTicked>();
            opts.Events.AddEventType<ParkPoisoned>();
            opts.Events.AddEventType<ParkKeyAssigned>();

            // One projection whose aggregate has a natural key and one whose aggregate doesn't, over the same streams
            if (guids)
            {
                opts.Projections.Snapshot<ParkedKeyedGuidCounter>(SnapshotLifecycle.Async);
                opts.Projections.Snapshot<ParkedGuidCounter>(SnapshotLifecycle.Async);
                if (mapped) opts.Projections.Snapshot<ParkedMappedGuidCounter>(SnapshotLifecycle.Async);
            }
            else
            {
                opts.Projections.Snapshot<ParkedKeyedStringCounter>(SnapshotLifecycle.Async);
                opts.Projections.Snapshot<ParkedStringCounter>(SnapshotLifecycle.Async);
            }
        };

        var store = DocumentStore.For(_configure);
        _disposables.Add(store);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var daemon = await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        _disposables.Add(new DaemonStopper(daemon));

        return store;
    }

    private sealed class DaemonStopper(IProjectionDaemon daemon): IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await daemon.StopAllAsync();
            daemon.Dispose();
        }
    }

    private static string keyed(bool guids) => guids ? nameof(ParkedKeyedGuidCounter) : nameof(ParkedKeyedStringCounter);

    private static string keyless(bool guids) => guids ? nameof(ParkedGuidCounter) : nameof(ParkedStringCounter);

    /// <summary>A poisoned stream, with a natural key when <paramref name="code" /> is given.</summary>
    private async Task aPoisonedStreamAsync(DocumentStore store, object stream, string? tenant, string? code)
    {
        ParkingPoison.On(stream);
        _poisoned.Add(stream);

        var events = new List<object> { new ParkStarted() };
        if (code != null) events.Add(new ParkKeyAssigned(code));
        events.Add(new ParkPoisoned());
        events.Add(new ParkTicked());

        await using var session = tenant == null ? store.LightweightSession() : store.LightweightSession(tenant);
        if (stream is Guid id)
        {
            session.Events.StartStream<ParkedKeyedGuidCounter>(id, events.ToArray());
        }
        else
        {
            session.Events.StartStream<ParkedKeyedStringCounter>((string)stream, events.ToArray());
        }

        await session.SaveChangesAsync(Ct);
    }

    private static async Task<IReadOnlyList<ParkedIdentity>> parkedAsync(DocumentStore store, string? projection = null)
        => await ((IEventDatabase)store.Database).FetchParkedIdentitiesAsync(
            new ParkedIdentityQuery { ProjectionName = projection }, Ct);

    public static TheoryData<string> StreamIdentities => new() { "guid", "string" };

    [Theory]
    [MemberData(nameof(StreamIdentities))]
    public async Task the_parked_list_carries_the_natural_key_of_a_keyed_stream_and_null_otherwise(string kind)
    {
        var guids = kind == "guid";
        var store = await aStoreAsync($"parked_nk_{kind}", guids, conjoined: false);

        object withKey = guids ? Guid.NewGuid() : $"order/{Guid.NewGuid():N}";
        object withoutKey = guids ? Guid.NewGuid() : $"order/{Guid.NewGuid():N}";
        await aPoisonedStreamAsync(store, withKey, null, "ORD-1001");
        await aPoisonedStreamAsync(store, withoutKey, null, null);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        // Both projections parked both streams. Only the keyed aggregate's row for the keyed stream has a key.
        var all = await parkedAsync(store);
        all.Count.ShouldBe(4);

        var keyedRows = all.Where(x => x.ProjectionName == keyed(guids)).ToList();
        keyedRows.Single(x => x.IdentityText == ParkedIdentityText.Format(withKey)).NaturalKey.ShouldBe("ORD-1001");
        keyedRows.Single(x => x.IdentityText == ParkedIdentityText.Format(withoutKey)).NaturalKey.ShouldBeNull();

        all.Where(x => x.ProjectionName == keyless(guids)).ShouldAllBe(x => x.NaturalKey == null);

        // Filtered to the keyed projection alone, the same
        (await parkedAsync(store, keyed(guids)))
            .Single(x => x.IdentityText == ParkedIdentityText.Format(withKey)).NaturalKey.ShouldBe("ORD-1001");
    }

    [Theory]
    [MemberData(nameof(StreamIdentities))]
    public async Task under_conjoined_tenancy_each_tenants_parked_row_gets_its_own_tenants_natural_key(string kind)
    {
        var guids = kind == "guid";
        var store = await aStoreAsync($"parked_nk_conjoined_{kind}", guids, conjoined: true);

        // The same stream identity in two tenants, with a different natural key in each
        object stream = guids ? Guid.NewGuid() : $"order/{Guid.NewGuid():N}";
        await aPoisonedStreamAsync(store, stream, "red", "RED-1");
        await aPoisonedStreamAsync(store, stream, "blue", "BLUE-1");
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        var rows = await parkedAsync(store, keyed(guids));
        rows.Count.ShouldBe(2);
        rows.Single(x => x.TenantId == "red").NaturalKey.ShouldBe("RED-1");
        rows.Single(x => x.TenantId == "blue").NaturalKey.ShouldBe("BLUE-1");

        (await parkedAsync(store, keyless(guids))).ShouldAllBe(x => x.NaturalKey == null);
    }

    [Fact]
    public async Task a_read_model_with_a_mapped_key_shows_the_key_its_source_keeps()
    {
        var store = await aStoreAsync("parked_nk_mapped", guids: true, conjoined: false, mapped: true);

        var stream = Guid.NewGuid();
        await aPoisonedStreamAsync(store, stream, null, "ORD-3003");
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        (await parkedAsync(store, nameof(ParkedMappedGuidCounter)))
            .Single(x => x.IdentityText == ParkedIdentityText.Format(stream)).NaturalKey.ShouldBe("ORD-3003");
    }

    [Fact]
    public async Task projections_parked_shows_the_natural_key_column()
    {
        var store = await aStoreAsync("parked_nk_cli", guids: true, conjoined: false);

        var withKey = Guid.NewGuid();
        var withoutKey = Guid.NewGuid();
        await aPoisonedStreamAsync(store, withKey, null, "ORD-2002");
        await aPoisonedStreamAsync(store, withoutKey, null, null);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        // The keyed projection: the keyed stream's row shows its key
        var (exitCode, output) = await runAsync("--projection", nameof(ParkedKeyedGuidCounter));
        exitCode.ShouldBe(0, output);
        output.ShouldContain("Natural Key");
        lineFor(output, withKey).ShouldContain("ORD-2002");
        lineFor(output, withoutKey).ShouldNotContain("ORD-2002");

        // The keyless projection: the same streams, and no key on either
        var (keylessExit, keyless) = await runAsync("--projection", nameof(ParkedGuidCounter));
        keylessExit.ShouldBe(0, keyless);
        lineFor(keyless, withKey).ShouldNotContain("ORD-2002");
        lineFor(keyless, withoutKey).ShouldNotContain("ORD-2002");
    }

    private static string lineFor(string output, Guid stream)
        => output.Split('\n').Single(x => x.Contains(ParkedIdentityText.Format(stream)));

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
                .ConfigureServices(services => services.AddPolecat(_configure))
                .RunJasperFxCommands(["projections", "parked", ..arguments]);

            return (exitCode, writer.ToString());
        }
        finally
        {
            AnsiConsole.Console = original;
        }
    }
}
