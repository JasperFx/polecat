using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public record TallyRaised(int Amount);

/// <summary>Snapshotted inline. Additive, so a leak shows as a wrong total rather than a missing row.</summary>
public partial class SharedInlineTally
{
    public Guid Id { get; set; }
    public int Total { get; set; }
    public void Apply(TallyRaised e) => Total += e.Amount;
}

/// <summary>The same shape, snapshotted asynchronously — a different code path entirely.</summary>
public partial class SharedAsyncTally
{
    public Guid Id { get; set; }
    public int Total { get; set; }
    public void Apply(TallyRaised e) => Total += e.Amount;
}

/// <summary>
///     #681 — one stream id used by two tenants, through the paths that turn a stream into something
///     else: an <b>inline</b> snapshot, an <b>async</b> snapshot, and <b>archiving</b>.
/// </summary>
/// <remarks>
///     <para>
///         The shared stream id is the whole point. <c>conjoined_event_tenancy_tests</c> already covers
///         appending and reading one, but nothing covered projecting or archiving it, and those are the
///         paths where the stream id alone is the natural key: a snapshot keyed by stream id lands in a
///         document table whose own primary key must carry the tenant too, and an archive flips a
///         column on rows selected by stream id.
///     </para>
///     <para>
///         Each tenant's events are deliberately different, so an assertion cannot pass by coincidence:
///         Red totals 3 and Blue totals 100, and neither may ever see 103.
///     </para>
/// </remarks>
public class conjoined_shared_stream_id_tests : IAsyncLifetime
{
    private const string Schema = "conjoined_shared_stream";
    private static readonly Guid SharedStreamId = Guid.NewGuid();
    private const string SharedStreamKey = "shared-stream-key";

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => await TestSchema.DropSchemaTablesAsync(Schema);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore(Action<StoreOptions> configure)
        => DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            configure(opts);
        });

    private static async Task AppendAsync(IDocumentStore store, string tenantId, Guid streamId,
        params TallyRaised[] events)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenantId });
        session.Events.StartStream(streamId, events.Cast<object>().ToArray());
        await session.SaveChangesAsync(Token);
    }

    [Fact]
    public async Task an_inline_snapshot_of_a_shared_stream_id_stays_isolated_per_tenant()
    {
        using var store = CreateStore(opts =>
            opts.Projections.Snapshot<SharedInlineTally>(SnapshotLifecycle.Inline));
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        await AppendAsync(store, "Red", SharedStreamId, new TallyRaised(1), new TallyRaised(2));
        await AppendAsync(store, "Blue", SharedStreamId, new TallyRaised(100));

        await using (var red = store.QuerySession(new SessionOptions { TenantId = "Red" }))
        {
            (await red.LoadAsync<SharedInlineTally>(SharedStreamId, Token))
                .ShouldNotBeNull()!.Total.ShouldBe(3);
        }

        await using var blue = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        (await blue.LoadAsync<SharedInlineTally>(SharedStreamId, Token))
            .ShouldNotBeNull()!.Total.ShouldBe(100);
    }

    [Fact]
    public async Task an_async_snapshot_of_a_shared_stream_id_stays_isolated_per_tenant()
    {
        using var store = CreateStore(opts =>
            opts.Projections.Snapshot<SharedAsyncTally>(SnapshotLifecycle.Async));
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        await AppendAsync(store, "Red", SharedStreamId, new TallyRaised(1), new TallyRaised(2));
        await AppendAsync(store, "Blue", SharedStreamId, new TallyRaised(100));

        using (var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync())
        {
            await daemon.StartAllAsync();
            await daemon.WaitForNonStaleData(TimeSpan.FromSeconds(60));
        }

        // The daemon resolves the tenant per event rather than from a session, so this is a different
        // decision from the inline arm's and worth its own assertion.
        await using (var red = store.QuerySession(new SessionOptions { TenantId = "Red" }))
        {
            (await red.LoadAsync<SharedAsyncTally>(SharedStreamId, Token))
                .ShouldNotBeNull()!.Total.ShouldBe(3);
        }

        await using var blue = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        (await blue.LoadAsync<SharedAsyncTally>(SharedStreamId, Token))
            .ShouldNotBeNull()!.Total.ShouldBe(100);
    }

    [Fact]
    public async Task archiving_a_stream_in_one_tenant_leaves_the_same_stream_id_live_in_another()
    {
        using var store = CreateStore(_ => { });
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        await AppendAsync(store, "Red", SharedStreamId, new TallyRaised(1));
        await AppendAsync(store, "Blue", SharedStreamId, new TallyRaised(100));

        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Events.ArchiveStream(SharedStreamId);
            await red.SaveChangesAsync(Token);
        }

        await using (var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" }))
        {
            (await redQuery.Events.FetchStreamStateAsync(SharedStreamId, Token))
                .ShouldNotBeNull()!.IsArchived.ShouldBeTrue();
        }

        // Blue's stream shares the id and must be untouched — both the stream row and its events.
        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var blueState = await blueQuery.Events.FetchStreamStateAsync(SharedStreamId, Token);
        blueState.ShouldNotBeNull()!.IsArchived.ShouldBeFalse();
        (await blueQuery.Events.FetchStreamAsync(SharedStreamId, token: Token)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task archiving_is_tenant_scoped_under_string_stream_identity_too()
    {
        // The string-identity arm runs through a different storage type (StringEventStorage), so the
        // tenant predicate is written twice and only tested once without this.
        using var store = CreateStore(opts => opts.Events.StreamIdentity = StreamIdentity.AsString);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        foreach (var tenant in new[] { "Red", "Blue" })
        {
            await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
            session.Events.StartStream(SharedStreamKey, new TallyRaised(1));
            await session.SaveChangesAsync(Token);
        }

        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Events.ArchiveStream(SharedStreamKey);
            await red.SaveChangesAsync(Token);
        }

        await using (var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" }))
        {
            (await redQuery.Events.FetchStreamStateAsync(SharedStreamKey, Token))
                .ShouldNotBeNull()!.IsArchived.ShouldBeTrue();
        }

        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        (await blueQuery.Events.FetchStreamStateAsync(SharedStreamKey, Token))
            .ShouldNotBeNull()!.IsArchived.ShouldBeFalse();
        (await blueQuery.Events.FetchStreamAsync(SharedStreamKey, token: Token)).Count.ShouldBe(1);
    }
}
