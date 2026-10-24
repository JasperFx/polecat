using JasperFx;
using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.ExplorerApi;

/// <summary>
///     #819 (CritterWatch#1464): on a conjoined store with <see cref="StoreOptions.DefaultTenantUsageEnabled" />
///     off, the event store explorer reads opened the store's DEFAULT query session to run raw SQL over
///     <c>pc_streams</c> / <c>pc_events</c>, and the tenant guard refused that session before any SQL ran —
///     even though every one of these reads scopes itself with its own <c>tenant_id</c> predicate (or
///     deliberately none). Marten answers the store-wide listing with every tenant's streams.
/// </summary>
public class explorer_reads_with_default_tenant_disabled_tests
{
    private const string Schema = "explorer_no_default";

    private static async Task<DocumentStore> CreateSeededStoreAsync(Guid shared, CancellationToken ct)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.DefaultTenantUsageEnabled = false;
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: ct);

        // Every test in this class shares the schema, so each seeds streams under its own id and
        // asserts only on those.
        await using (var t1 = store.LightweightSession("t1"))
        {
            t1.Events.StartStream(shared, new QuestStarted("t1-a"), new QuestStarted("t1-b"));
            await t1.SaveChangesAsync(ct);
        }

        await using (var t2 = store.LightweightSession("t2"))
        {
            t2.Events.StartStream(shared, new QuestStarted("t2-a"));
            await t2.SaveChangesAsync(ct);
        }

        return store;
    }

    [Fact]
    public async Task store_wide_recent_streams_lists_every_tenant()
    {
        var ct = TestContext.Current.CancellationToken;
        var shared = Guid.NewGuid();
        await using var store = await CreateSeededStoreAsync(shared, ct);

        var recent = await ((IEventStore)store).GetRecentStreamsAsync(100, null, ct);

        recent.Where(x => x.StreamId == shared.ToString()).Select(x => x.TenantId).OrderBy(x => x)
            .ShouldBe(["t1", "t2"]);
    }

    [Fact]
    public async Task tenant_scoped_recent_streams_read_without_a_default_session()
    {
        var ct = TestContext.Current.CancellationToken;
        var shared = Guid.NewGuid();
        await using var store = await CreateSeededStoreAsync(shared, ct);

        var recent = await ((IEventStore)store).GetRecentStreamsAsync(100, "t2", ct);

        recent.ShouldContain(x => x.StreamId == shared.ToString());
        recent.ShouldAllBe(x => x.TenantId == "t2");
    }

    [Fact]
    public async Task tenant_scoped_stream_read_without_a_default_session()
    {
        var ct = TestContext.Current.CancellationToken;
        var shared = Guid.NewGuid();
        await using var store = await CreateSeededStoreAsync(shared, ct);

        var events = new List<JasperFx.Descriptors.EventRecord>();
        await foreach (var e in ((IEventStore)store).ReadStreamAsync(shared.ToString(), "t1", ct))
        {
            events.Add(e);
        }

        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task tenant_scoped_stream_metadata_without_a_default_session()
    {
        var ct = TestContext.Current.CancellationToken;
        var shared = Guid.NewGuid();
        await using var store = await CreateSeededStoreAsync(shared, ct);

        var metadata = await ((IEventStore)store).GetStreamMetadataAsync(shared.ToString(), "t1", ct);

        metadata.ShouldNotBeNull();
        metadata.Version.ShouldBe(2);
        metadata.TenantId.ShouldBe("t1");
    }

    [Fact]
    public async Task store_wide_stream_read_without_a_default_session()
    {
        var ct = TestContext.Current.CancellationToken;
        var shared = Guid.NewGuid();
        await using var store = await CreateSeededStoreAsync(shared, ct);

        // The tenant-less read is the documented union across tenants (#782).
        var events = new List<JasperFx.Descriptors.EventRecord>();
        await foreach (var e in ((IEventStore)store).ReadStreamAsync(shared.ToString(), null, ct))
        {
            events.Add(e);
        }

        events.Count.ShouldBe(3);
    }
}
