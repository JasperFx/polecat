using JasperFx;
using JasperFx.Descriptors;
using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.ExplorerApi;

/// <summary>
///     #782 / jasperfx#503 — on a conjoined multi-tenant Polecat store the same stream id can
///     live under two tenants. The tenant-less explorer read returns an ambiguous union of both;
///     the tenant-scoped <c>IEventStore.ReadStreamAsync(streamId, tenantId, ct)</c> /
///     <c>GetRecentStreamsAsync(count, tenantId, ct)</c> overloads must isolate each tenant's
///     slice via a <c>tenant_id</c> predicate.
/// </summary>
public class event_store_explorer_tenant_isolation_tests
{
    private const string Schema = "explorer_tenant_iso";

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });
    }

    [Fact]
    public async Task read_stream_is_isolated_per_tenant_on_conjoined_store()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Same stream id under two tenants — the exact cross-tenant ambiguity #782 closes.
        var streamId = Guid.NewGuid();

        await using (var a = store.LightweightSession(new SessionOptions { TenantId = "tenant-a" }))
        {
            a.Events.StartStream(streamId, new QuestStarted("Alice-1"), new QuestStarted("Alice-2"));
            await a.SaveChangesAsync();
        }

        await using (var b = store.LightweightSession(new SessionOptions { TenantId = "tenant-b" }))
        {
            b.Events.StartStream(streamId, new QuestStarted("Bob-1"));
            await b.SaveChangesAsync();
        }

        IEventStore explorer = store;

        var tenantA = new List<EventRecord>();
        await foreach (var e in explorer.ReadStreamAsync(streamId.ToString(), "tenant-a", CancellationToken.None))
            tenantA.Add(e);

        var tenantB = new List<EventRecord>();
        await foreach (var e in explorer.ReadStreamAsync(streamId.ToString(), "tenant-b", CancellationToken.None))
            tenantB.Add(e);

        tenantA.Count.ShouldBe(2);
        tenantB.Count.ShouldBe(1);

        // The tenant-less overload is unchanged: it still reads across every tenant, so it sees
        // the union — the ambiguity the tenant-scoped read exists to resolve.
        var union = new List<EventRecord>();
        await foreach (var e in explorer.ReadStreamAsync(streamId.ToString(), CancellationToken.None))
            union.Add(e);
        union.Count.ShouldBe(3);
    }

    [Fact]
    public async Task recent_streams_is_isolated_per_tenant_on_conjoined_store()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamA = Guid.NewGuid();
        var streamB = Guid.NewGuid();

        await using (var a = store.LightweightSession(new SessionOptions { TenantId = "tenant-a" }))
        {
            a.Events.StartStream(streamA, new QuestStarted("Alice"));
            await a.SaveChangesAsync();
        }

        await using (var b = store.LightweightSession(new SessionOptions { TenantId = "tenant-b" }))
        {
            b.Events.StartStream(streamB, new QuestStarted("Bob"));
            await b.SaveChangesAsync();
        }

        IEventStore explorer = store;

        var tenantA = await explorer.GetRecentStreamsAsync(100, "tenant-a", CancellationToken.None);
        tenantA.ShouldContain(s => s.StreamId == streamA.ToString());
        tenantA.ShouldNotContain(s => s.StreamId == streamB.ToString());

        var tenantB = await explorer.GetRecentStreamsAsync(100, "tenant-b", CancellationToken.None);
        tenantB.ShouldContain(s => s.StreamId == streamB.ToString());
        tenantB.ShouldNotContain(s => s.StreamId == streamA.ToString());
    }
}
