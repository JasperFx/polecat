using JasperFx;
using JasperFx.Events;
using JasperFx.MultiTenancy;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer.Tables.Partitioning;
using Weasel.Storage;

namespace Polecat.Tests.Events;

public record ReleasedOrdinalHappened(int Number);

/// <summary>
///     #922 / #921: removing a tenant with <c>RetainData</c> drops its per-ordinal event sequence but keeps
///     its events. When the ordinal comes back, for the same tenant or another one, the fresh sequence
///     restarted at 1 and collided with the retained rows on the <c>pc_events</c> primary key, which
///     surfaced as an <c>ExistingStreamIdCollisionException</c> for a brand new stream id.
/// </summary>
[Collection("tenant-partitioning")]
public class tenant_partition_released_ordinal_reuse_tests : IAsyncLifetime
{
    private const string Schema = "pt_released_reuse";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async Task<DocumentStore> CreateStoreAsync()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Events.AddEventType<ReleasedOrdinalHappened>();
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        return store;
    }

    private static async Task StartAsync(DocumentStore store, string tenant, Guid id, int events)
    {
        await using var session = store.LightweightSession(tenant);
        session.Events.StartStream(id, Enumerable.Range(1, events).Select(x => new ReleasedOrdinalHappened(x))
            .Cast<object>().ToArray());
        await session.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task another_tenant_taking_a_released_ordinal_with_retained_events_can_append()
    {
        using var store = await CreateStoreAsync();
        var original = Guid.NewGuid();
        await StartAsync(store, "red", original, 3);

        var ordinal = (int)(await TestSchema.QueryAsync($"SELECT TOP 1 tenant_ordinal FROM [{Schema}].[pc_events]"))
            .Single()[0];

        await store.Advanced.RemovePolecatManagedTenantsAsync(["red"], TenantDropBehavior.RetainData, Ct);
        await store.Advanced.AddPolecatManagedTenantsAsync(new Dictionary<string, int> { ["blue"] = ordinal }, Ct);

        var fresh = Guid.NewGuid();
        await StartAsync(store, "blue", fresh, 2);

        await using var query = store.QuerySession("blue");
        (await query.Events.FetchStreamAsync(fresh, token: Ct)).Count.ShouldBe(2);
    }

    [Fact]
    public async Task the_same_tenant_coming_back_on_its_ordinal_can_append()
    {
        using var store = await CreateStoreAsync();
        await StartAsync(store, "red", Guid.NewGuid(), 3);

        var ordinal = (int)(await TestSchema.QueryAsync($"SELECT TOP 1 tenant_ordinal FROM [{Schema}].[pc_events]"))
            .Single()[0];

        await store.Advanced.RemovePolecatManagedTenantsAsync(["red"], TenantDropBehavior.RetainData, Ct);
        await store.Advanced.AddPolecatManagedTenantsAsync(new Dictionary<string, int> { ["red"] = ordinal }, Ct);

        var fresh = Guid.NewGuid();
        await StartAsync(store, "red", fresh, 2);

        await using var query = store.QuerySession("red");
        (await query.Events.FetchStreamAsync(fresh, token: Ct)).Count.ShouldBe(2);
    }
}
