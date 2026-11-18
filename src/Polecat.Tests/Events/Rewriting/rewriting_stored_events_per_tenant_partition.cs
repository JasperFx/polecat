using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events.Rewriting;

/// <summary>
///     #1034 (marten#5643) under per-tenant event partitions, where every tenant numbers its events from 1, so the
///     rewrite's keyset has to be (sequence, tenant).
/// </summary>
[Collection("tenant-partitioning")]
public class rewriting_stored_events_per_tenant_partition : IAsyncLifetime
{
    private const string Schema = "pt_rewrite";

    public record ParcelSent(string To);

    private DocumentStore _store = null!;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);

        _store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Events.AddEventType<ParcelSent>();
        });

        await _store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    public ValueTask DisposeAsync()
    {
        _store.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task appendAsync(params string[] tenants)
    {
        await _store.Advanced.AddPolecatManagedTenantsAsync(Ct, tenants);

        foreach (var tenant in tenants)
        {
            await using var session = _store.LightweightSession(tenant);
            session.Events.StartStream(Guid.NewGuid(), new ParcelSent($"{tenant} 1"), new ParcelSent($"{tenant} 2"));
            await session.SaveChangesAsync(Ct);
        }
    }

    private async Task<string[]> destinationsOfAsync(string tenant)
    {
        await using var query = _store.QuerySession(tenant);
        var events = await query.Events.QueryAllRawEvents().ToListAsync(Ct);
        return events.Select(e => ((ParcelSent)e.Data).To).OrderBy(x => x).ToArray();
    }

    [Fact]
    public async Task every_tenants_events_are_rewritten_although_they_share_sequence_numbers()
    {
        string[] tenants = ["alpha", "beta", "gamma"];
        await appendAsync(tenants);

        // One event per page, so every page boundary falls between two tenants' same-numbered events
        var result = await _store.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.Transform<ParcelSent>(e => e with { To = e.To.ToUpperInvariant() });
            x.BatchSize = 1;
        }, Ct);

        result.Databases.Single().Rewritten.ShouldBe(6);

        foreach (var tenant in tenants)
        {
            (await destinationsOfAsync(tenant))
                .ShouldBe([$"{tenant.ToUpperInvariant()} 1", $"{tenant.ToUpperInvariant()} 2"]);
        }
    }

    [Fact]
    public async Task one_tenant_is_rewritten_alone_and_the_others_keep_their_rows()
    {
        await appendAsync("alpha", "beta");

        var result = await _store.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.Transform<ParcelSent>(e => e with { To = e.To.ToUpperInvariant() });
            x.ForTenant("beta");
            x.BatchSize = 1;
        }, Ct);

        result.Databases.Single().Visited.ShouldBe(2);
        result.Databases.Single().Rewritten.ShouldBe(2);
        result.Databases.Single().Cursor.TenantId.ShouldBe("beta");

        (await destinationsOfAsync("alpha")).ShouldBe(["alpha 1", "alpha 2"]);
        (await destinationsOfAsync("beta")).ShouldBe(["BETA 1", "BETA 2"]);
    }
}
