using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Linq;
using Polecat.Metadata;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

public record RollupA;

public record RollupB;

/// <summary>One document per TENANT rather than per stream — the roll-up shape.</summary>
public class TenantRollup
{
    [Identity]
    public string TenantId { get; set; } = "";
    public int ACount { get; set; }
    public int BCount { get; set; }
}

public partial class TenantRollupProjection : MultiStreamProjection<TenantRollup, string>
{
    public TenantRollupProjection() => RollUpByTenant();

    public void Apply(TenantRollup state, RollupA _) => state.ACount++;
    public void Apply(TenantRollup state, RollupB _) => state.BCount++;
}

/// <summary>
///     #681 — multi-stream roll-up across tenants, <c>RollUpByTenant()</c>.
/// </summary>
/// <remarks>
///     <para>
///         The triage on #681 called this a feature gap in Polecat, and that was <b>wrong</b>:
///         <c>RollUpByTenant()</c> is declared on <c>JasperFxMultiStreamProjectionBase</c>, which
///         <see cref="MultiStreamProjection{TDoc,TId}" /> derives from, so Polecat inherits it and the
///         grouping is JasperFx's. What was missing was any assertion that it works here — which is
///         exactly the kind of claim an inherited API invites and nobody checks.
///     </para>
///     <para>
///         The roll-up is the one projection shape that deliberately collapses many streams into one
///         document per tenant, so it is also the shape where a tenancy mistake is hardest to see: the
///         document is KEYED by tenant id, so a slicing bug produces plausible-looking documents with
///         the wrong totals rather than a missing or duplicated row. The counts below are therefore all
///         different from each other and from their sum.
///     </para>
///     <para>
///         ⚠️ <b>The roll-up documents are stored under the DEFAULT tenant</b>, not under the tenants
///         they summarize, and that is by design rather than a Polecat quirk:
///         <c>JasperFx.Events.Grouping.TenantRollupSlicer</c> builds its slice group with
///         <c>StorageConstants.DefaultTenantId</c> for every store. It has to — a roll-up is a
///         cross-tenant view, and filing each summary inside the tenant it describes would make the set
///         unreadable from any one session. Surprising enough given the method's name that the last
///         fact here pins it explicitly; the first draft of this test asserted the opposite and failed.
///     </para>
/// </remarks>
public class roll_up_by_tenant_tests : IAsyncLifetime
{
    private const string Schema = "rollup_by_tenant";

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => await TestSchema.DropSchemaTablesAsync(Schema);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore()
        => DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Projections.Add<TenantRollupProjection>(ProjectionLifecycle.Async);
        });

    [Fact]
    public async Task totals_are_rolled_up_per_tenant_across_many_streams()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        // Three tenants, several streams each, and a different A/B mix per tenant so no two expected
        // documents are alike and none equals the store-wide total (A: 1/3/4, sum 8).
        await AppendAsync(store, "one", [new RollupA(), new RollupB()], [new RollupB(), new RollupB()]);
        await AppendAsync(store, "two", [new RollupA(), new RollupA()], [new RollupB(), new RollupA()]);
        await AppendAsync(store, "three", [new RollupA(), new RollupA()], [new RollupA(), new RollupA()]);

        using (var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync())
        {
            await daemon.StartAllAsync();
            await daemon.WaitForNonStaleData(TimeSpan.FromSeconds(60));
        }

        await AssertRollupAsync(store, "one", aCount: 1, bCount: 3);
        await AssertRollupAsync(store, "two", aCount: 3, bCount: 1);
        await AssertRollupAsync(store, "three", aCount: 4, bCount: 0);
    }

    [Fact]
    public async Task the_roll_up_documents_live_under_the_default_tenant()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        await AppendAsync(store, "one", [new RollupA()]);
        await AppendAsync(store, "two", [new RollupB()]);

        using (var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync())
        {
            await daemon.StartAllAsync();
            await daemon.WaitForNonStaleData(TimeSpan.FromSeconds(60));
        }

        // Both summaries are visible from ONE session, which is the whole reason the slicer files them
        // under the default tenant — and is also why they are NOT reachable from the tenants they
        // describe. Asserted in both directions so a future change to TenantRollupSlicer cannot move
        // them silently.
        await using (var defaultTenant = store.QuerySession())
        {
            var all = await defaultTenant.Query<TenantRollup>().ToListAsync(Token);
            all.Select(x => x.TenantId).OrderBy(x => x).ShouldBe(["one", "two"]);
        }

        await using var tenantOne = store.QuerySession(new SessionOptions { TenantId = "one" });
        (await tenantOne.LoadAsync<TenantRollup>("one", Token)).ShouldBeNull();
    }

    private static async Task AppendAsync(DocumentStore store, string tenantId, params object[][] streams)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenantId });
        foreach (var events in streams)
        {
            session.Events.StartStream(Guid.NewGuid(), events);
        }

        await session.SaveChangesAsync(Token);
    }

    private static async Task AssertRollupAsync(DocumentStore store, string tenantId, int aCount, int bCount)
    {
        // Read from the DEFAULT tenant: see the note on the class about where TenantRollupSlicer files
        // these.
        await using var query = store.QuerySession();
        var rollup = await query.LoadAsync<TenantRollup>(tenantId, Token);

        rollup.ShouldNotBeNull($"no roll-up document for tenant '{tenantId}'");
        rollup!.ACount.ShouldBe(aCount);
        rollup.BCount.ShouldBe(bCount);
    }
}
