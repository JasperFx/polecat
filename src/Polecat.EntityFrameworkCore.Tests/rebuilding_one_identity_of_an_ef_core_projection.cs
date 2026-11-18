using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     #1060 (marten#6071): a per-identity rebuild clears the identity with <c>HardDeleteById</c> inside the shard's
///     own batch, and EF Core projection storage cannot. The daemon refuses the request before anything is queued,
///     rather than failing the shard's batch.
/// </summary>
public class rebuilding_one_identity_of_an_ef_core_projection
{
    private const string Schema = "ef1060_identity";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task is_refused_before_anything_is_queued()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.DatabaseSchemaName = Schema;

            opts.Projections.Add<OrderAggregate, Order, TestDbContext>(opts, new OrderAggregate(),
                ProjectionLifecycle.Async);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);
        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString, "ef_orders",
            "ef_order_summaries");

        var orderId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "identity", 10m, 1));
            await session.SaveChangesAsync(Ct);
        }

        var projectionName = store.Options.Projections.All
            .Single(x => x is IAggregateProjection { AggregateType: var type } && type == typeof(Order)).Name;

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), Ct);

        var refused = await Should.ThrowAsync<IdentityRebuildRefusedException>(() =>
            daemon.RebuildIdentityAsync(projectionName, null, orderId, Ct));

        refused.Reason.ShouldBe(IdentityRebuildRefusal.NotSupportedByStore);

        await daemon.StopAllAsync();
    }
}
