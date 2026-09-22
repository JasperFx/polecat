using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.TestUtils;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     A projection over <see cref="DisposalTrackingDbContext" />, so the leak can be counted
///     against a real projection run rather than a hand-built storage instance.
/// </summary>
public class TrackedOrderAggregate : EfCoreSingleStreamProjection<Order, DisposalTrackingDbContext>
{
    public TrackedOrderAggregate()
    {
        IncludeType<OrderPlaced>();
        IncludeType<OrderShipped>();
    }

    protected override Order? ApplyEvent(Order? snapshot, Guid identity, IEvent @event,
        DisposalTrackingDbContext dbContext, IQuerySession session)
    {
        return @event.Data switch
        {
            OrderPlaced placed => new Order
            {
                Id = placed.OrderId,
                CustomerName = placed.CustomerName,
                TotalAmount = placed.Amount,
                ItemCount = placed.Items
            },
            OrderShipped => snapshot is null ? null : Ship(snapshot),
            _ => snapshot
        };
    }

    private static Order Ship(Order order)
    {
        order.IsShipped = true;
        return order;
    }
}

/// <summary>
///     #650 (the Polecat twin of marten#5457). <c>EfCoreProjectionStorage</c> owns a
///     <c>DbContext</c> built once per tenant per batch, and nothing disposed it on any path —
///     <c>IProjectionStorage&lt;,&gt;</c> declares no disposal contract, so the type that owns the
///     context had nothing to hook.
/// </summary>
/// <remarks>
///     These count construction against disposal rather than backends in
///     <c>sys.dm_exec_sessions</c>. On Polecat's SQL Server path the placeholder connection is never
///     opened, so a backend count would have reported nothing while the context leak was fully
///     present — the connection-counting version of this test passes on unfixed code and proves
///     nothing. Each fact also asserts a context was actually BUILT, so it cannot pass vacuously by
///     never exercising the path.
/// </remarks>
public class Bug_650_efcore_dbcontext_never_disposed
{
    private static DocumentStore CreateStore(string schema)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.DatabaseSchemaName = schema;

            opts.Projections.Add<TrackedOrderAggregate, Order, DisposalTrackingDbContext>(
                opts, new TrackedOrderAggregate(), ProjectionLifecycle.Inline);
        });
    }

    [Fact]
    public async Task every_dbcontext_the_projection_storage_builds_is_disposed()
    {
        var token = TestContext.Current.CancellationToken;

        using var store = CreateStore("efcore_bug650");
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);

        DisposalTrackingDbContext.Reset();

        // Five sessions, so a leak is five contexts rather than one — the shape the reporter saw
        // was monotonic growth across batches, not a single stranded object.
        for (var i = 0; i < 5; i++)
        {
            var orderId = Guid.NewGuid();
            await using var session = store.LightweightSession();
            session.Events.StartStream(orderId, new OrderPlaced(orderId, $"Customer {i}", 10m * i, i));
            await session.SaveChangesAsync(token);
        }

        // Not vacuous: the path really did build contexts.
        DisposalTrackingDbContext.Created.ShouldBe(5);

        // And every one of them was disposed. Before #650 this read 5 created / 0 disposed.
        DisposalTrackingDbContext.Disposed.ShouldBe(DisposalTrackingDbContext.Created);
    }

    /// <summary>
    ///     The failure path. A participant that is only drained on a successful commit is the defect
    ///     marten#5228 fixed and Polecat never received — so the release has to survive a session
    ///     that throws before it ever commits.
    /// </summary>
    [Fact]
    public async Task a_session_that_never_commits_still_disposes_its_dbcontext()
    {
        var token = TestContext.Current.CancellationToken;

        using var store = CreateStore("efcore_bug650_fail");
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);

        DisposalTrackingDbContext.Reset();

        var orderId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "Never committed", 1m, 1));

            // Force the inline projection to build its storage (and therefore its DbContext), then
            // abandon the session without a successful commit.
            await session.SaveChangesAsync(token);
        }

        DisposalTrackingDbContext.Created.ShouldBeGreaterThan(0);
        DisposalTrackingDbContext.Disposed.ShouldBe(DisposalTrackingDbContext.Created);
    }
}
