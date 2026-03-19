using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;

using JasperFx;

namespace Polecat.EntityFrameworkCore.Tests;

public abstract class ef_core_tenanted_single_stream_tests_base : IAsyncLifetime
{
    protected DocumentStore Store = null!;
    protected abstract ProjectionLifecycle Lifecycle { get; }

    public async Task InitializeAsync()
    {
        Store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.DatabaseSchemaName = $"efcore_ten_{Lifecycle.ToString().ToLowerInvariant()}";
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;

            opts.Projections.Add<TenantedOrderAggregate, TenantedOrder, TenantedTestDbContext>(
                opts, new TenantedOrderAggregate(), Lifecycle);
        });

        await Store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TenantedTestDbContext>(ConnectionSource.ConnectionString);
        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString, "ef_tenanted_orders");
    }

    public Task DisposeAsync()
    {
        Store?.Dispose();
        return Task.CompletedTask;
    }

    protected virtual Task WaitForProjectionAsync() => Task.CompletedTask;

    [Fact]
    public async Task tenant_id_is_written_to_ef_core_table()
    {
        var orderId = Guid.NewGuid();
        await using var session = Store.LightweightSession(new SessionOptions { TenantId = "tenant-a" });
        session.Events.StartStream(orderId,
            new OrderPlaced(orderId, "TenantUser", 100.00m, 2));
        await session.SaveChangesAsync();

        await WaitForProjectionAsync();

        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, tenant_id FROM ef_tenanted_orders WHERE id = @id",
            ("@id", orderId));

        row.ShouldNotBeNull();
        row["customer_name"].ShouldBe("TenantUser");
        row["tenant_id"].ShouldBe("tenant-a");
    }

    [Fact]
    public async Task different_tenants_get_isolated_data()
    {
        var orderId1 = Guid.NewGuid();
        var orderId2 = Guid.NewGuid();

        await using var session1 = Store.LightweightSession(new SessionOptions { TenantId = "tenant-x" });
        session1.Events.StartStream(orderId1,
            new OrderPlaced(orderId1, "UserX", 50.00m, 1));
        await session1.SaveChangesAsync();

        await WaitForProjectionAsync();

        await using var session2 = Store.LightweightSession(new SessionOptions { TenantId = "tenant-y" });
        session2.Events.StartStream(orderId2,
            new OrderPlaced(orderId2, "UserY", 75.00m, 3));
        await session2.SaveChangesAsync();

        await WaitForProjectionAsync();

        var rowX = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, tenant_id FROM ef_tenanted_orders WHERE id = @id",
            ("@id", orderId1));
        var rowY = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, tenant_id FROM ef_tenanted_orders WHERE id = @id",
            ("@id", orderId2));

        rowX.ShouldNotBeNull();
        rowX["tenant_id"].ShouldBe("tenant-x");

        rowY.ShouldNotBeNull();
        rowY["tenant_id"].ShouldBe("tenant-y");
    }

    [Fact]
    public async Task subsequent_appends_preserve_tenant_id()
    {
        var orderId = Guid.NewGuid();

        await using var session = Store.LightweightSession(new SessionOptions { TenantId = "tenant-z" });
        session.Events.StartStream(orderId,
            new OrderPlaced(orderId, "TenantZ", 200.00m, 4));
        await session.SaveChangesAsync();

        await WaitForProjectionAsync();

        await using var session2 = Store.LightweightSession(new SessionOptions { TenantId = "tenant-z" });
        session2.Events.Append(orderId, new OrderShipped(orderId));
        await session2.SaveChangesAsync();

        await WaitForProjectionAsync();

        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, is_shipped, tenant_id FROM ef_tenanted_orders WHERE id = @id",
            ("@id", orderId));

        row.ShouldNotBeNull();
        row["customer_name"].ShouldBe("TenantZ");
        ((bool)row["is_shipped"]!).ShouldBeTrue();
        row["tenant_id"].ShouldBe("tenant-z");
    }
}

public class ef_core_tenanted_inline_tests : ef_core_tenanted_single_stream_tests_base
{
    protected override ProjectionLifecycle Lifecycle => ProjectionLifecycle.Inline;
}

public class ef_core_tenanted_async_tests : ef_core_tenanted_single_stream_tests_base
{
    protected override ProjectionLifecycle Lifecycle => ProjectionLifecycle.Async;
    private IProjectionDaemon? _daemon;

    protected override async Task WaitForProjectionAsync()
    {
        if (_daemon == null)
        {
            SqlConnection.ClearAllPools();
            _daemon = (IProjectionDaemon)await Store.BuildProjectionDaemonAsync();
            await _daemon.StartAllAsync();
        }

        for (var attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                await _daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
                return;
            }
            catch (AggregateException) when (attempt < 2)
            {
                SqlConnection.ClearAllPools();
                await Task.Delay(200);
            }
        }
    }
}
