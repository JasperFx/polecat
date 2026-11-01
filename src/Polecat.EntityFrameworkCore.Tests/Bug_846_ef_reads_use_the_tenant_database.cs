using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Polecat.Storage;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     Reads the ef_orders row the session's own tenant owns through the DbContext it was handed, so
///     the live-aggregation route, which only ever sees a DbContext the projection built for itself,
///     has something to get wrong. <c>OrderShipped</c> on its own folds from whatever row that finds.
/// </summary>
public class OrderLookupAggregate : EfCoreSingleStreamProjection<OrderLookup, TestDbContext>
{
    public OrderLookupAggregate()
    {
        IncludeType<OrderShipped>();
    }

    protected override OrderLookup? ApplyEvent(OrderLookup? snapshot, Guid identity, IEvent @event,
        TestDbContext dbContext, IQuerySession session)
    {
        if (@event.Data is not OrderShipped shipped) return snapshot;

        var order = dbContext.Orders.AsNoTracking().FirstOrDefault(x => x.Id == shipped.OrderId);
        return new OrderLookup { Id = identity, CustomerName = order?.CustomerName };
    }
}

public class OrderLookup
{
    public Guid Id { get; set; }
    public string? CustomerName { get; set; }
}

/// <summary>
///     Reads the row an earlier event projected, through the DbContext, so a per-event projection
///     that follows up on its own state has to reach the database the row is in.
/// </summary>
public class OrderDetailFollowUpProjection : EfCoreEventProjection<TestDbContext>
{
    public OrderDetailFollowUpProjection()
    {
        IncludeType<OrderPlaced>();
        IncludeType<OrderShipped>();
    }

    public int FollowUpsThatFoundTheirRow { get; private set; }

    protected override async Task ProjectAsync(IEvent @event, TestDbContext dbContext,
        IDocumentOperations operations, CancellationToken token)
    {
        switch (@event.Data)
        {
            case OrderPlaced placed:
                dbContext.OrderDetails.Add(new OrderDetail
                {
                    Id = placed.OrderId,
                    CustomerName = placed.CustomerName,
                    TotalAmount = placed.Amount,
                    ItemCount = placed.Items,
                    Status = "Placed"
                });
                break;

            case OrderShipped shipped:
                var detail = await dbContext.OrderDetails.FindAsync([shipped.OrderId], token)
                             ?? throw new InvalidOperationException(
                                 $"No ef_order_details row for {shipped.OrderId} in the database this read reached");

                FollowUpsThatFoundTheirRow++;
                detail.IsShipped = true;
                detail.Status = "Shipped";
                break;
        }
    }
}

/// <summary>
///     Three databases, so a read that lands on the store's top-level connection string can be told
///     apart from one that lands on the tenant's database, and neither is the database the write
///     goes to by accident.
/// </summary>
public class Bug_846_TenantDatabases : IAsyncLifetime
{
    public static readonly string Main = ConnectionSource.Scoped("ef846_main");
    public static readonly string TenantA = ConnectionSource.Scoped("ef846_a");
    public static readonly string TenantB = ConnectionSource.Scoped("ef846_b");

    private static readonly string[] All = [Main, TenantA, TenantB];

    public async ValueTask InitializeAsync()
    {
        await DropAsync();

        await using (var conn = new SqlConnection(ConnectionSource.MasterConnectionString))
        {
            await conn.OpenAsync();
            foreach (var db in All)
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];";
                await cmd.ExecuteNonQueryAsync();
            }
        }

        SqlConnection.ClearAllPools();

        // The EF model owns these tables, not Polecat, so each database needs its own copy for a
        // misdirected read to find an EMPTY table rather than fail on a missing one. An empty table
        // is the silent version of this bug, which is the one that matters.
        foreach (var db in All)
        {
            await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionStringFor(db));
        }
    }

    public ValueTask DisposeAsync() => new(DropAsync());

    private static async Task DropAsync()
    {
        SqlConnection.ClearAllPools();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in All)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF DB_ID('{db}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{db}];
                END
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}

/// <summary>
///     #846. <c>EfCoreProjectionExtensions</c> built the DbContext's placeholder from
///     <see cref="StoreOptions.ConnectionString" /> while the writes went through the session's
///     tenant connection, so under database-per-tenant tenancy an EF projection read one database and
///     wrote another. A follow-up event found no row, the fold started from null, and the update
///     either vanished or became a primary key violation; under master-table tenancy the reads went
///     to the registry database.
/// </summary>
public class Bug_846_ef_reads_use_the_tenant_database : IClassFixture<Bug_846_TenantDatabases>
{
    private const string TenantA = "tenant-a";
    private const string TenantB = "tenant-b";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string ConnectionFor(string database) => ConnectionSource.ConnectionStringFor(database);

    private static void Configure(StoreOptions opts, string schema)
    {
        opts.ConnectionString = ConnectionFor(Bug_846_TenantDatabases.Main);
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.DatabaseSchemaName = schema;
    }

    private static void AddStaticTenants(StoreOptions opts)
    {
        opts.MultiTenantedDatabases(tenancy =>
        {
            tenancy.AddTenant(TenantA, ConnectionFor(Bug_846_TenantDatabases.TenantA));
            tenancy.AddTenant(TenantB, ConnectionFor(Bug_846_TenantDatabases.TenantB));
        });
    }

    private static DocumentStore StoreWithAggregate(ProjectionLifecycle lifecycle, string schema)
    {
        return DocumentStore.For(opts =>
        {
            Configure(opts, schema);
            AddStaticTenants(opts);
            opts.Projections.Add<OrderAggregate, Order, TestDbContext>(opts, new OrderAggregate(), lifecycle);
        });
    }

    private static async Task ApplyToAllAsync(DocumentStore store)
    {
        foreach (var database in store.Options.Tenancy!.AllDatabases())
        {
            await database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        }
    }

    private static async Task<bool?> IsShippedAsync(string database, Guid orderId)
    {
        await using var conn = new SqlConnection(ConnectionFor(database));
        await conn.OpenAsync(Ct);
        await using var cmd = new SqlCommand("SELECT is_shipped FROM ef_orders WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("@id", orderId);
        return await cmd.ExecuteScalarAsync(Ct) as bool?;
    }

    private static async Task ExecuteAsync(string database, string sql, params (string Name, object Value)[] parameters)
    {
        await using var conn = new SqlConnection(ConnectionFor(database));
        await conn.OpenAsync(Ct);
        await using var cmd = new SqlCommand(sql, conn);
        foreach (var (name, value) in parameters) cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task RunDaemonAsync(DocumentStore store, string tenantId)
    {
        using var daemon = await store.BuildProjectionDaemonAsync(tenantId);
        await daemon.StartAllAsync();
        await daemon.WaitForNonStaleData(30.Seconds());
        await daemon.StopAllAsync();
    }

    /// <summary>
    ///     The core claim. The tenant here is deliberately not the first one registered, and the
    ///     store's top-level connection string is a third database, so no routing accident lands on
    ///     the right one.
    /// </summary>
    [Fact]
    public async Task inline_follow_up_event_finds_the_row_in_the_tenant_database()
    {
        using var store = StoreWithAggregate(ProjectionLifecycle.Inline, "ef846_inline");
        await ApplyToAllAsync(store);

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "tenant b", 10m, 1));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.Append(orderId, new OrderShipped(orderId));
            await session.SaveChangesAsync(Ct);
        }

        (await IsShippedAsync(Bug_846_TenantDatabases.TenantB, orderId)).ShouldBe(true);
        (await IsShippedAsync(Bug_846_TenantDatabases.Main, orderId)).ShouldBeNull();
    }

    /// <summary>
    ///     The inverse of the vanishing update, and the worse direction: the same id already exists
    ///     in the database the read used to reach, so the fold silently starts from another
    ///     database's row.
    /// </summary>
    /// <remarks>
    ///     That row has to be already shipped for this to bite. EF writes only the columns it saw
    ///     change, so a fold that changes something leaves the foreign values behind; a fold that
    ///     changes nothing reaches <c>AddOrUpdateUnsafe</c> as <c>Unchanged</c>, which forces
    ///     <c>Modified</c>, and then every column of the other database's row is written into this
    ///     tenant's.
    /// </remarks>
    [Fact]
    public async Task an_inline_fold_never_starts_from_a_row_in_another_database()
    {
        using var store = StoreWithAggregate(ProjectionLifecycle.Inline, "ef846_inline_foreign");
        await ApplyToAllAsync(store);

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "the real customer", 10m, 1));
            await session.SaveChangesAsync(Ct);
        }

        await ExecuteAsync(Bug_846_TenantDatabases.Main,
            """
            INSERT INTO ef_orders (id, customer_name, total_amount, item_count, is_shipped, is_cancelled)
            VALUES (@id, 'somebody else', 99, 9, 1, 0)
            """, ("@id", orderId));

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.Append(orderId, new OrderShipped(orderId));
            await session.SaveChangesAsync(Ct);
        }

        var row = await QueryRowAsync(Bug_846_TenantDatabases.TenantB,
            "SELECT customer_name, is_shipped FROM ef_orders WHERE id = @id", orderId);

        row.ShouldNotBeNull();
        row["customer_name"].ShouldBe("the real customer");
        row["is_shipped"].ShouldBe(true);
    }

    /// <summary>
    ///     The daemon reads outside any transaction, so nothing about the session's connection helps
    ///     it: this half is only right if the reads are aimed at the database the batch is working.
    /// </summary>
    [Fact]
    public async Task async_follow_up_event_finds_the_row_in_the_tenant_database()
    {
        using var store = StoreWithAggregate(ProjectionLifecycle.Async, "ef846_async");
        await ApplyToAllAsync(store);

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "tenant b", 10m, 1));
            await session.SaveChangesAsync(Ct);
        }

        await RunDaemonAsync(store, TenantB);

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.Append(orderId, new OrderShipped(orderId));
            await session.SaveChangesAsync(Ct);
        }

        await RunDaemonAsync(store, TenantB);

        (await IsShippedAsync(Bug_846_TenantDatabases.TenantB, orderId)).ShouldBe(true);
    }

    /// <summary>
    ///     <c>EfCoreEventProjection</c> builds its own context on a separate path from the storage
    ///     factory, so it had the same bug independently and throws here when its read misses.
    /// </summary>
    [Fact]
    public async Task an_inline_event_projection_reads_the_tenant_database()
    {
        using var store = DocumentStore.For(opts =>
        {
            Configure(opts, "ef846_ep_inline");
            AddStaticTenants(opts);
            opts.Projections.Add<OrderDetailFollowUpProjection, TestDbContext>(
                opts, new OrderDetailFollowUpProjection(), ProjectionLifecycle.Inline);
        });

        await ApplyToAllAsync(store);

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "tenant b", 10m, 1));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.Append(orderId, new OrderShipped(orderId));
            await session.SaveChangesAsync(Ct);
        }

        var row = await QueryRowAsync(Bug_846_TenantDatabases.TenantB,
            "SELECT status FROM ef_order_details WHERE id = @id", orderId);

        row.ShouldNotBeNull();
        row["status"].ShouldBe("Shipped");
    }

    /// <summary>
    ///     The same read under the daemon, which reaches this projection with a session and no
    ///     transaction, and the write it goes on to make.
    /// </summary>
    /// <remarks>
    ///     This used to stop at the read, because the daemon disposes an event projection's session
    ///     as soon as <c>ApplyAsync</c> returns and that drained the participant before the batch
    ///     collected it, so the write was never flushed on any tenancy. #867 fixed that, so the row
    ///     has to have changed in the database the read reached, and only there.
    /// </remarks>
    [Fact]
    public async Task an_async_event_projection_reads_the_tenant_database()
    {
        var projection = new OrderDetailFollowUpProjection();

        using var store = DocumentStore.For(opts =>
        {
            Configure(opts, "ef846_ep_async");
            AddStaticTenants(opts);
            opts.Projections.Add<OrderDetailFollowUpProjection, TestDbContext>(
                opts, projection, ProjectionLifecycle.Async);
        });

        await ApplyToAllAsync(store);

        var orderId = Guid.NewGuid();

        await ExecuteAsync(Bug_846_TenantDatabases.TenantB,
            """
            INSERT INTO ef_order_details (id, customer_name, total_amount, item_count, is_shipped, status)
            VALUES (@id, 'tenant b', 10, 1, 0, 'Placed')
            """, ("@id", orderId));

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.StartStream(orderId, new OrderShipped(orderId));
            await session.SaveChangesAsync(Ct);
        }

        await RunDaemonAsync(store, TenantB);

        projection.FollowUpsThatFoundTheirRow.ShouldBe(1);

        var row = await QueryRowAsync(Bug_846_TenantDatabases.TenantB,
            "SELECT status, is_shipped FROM ef_order_details WHERE id = @id", orderId);

        row.ShouldNotBeNull();
        row["status"].ShouldBe("Shipped");
        row["is_shipped"].ShouldBe(true);

        (await QueryRowAsync(Bug_846_TenantDatabases.Main,
            "SELECT status FROM ef_order_details WHERE id = @id", orderId)).ShouldBeNull();
    }

    /// <summary>
    ///     #867: a new row, under database-per-tenant tenancy. The batch's transaction belongs to the
    ///     database the daemon is working, so each tenant's row has to land in that tenant's database
    ///     and nowhere else, and neither of them in the store's own.
    /// </summary>
    [Fact]
    public async Task an_async_event_projection_writes_each_tenants_row_to_its_own_database()
    {
        using var store = DocumentStore.For(opts =>
        {
            Configure(opts, "ef867_ep_async_db");
            AddStaticTenants(opts);
            opts.Projections.Add<OrderDetailFollowUpProjection, TestDbContext>(
                opts, new OrderDetailFollowUpProjection(), ProjectionLifecycle.Async);
        });

        await ApplyToAllAsync(store);

        var orderForA = Guid.NewGuid();
        var orderForB = Guid.NewGuid();

        await using (var session = store.LightweightSession(TenantA))
        {
            session.Events.StartStream(orderForA, new OrderPlaced(orderForA, "tenant a", 10m, 1));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.StartStream(orderForB, new OrderPlaced(orderForB, "tenant b", 20m, 2));
            await session.SaveChangesAsync(Ct);
        }

        await RunDaemonAsync(store, TenantA);
        await RunDaemonAsync(store, TenantB);

        const string sql = "SELECT customer_name FROM ef_order_details WHERE id = @id";

        (await QueryRowAsync(Bug_846_TenantDatabases.TenantA, sql, orderForA)).ShouldNotBeNull()
            ["customer_name"].ShouldBe("tenant a");
        (await QueryRowAsync(Bug_846_TenantDatabases.TenantB, sql, orderForB)).ShouldNotBeNull()
            ["customer_name"].ShouldBe("tenant b");

        (await QueryRowAsync(Bug_846_TenantDatabases.TenantB, sql, orderForA)).ShouldBeNull();
        (await QueryRowAsync(Bug_846_TenantDatabases.TenantA, sql, orderForB)).ShouldBeNull();
        (await QueryRowAsync(Bug_846_TenantDatabases.Main, sql, orderForA)).ShouldBeNull();
        (await QueryRowAsync(Bug_846_TenantDatabases.Main, sql, orderForB)).ShouldBeNull();
    }

    /// <summary>
    ///     Live aggregation has no storage at all: the projection builds a context of its own from
    ///     the connection string it captured at registration, against a query session.
    /// </summary>
    [Fact]
    public async Task live_aggregation_reads_the_tenant_database()
    {
        using var store = DocumentStore.For(opts =>
        {
            Configure(opts, "ef846_live");
            AddStaticTenants(opts);
            opts.Projections.Add<OrderLookupAggregate, OrderLookup, TestDbContext>(
                opts, new OrderLookupAggregate(), ProjectionLifecycle.Live);
        });

        await ApplyToAllAsync(store);

        var orderId = Guid.NewGuid();

        await ExecuteAsync(Bug_846_TenantDatabases.TenantB,
            """
            INSERT INTO ef_orders (id, customer_name, total_amount, item_count, is_shipped, is_cancelled)
            VALUES (@id, 'found in tenant b', 1, 1, 0, 0)
            """, ("@id", orderId));

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.StartStream(orderId, new OrderShipped(orderId));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = store.QuerySession(TenantB);
        var lookup = await query.Events.AggregateStreamAsync<OrderLookup>(orderId, token: Ct);

        lookup.ShouldNotBeNull();
        lookup.CustomerName.ShouldBe("found in tenant b");
    }

    /// <summary>
    ///     Under master-table tenancy the store's own connection string IS the registry database, so
    ///     the old reads went there and the ef_ tables are not even a thing that database has.
    /// </summary>
    [Fact]
    public async Task master_table_tenancy_reads_the_tenant_database()
    {
        var masterConnection = ConnectionFor(Bug_846_TenantDatabases.Main);

        using var store = DocumentStore.For(opts =>
        {
            Configure(opts, "ef846_master");
            opts.MultiTenantedMasterTable(masterConnection);
            opts.Projections.Add<OrderAggregate, Order, TestDbContext>(
                opts, new OrderAggregate(), ProjectionLifecycle.Inline);
        });

        var tenancy = (MasterTableTenancy)store.Options.Tenancy!;
        await tenancy.AddDatabaseRecordAsync(TenantB, ConnectionFor(Bug_846_TenantDatabases.TenantB), Ct);
        await store.Options.Tenancy!.GetDatabase(TenantB).ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "tenant b", 10m, 1));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession(TenantB))
        {
            session.Events.Append(orderId, new OrderShipped(orderId));
            await session.SaveChangesAsync(Ct);
        }

        (await IsShippedAsync(Bug_846_TenantDatabases.TenantB, orderId)).ShouldBe(true);
    }

    private static async Task<Dictionary<string, object?>?> QueryRowAsync(string database, string sql, Guid id)
    {
        await using var conn = new SqlConnection(ConnectionFor(database));
        await conn.OpenAsync(Ct);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", id);

        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        if (!await reader.ReadAsync(Ct)) return null;

        var row = new Dictionary<string, object?>();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
        }

        return row;
    }
}
