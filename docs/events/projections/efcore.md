# EF Core Projections

Polecat integrates with Entity Framework Core, allowing you to use DbContext within your event projections. This is provided by the `Polecat.EntityFrameworkCore` package.

## Installation

```shell
dotnet add package Polecat.EntityFrameworkCore
```

## Single Stream Projection with EF Core

```cs
public class OrderDbContext : DbContext
{
    public DbSet<OrderReadModel> Orders { get; set; } = null!;

    public OrderDbContext(DbContextOptions<OrderDbContext> options)
        : base(options) { }
}

public class OrderEfProjection : EfCoreSingleStreamProjection<OrderReadModel, OrderDbContext>
{
    public OrderReadModel Create(OrderCreated e, OrderDbContext db)
    {
        var model = new OrderReadModel
        {
            Status = "Created",
            Amount = e.Amount
        };
        db.Orders.Add(model);
        return model;
    }

    public void Apply(OrderShipped e, OrderReadModel current, OrderDbContext db)
    {
        current.Status = "Shipped";
        current.ShippedDate = e.ShippedAt;
    }
}
```

## Multi Stream Projection with EF Core

```cs
public class CustomerEfProjection : EfCoreMultiStreamProjection<CustomerDashboard, Guid, AppDbContext>
{
    public CustomerEfProjection()
    {
        Identity<OrderCreated>(e => e.CustomerId);
    }

    public CustomerDashboard Create(OrderCreated e, AppDbContext db)
    {
        return new CustomerDashboard { TotalOrders = 1, TotalSpent = e.Amount };
    }

    public void Apply(OrderCreated e, CustomerDashboard current, AppDbContext db)
    {
        current.TotalOrders++;
        current.TotalSpent += e.Amount;
    }
}
```

## Event Projection with EF Core

```cs
public class AuditEfProjection : EfCoreEventProjection
{
    public void Project(IEvent<OrderCreated> @event, AppDbContext db)
    {
        db.AuditEntries.Add(new AuditEntry
        {
            Action = "OrderCreated",
            Timestamp = @event.Timestamp
        });
    }
}
```

### Rebuilding an Event Projection

A rebuild replays every event, and Polecat doesn't know which EF tables your projection writes. An event projection that inserts rows therefore fails with a primary key `DbUpdateException` when it replays over rows that are already there. Declare the tables it writes so that a rebuild empties them first:

```cs
public class OrderDetailProjection : EfCoreEventProjection<AppDbContext>
{
    public OrderDetailProjection()
    {
        Options.DeleteDataInTableOnTeardown("dbo.ef_order_details");
    }

    // ...
}
```

The same applies to any rewind that replays events the projection has already written.

## Schema Migration with AddEntityTablesFromDbContext

By default, EF Core entity tables must be created separately (e.g., via `dotnet ef database update` or `Database.GenerateCreateScript()`). If you want Polecat's Weasel migration pipeline to manage your EF Core entity tables alongside Polecat's own schema objects, use `AddEntityTablesFromDbContext`:

```cs
var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = connectionString;
    opts.AddEntityTablesFromDbContext<OrderDbContext>();
});
```

This reads the entity model from your `DbContext` and registers each table with Polecat's Weasel migration system. When `ApplyAllConfiguredChangesToDatabaseAsync()` runs, EF Core entity tables will be created and migrated automatically.

### Schema Handling

`AddEntityTablesFromDbContext` respects explicit schema configuration in your EF Core model:

- **Entities with an explicit schema** (via `HasDefaultSchema()` or `ToTable("name", "schema")`) will retain their configured schema
- **Entities without an explicit schema** will use the SQL Server default schema (`dbo`)

```cs
// This DbContext places entities in a custom schema
public class MyDbContext : DbContext
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("my_schema");
        modelBuilder.Entity<Order>().ToTable("orders");
    }
}

var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = connectionString;
    opts.DatabaseSchemaName = "polecat_schema";

    // The "orders" table will be created in "my_schema", NOT "polecat_schema"
    opts.AddEntityTablesFromDbContext<MyDbContext>();
});
```

### Combining with Projections

You can use `AddEntityTablesFromDbContext` alongside EF Core projections for a fully integrated setup:

```cs
var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = connectionString;
    opts.Projections.Add<OrderEfProjection, Order, OrderDbContext>(
        opts, new OrderEfProjection(), ProjectionLifecycle.Inline);

    // Also register the entity tables for Weasel migration
    opts.AddEntityTablesFromDbContext<OrderDbContext>();
});

// This single call now creates both Polecat event tables and EF Core entity tables
await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
```

## How It Works

1. Polecat creates a `DbContext` for each projection batch
2. `DbContext.SaveChangesAsync()` is called as a `ITransactionParticipant` within Polecat's transaction
3. Both Polecat document operations and EF Core changes commit atomically

Where the `DbContext` reads depends on the projection's lifecycle:

| Mode | Reads run on |
| :--- | :--- |
| Inline | The session's own connection, inside its transaction, so the read sees what the session has written and a save needs one pooled connection however many reads it makes |
| Async | A connection to the database the daemon is working, opened around each read and closed again, so a batch holds nothing between reads |
| Live | The same, for the session's database, unless the session already has a transaction open, in which case the read joins it |

Commands the `DbContext` runs on Polecat's behalf go through `StoreOptions.ResiliencePipeline` along with everything else: loading a projected entity, and the flush itself. The one thing Polecat cannot wrap is a query your own projection code issues against the `DbContext` it was handed.

::: warning
An inline projection's `DbContext` shares a connection with the session, so on a connection without `MultipleActiveResultSets=True` it cannot have a reader open while the session runs a command, or the other way round. Materialize a query (`ToListAsync()` and friends) before using the other one, rather than running one inside a loop over the other.
:::

## Registration

```cs
opts.Projections.Add<OrderEfProjection>(ProjectionLifecycle.Inline);
// or
opts.Projections.Add<OrderEfProjection>(ProjectionLifecycle.Async);
```

## Lifecycle Support

EF Core projections support all lifecycle modes:

| Mode | Description |
| :--- | :--- |
| Inline | DbContext changes commit in same transaction as events |
| Async | Daemon processes events and applies DbContext changes |
| Live | Aggregate built on demand with DbContext |

## Tenanted Projections

EF Core projections work with multi-tenancy -- the DbContext reads and writes through the session's connection to the tenant's database, including under database-per-tenant and master-table tenancy. The entity tables have to exist in every tenant database for that to work.

::: tip
The `ITransactionParticipant` pattern ensures that EF Core's `SaveChanges` runs within Polecat's transaction boundary, providing atomic consistency between event-sourced projections and EF Core-managed tables.
:::
