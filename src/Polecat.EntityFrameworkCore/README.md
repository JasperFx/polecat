# Polecat.EntityFrameworkCore

Entity Framework Core integration for [Polecat](https://www.nuget.org/packages/Polecat/) projections.

Project your event streams into EF Core entities instead of Polecat documents, and have both written
in the same transaction. Useful when a read model already belongs to an existing `DbContext`, or when
you want EF Core's mapping and migrations over a projected table.

## Install

```
dotnet add package Polecat.EntityFrameworkCore
```

## Projection base classes

Derive from the base that matches your projection shape and override `ApplyEvent`:

```csharp
public class OrderProjection : EfCoreSingleStreamProjection<Order, MyDbContext>
{
    public OrderProjection() => IncludeType<OrderPlaced>();

    protected override Order? ApplyEvent(Order? snapshot, Guid identity, IEvent @event,
        MyDbContext dbContext, IQuerySession session)
        => @event.Data switch
        {
            OrderPlaced placed => new Order { Id = placed.OrderId, Customer = placed.CustomerName },
            _ => snapshot
        };
}
```

Register it against the store, choosing a lifecycle:

```csharp
opts.Projections.Add<OrderProjection, Order, MyDbContext>(
    opts, new OrderProjection(), ProjectionLifecycle.Async);
```

Three shapes are available:

- `EfCoreSingleStreamProjection<TDoc, TDbContext>` — one aggregate per stream
- `EfCoreMultiStreamProjection<TDoc, TId, TDbContext>` — aggregate across streams via `Identity` / `Identities`
- `EfCoreEventProjection<TDbContext>` — free-form per-event side effects, into EF Core and Polecat alike

## Transactions

The `DbContext` enlists in the Polecat session's unit of work, so `SaveChangesAsync` flushes the
event append and the EF Core changes together — there is no window where one landed and the other
did not.

## Requirements

- .NET 9 or .NET 10
- A configured Polecat `IDocumentStore` — see the [Polecat package](https://www.nuget.org/packages/Polecat/)

## Links

- [Documentation](https://polecat.jasperfx.net/)
- [GitHub](https://github.com/JasperFx/polecat)
- [Discord](https://discord.gg/WMxrvegf8H)
