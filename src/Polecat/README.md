# Polecat

SQL Server-backed event store and lightweight document database for the [Critter Stack](https://jasperfx.net/).

Polecat is "Marten for SQL Server" — the same API shape, on SQL Server 2025. It leans on the native
`json` column type, and stores events, streams and documents in `pc_`-prefixed tables it manages for you.

## Install

```
dotnet add package Polecat
```

## Getting started

```csharp
builder.Services.AddPolecat(opts =>
{
    opts.ConnectionString = connectionString;
    opts.DatabaseSchemaName = "myapp";
});
```

Then take a session and append events:

```csharp
await using var session = store.LightweightSession();

var streamId = Guid.NewGuid();
session.Events.StartStream(streamId, new OrderPlaced(streamId, "Alice", 100m));
await session.SaveChangesAsync();

var events = await session.Events.FetchStreamAsync(streamId);
```

Documents work the same way:

```csharp
session.Store(new Customer { Id = customerId, Name = "Alice" });
await session.SaveChangesAsync();

var customer = await session.LoadAsync<Customer>(customerId);
```

## What's in the box

- Event sourcing — streams keyed by `Guid` or `string`, live aggregation, snapshots
- Projections — inline and async, single-stream, multi-stream, event, flat-table and composite
- Async daemon — high water mark tracking, rebuilds, subscriptions
- Document storage — LINQ querying, patching, bulk insert, soft deletes, optimistic concurrency
- Multi-tenancy — conjoined (per-row) and separate-database, with optional table partitioning

## Requirements

- .NET 9 or .NET 10
- SQL Server 2025 (the native `json` type is used for event data, document bodies and snapshots)

## Links

- [Documentation](https://polecat.jasperfx.net/)
- [GitHub](https://github.com/JasperFx/polecat)
- [Discord](https://discord.gg/WMxrvegf8H)
- [Commercial support](https://jasperfx.net/support-plans/)
