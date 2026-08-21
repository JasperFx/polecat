# Polecat.AspNetCore

ASP.NET Core helpers for applications built on [Polecat](https://www.nuget.org/packages/Polecat/), the
SQL Server-backed event store in the Critter Stack.

## Install

```
dotnet add package Polecat.AspNetCore
```

## What it gives you

**Write event streams straight to the response.** `WriteStreamState` and `WriteEvents` serialize a
stream's aggregate or its raw events using the store's own serializer, so the wire format matches
what Polecat persists:

```csharp
app.MapGet("/orders/{id:guid}", (Guid id, HttpContext context, IQuerySession session)
    => context.Response.WriteStreamState<Order>(session, id));
```

**ETag helpers for optimistic concurrency.** `ETagHelpers` formats a stream version — `Guid` or
`long` — as an HTTP ETag and checks incoming `If-None-Match` headers, so a caller can be told
`304 Not Modified` without you reloading the aggregate.

**MCP endpoints.** `MapPolecatMcp` exposes the event store over the Model Context Protocol for
agent-driven exploration of streams and events.

## Requirements

- .NET 9 or .NET 10
- A configured Polecat `IDocumentStore` — see the [Polecat package](https://www.nuget.org/packages/Polecat/)

## Links

- [Documentation](https://polecat.jasperfx.net/)
- [GitHub](https://github.com/JasperFx/polecat)
- [Discord](https://discord.gg/WMxrvegf8H)
