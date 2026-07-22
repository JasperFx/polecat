# Polecat.AspNetCore

Polecat ships a small companion package, **Polecat.AspNetCore**, with helpers for ASP.NET Core
development. The main feature is a set of typed `IResult` wrappers that let you return
Polecat documents and event-sourced aggregates directly from Minimal API endpoints with
correct status codes, content types, and OpenAPI metadata.

Install the NuGet package:

```powershell
PM> Install-Package Polecat.AspNetCore
```

## Typed Streaming Result Types

For Minimal API endpoints (and frameworks like [Wolverine.Http](https://wolverinefx.net/guide/http/)
that dispatch any `IResult` return value), Polecat.AspNetCore ships three typed result wrappers:

| Type | Source | Response shape | 404 on miss? |
| --- | --- | --- | --- |
| `StreamOne<T>` | `IQueryable<T>` — document query | Single `T` | yes |
| `StreamMany<T>` | `IQueryable<T>` — document query | JSON array `T[]` | no (empty array = 200) |
| `StreamAggregate<T>` | `IQuerySession` + stream id — event-sourced | Single `T` | yes |

Each type implements both `IResult` (so ASP.NET dispatches it via `ExecuteAsync`) and
`IEndpointMetadataProvider` (so OpenAPI generators see the right response shape).

### StreamOne — single document with 404 on miss

```csharp
app.MapGet("/issues/{id:guid}",
    (Guid id, IQuerySession session) =>
        new StreamOne<Issue>(session.Query<Issue>().Where(x => x.Id == id)));
```

Returns `200 application/json` with the document JSON on a hit, `404` on a miss.
`Content-Length` and `Content-Type` are set automatically.

### StreamMany — JSON array

```csharp
app.MapGet("/issues/open",
    (IQuerySession session) =>
        new StreamMany<Issue>(session.Query<Issue>().Where(x => x.IsOpen)));
```

Returns `200 application/json` with a JSON array body. An empty result set yields `[]`,
not a 404.

### StreamAggregate — event-sourced aggregate (latest)

```csharp
app.MapGet("/orders/{id:guid}",
    (Guid id, IQuerySession session) =>
        new StreamAggregate<Order>(session, id));
```

Returns `200 application/json` with the latest projected aggregate state, or `404` if no
stream exists. A constructor overload accepts `string` ids for stores configured with
string-keyed streams.

### StreamOne vs StreamAggregate

- **`StreamOne<T>`** is for regular documents — objects stored via `session.Store()` and
  queried with `session.Query<T>()`.
- **`StreamAggregate<T>`** is for event-sourced aggregates — Polecat rebuilds (or reads the
  snapshot of) the latest aggregate state from events before writing the response.

### ETag / Conditional Requests

`StreamOne<T>` and `StreamAggregate<T>` support HTTP conditional requests
(`ETag` / `If-None-Match` → `304 Not Modified`) so polling clients skip re-downloading
unchanged documents/aggregates. It is **on by default**.

- On a normal hit, an `ETag` response header carrying the version is emitted.
- An incoming `If-None-Match` that matches the current version yields `304 Not Modified`
  with an empty body and the `ETag` header — for `StreamAggregate<T>` this skips the
  aggregation entirely (the stream version is read cheaply first).
- The version source differs by type:
  - **`StreamOne<T>`** uses the document's `version` column (read inline with the document
    JSON in a single round trip — no follow-up metadata query).
  - **`StreamAggregate<T>`** uses the event stream's version (via `FetchStreamStateAsync`,
    read before folding).
- `StreamMany<T>` is intentionally out of scope (a cheap collection-wide ETag is hard to
  derive).

Opt out per endpoint with `EmitETag = false`, which restores the exact pre-ETag behavior
(no header, no conditional handling):

```csharp
app.MapGet("/issues/{id:guid}",
    (Guid id, IQuerySession session) =>
        new StreamOne<Issue>(session.Query<Issue>().Where(x => x.Id == id))
        {
            EmitETag = false
        });
```

::: tip
ETag values are opaque per RFC 7232. Polecat document tables always carry a `version`
column, so a document ETag is always available when `EmitETag` is on; the only way to
suppress it is `EmitETag = false`. `ETagHelpers` handles the `*` wildcard, comma-separated
`If-None-Match` lists, and `W/` weak validators (weak comparison, the correct function for
`If-None-Match`).
:::

### Customizing status code and content type

All three types expose `init`-only properties:

```csharp
app.MapPost("/issues",
    (CreateIssue cmd, IQuerySession session) =>
        new StreamOne<Issue>(session.Query<Issue>().Where(x => x.Id == cmd.IssueId))
        {
            OnFoundStatus = StatusCodes.Status201Created,
            ContentType = "application/vnd.myapi.issue+json"
        });
```

::: tip
`StreamOne<T>` streams the raw persisted document JSON straight through (no
deserialize/reserialize). `StreamMany<T>` and `StreamAggregate<T>` materialize via the
regular query/projection path and serialize through `System.Text.Json`. All of them
eliminate the endpoint boilerplate (null-check, status code, content type, OpenAPI metadata).
:::
