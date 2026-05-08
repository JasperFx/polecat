# Getting Started

Polecat integrates with the standard .NET `IServiceCollection` abstractions for IoC registration. Most features work without IoC, but the async daemon and schema management leverage the `IHost` model.

## Installation

::: code-group

```shell [.NET CLI]
dotnet add package Polecat
```

```powershell [Powershell]
PM> Install-Package Polecat
```

:::

## SQL Server 2025

Polecat requires **SQL Server 2025** (v17) or later for its native JSON type support. For local development, the easiest approach is Docker:

```yaml
# docker-compose.yml
services:
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2025-CU1-ubuntu-24.04
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "YourStrong!Password"
    ports:
      - "1433:1433"
```

## Registering Polecat

In your application startup, call `AddPolecat()` to register all services:

```cs
builder.Services.AddPolecat(options =>
{
    // Connection string to your SQL Server 2025 database
    options.Connection("Server=localhost;Database=myapp;User Id=sa;Password=YourStrong!Password;TrustServerCertificate=True");

    // Optionally change the default schema (default is "dbo")
    options.DatabaseSchemaName = "myschema";
})
// Run Weasel schema migration on startup so the pc_* tables exist
// before the first session is opened. Recommended in development;
// in production you'll typically generate scripts ahead of time instead.
.ApplyAllDatabaseChangesOnStartup();
```

If you have async projections registered, also opt the daemon into the host:

```cs
builder.Services.AddPolecat(options =>
{
    options.Connection("...");
    options.Projections.Add<MyProjection>(ProjectionLifecycle.Async);
})
.ApplyAllDatabaseChangesOnStartup()
.AddAsyncDaemon(DaemonMode.Solo);
```

See [Bootstrapping Polecat](/configuration/hostbuilder) for more options.

::: tip
`AddPolecat()` registers `IDocumentStore` as a singleton, and `IDocumentSession` / `IQuerySession` as scoped services. In most cases you should inject a session directly.
:::

::: warning
`AddPolecat()` only registers services. The pc_* event store and document
tables are not created until either `ApplyAllDatabaseChangesOnStartup()`
runs (recommended) or the first session triggers `AutoCreateSchemaObjects`.
Calling `store.Advanced.CleanAllEventDataAsync()` before either has happened
on a brand-new database is a no-op rather than an error.
:::

## Working with Documents

Define a simple document type:

```cs
public class User
{
    public Guid Id { get; set; }
    public required string FirstName { get; set; }
    public required string LastName { get; set; }
    public bool Internal { get; set; }
}
```

*For more information on document identity, see [identity](/documents/identity).*

Use `IDocumentSession` to store and query documents:

```cs
// Store a document
app.MapPost("/user", async (CreateUserRequest create, IDocumentSession session) =>
{
    var user = new User
    {
        FirstName = create.FirstName,
        LastName = create.LastName,
        Internal = create.Internal
    };
    session.Store(user);
    await session.SaveChangesAsync();
});

// Query with LINQ
app.MapGet("/users", async (bool internalOnly, IDocumentSession session, CancellationToken ct) =>
{
    return await session.Query<User>()
        .Where(x => x.Internal == internalOnly)
        .ToListAsync(ct);
});

// Load by ID
app.MapGet("/user/{id:guid}", async (Guid id, IQuerySession session, CancellationToken ct) =>
{
    return await session.LoadAsync<User>(id, ct);
});
```

For more information on querying, check [document querying](/documents/querying/).

## Working with Events

Please check the [Event Store quick start](/events/quickstart).

## Creating a Standalone Store

You can create a document store outside of the generic host infrastructure using `DocumentStore.For`:

```cs
var store = DocumentStore.For("Server=localhost;Database=myapp;User Id=sa;Password=YourStrong!Password;TrustServerCertificate=True");
```

Or with full configuration:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("Server=localhost;Database=myapp;User Id=sa;Password=YourStrong!Password;TrustServerCertificate=True");
    // Configure additional options...
});
```
