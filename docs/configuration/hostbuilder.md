# Bootstrapping Polecat

Polecat provides `AddPolecat()` extension methods on `IServiceCollection` for easy integration with .NET's dependency injection.

## Basic Registration

The simplest way to register Polecat:

```cs
builder.Services.AddPolecat(options =>
{
    options.Connection("Server=localhost;Database=myapp;User Id=sa;Password=YourStrong!Password;TrustServerCertificate=True");
});
```

## Registration Overloads

Polecat offers several `AddPolecat()` overloads:

```cs
// Connection string only
builder.Services.AddPolecat("Server=localhost;Database=myapp;...");

// Action-based configuration
builder.Services.AddPolecat(options =>
{
    options.Connection("...");
    options.DatabaseSchemaName = "myschema";
});

// Pre-built StoreOptions
var storeOptions = new StoreOptions();
storeOptions.Connection("...");
builder.Services.AddPolecat(storeOptions);

// Factory-based (access IServiceProvider)
builder.Services.AddPolecat(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var opts = new StoreOptions();
    opts.Connection(config.GetConnectionString("SqlServer")!);
    return opts;
});
```

## Registered Services

`AddPolecat()` registers the following services:

| Service | Lifetime | Description |
| :--- | :--- | :--- |
| `IDocumentStore` | Singleton | Main entry point, creates sessions |
| `ISessionFactory` | Singleton | Factory for creating sessions (default: lightweight) |
| `IDocumentSession` | Scoped | Read/write session with unit of work |
| `IQuerySession` | Scoped | Read-only session for queries |

## ConfigurePolecat

`ConfigurePolecat()` registers a post-configuration action that runs against `StoreOptions` after
`AddPolecat()` has built them. Use it when a module other than the one that called `AddPolecat()`
owns part of the store configuration:

```cs
builder.Services.ConfigurePolecat(options =>
{
    options.CommandTimeout = 120;
});
```

There is a second overload that also hands you the built `IServiceProvider`, for configuration that
depends on other registered services:

```cs
builder.Services.ConfigurePolecat((services, options) =>
{
    var settings = services.GetRequiredService<IOptions<RetentionSettings>>().Value;
    options.Schema.For<MetricsSample>().PartitionOn(x => x.Timestamp)
        .ByRollingRange(RollingPeriod.Day, ahead: 2, behind: settings.DaysRetained);
});
```

Both overloads have a `ConfigurePolecat<T>()` counterpart that targets an ancillary store registered
with `AddPolecatStore<T>()`.

## IConfigurePolecat

You can implement `IConfigurePolecat` to modularize your configuration:

```cs
public class MyPolecatConfig : IConfigurePolecat
{
    public void Configure(IServiceProvider services, StoreOptions options)
    {
        // Apply configuration here
    }
}
```

Register it before `AddPolecat()`:

```cs
builder.Services.AddSingleton<IConfigurePolecat, MyPolecatConfig>();
builder.Services.AddPolecat(options =>
{
    options.Connection("...");
});
```

## Session Factory

By default, Polecat creates lightweight sessions (no identity tracking). You can change this by providing a custom `ISessionFactory`:

```cs
builder.Services.AddPolecat(options =>
{
    options.Connection("...");
});
```

::: tip
Lightweight sessions are recommended for most use cases. Only use `IdentityMap` sessions when you need to ensure the same document instance is returned for repeated loads within a session.
:::
