# Polecat Metadata

Polecat can automatically track metadata on your documents through several built-in interfaces.

## Metadata Columns

A document table can have `correlation_id`, `causation_id`, `last_modified_by` and `headers` columns, filled from
the session, and `session.MetadataForAsync(doc)` reads them back. They are opt-in, per document type or for all of
them, as in Polecat 5 and Marten 10.

::: tip Why turn them on
[CritterWatch](https://critterwatch.jasperfx.net)'s document explorer filters documents by `last_modified_by`,
`correlation_id` and `causation_id`, and its saved filter watches use the same columns. Without them those filters have
nothing to match. They're also how you trace a document back to the request or message that last changed it.

What they cost: four more values on every document write, and four more columns on each table. The 6.0 alphas turned
them on for every document type; Marten measured them as about half of what its 10.0 release still cost on projection
rebuilds that write documents, which is why both stores keep them opt-in
([#1134](https://github.com/JasperFx/polecat/issues/1134), [marten#6144](https://github.com/JasperFx/marten/issues/6144)).
:::

To turn them on for one document type:

```cs
opts.Schema.For<Order>().Metadata(m =>
{
    m.CorrelationId.Enabled = true;
    m.CausationId.Enabled = true;
    m.LastModifiedBy.Enabled = true;
});
```

Or for every document type with `opts.Policies.EnableDocumentMetadataByDefault = true`. A document type can then still
turn one column off, with `m.Headers.Enabled = false`. A metadata attribute or `ITracked` (below) turns a column on for
its own type either way.

## ITracked

Implement `ITracked` to have correlation, causation, and user tracking automatically synced:

```cs
public class Order : ITracked
{
    public Guid Id { get; set; }
    public string Description { get; set; } = "";

    // ITracked members - auto-synced from session
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? LastModifiedBy { get; set; }
}
```

Set tracking values on the session:

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    CorrelationId = "request-123",
    CausationId = "command-456",
    LastModifiedBy = "user@example.com"
});

session.Store(new Order { Description = "New order" });
await session.SaveChangesAsync();
// Order.CorrelationId will be "request-123"
```

## ITenanted

Implement `ITenanted` to have the tenant ID automatically synced from the session:

```cs
public class Order : ITenanted
{
    public Guid Id { get; set; }
    public string TenantId { get; set; } = "";
}
```

See [Multi-Tenancy](/configuration/multitenancy) for more details.

## ISoftDeleted

Implement `ISoftDeleted` for automatic soft delete tracking:

```cs
public class Order : ISoftDeleted
{
    public Guid Id { get; set; }
    public bool Deleted { get; set; }
    public DateTimeOffset? DeletedAt { get; set; }
}
```

See [Deleting Documents](/documents/deletes#soft-deletes) for more details.

## IVersioned

Implement `IVersioned` for Guid-based optimistic concurrency:

```cs
public class Order : IVersioned
{
    public Guid Id { get; set; }
    public Guid Version { get; set; }
}
```

See [Optimistic Concurrency](/documents/concurrency) for more details.

### Mapping the version onto a member of your own

The version column can be mapped onto any member instead, and the member's type chooses the
concurrency mode — `Guid` for Guid versioning, `int` or `long` for numeric revisions:

```cs
opts.Schema.For<Order>().Metadata(m => m.Version.MapTo(x => x.Etag));
```

`[VersionMetadata]` on the member is the same declaration. Either way the mapped member is a full
concurrency declaration, not just a place to read the stored value: it is stamped on write,
populated on load, and carries the expected version into the write's guard. See
[Optimistic Concurrency](/documents/concurrency#naming-the-version-member-yourself).

## Built-in Metadata Columns

Every document table includes these metadata columns automatically:

| Column | Description |
| :--- | :--- |
| `last_modified` | Updated to `SYSDATETIMEOFFSET()` on every save |
| `created` | Set to `SYSDATETIMEOFFSET()` on first insert |
| `type` | Short type discriminator |
| `dotnet_type` | Full .NET assembly-qualified type name |

## Metadata LINQ Extensions

Query documents by their metadata:

```cs
// Find documents modified since a given time
var recent = await session.Query<Order>()
    .Where(x => x.ModifiedSince(DateTimeOffset.UtcNow.AddHours(-1)))
    .ToListAsync();

// Find documents modified before a given time
var old = await session.Query<Order>()
    .Where(x => x.ModifiedBefore(DateTimeOffset.UtcNow.AddDays(-30)))
    .ToListAsync();
```
