# Opening Sessions

Polecat uses sessions to manage document operations. Sessions provide a unit of work pattern -- changes are accumulated and flushed to the database in a single transaction when `SaveChangesAsync()` is called.

## Session Types

### Lightweight Session (Default)

No identity tracking. Each `LoadAsync()` call returns a new object instance:

```cs
await using var session = store.LightweightSession();
```

### Identity Map Session

Tracks loaded documents by ID. Repeated loads of the same document return the same instance:

```cs
await using var session = store.OpenSession(DocumentTracking.IdentityMap);
```

### Query Session (Read-Only)

For read-only operations. Cannot store or delete documents:

```cs
await using var session = store.QuerySession();
```

## Session Options

Configure sessions with `SessionOptions`:

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    // Multi-tenancy
    TenantId = "my-tenant",

    // Command timeout in seconds
    Timeout = 30,

    // Metadata tracking
    CorrelationId = "request-123",
    CausationId = "command-456",
    LastModifiedBy = "user@example.com",

    // Per-session listeners
    Listeners = { new MySessionListener() }
});
```

## OpenSessionAsync

For scenarios requiring explicit transaction control, use `OpenSessionAsync` to eagerly open a connection and begin a transaction:

```cs
await using var session = await store.OpenSessionAsync(new SessionOptions
{
    IsolationLevel = IsolationLevel.Serializable
});
```

This is particularly useful for:

- Exclusive writing patterns (pessimistic locking)
- Custom isolation levels
- Ensuring a transaction is active before any operations

## Unit of Work

Sessions accumulate pending operations:

```cs
await using var session = store.LightweightSession();

session.Store(new User { FirstName = "Alice" });
session.Store(new User { FirstName = "Bob" });
session.Delete<Order>(orderId);

// All operations execute in a single transaction
await session.SaveChangesAsync();
```

### Inspecting Pending Changes

```cs
var pending = session.PendingChanges;
```

### Ejecting Documents

Remove documents from pending changes without affecting the database:

```cs
// Eject a specific document
session.Eject(document);

// Eject all documents of a type
session.EjectAllOfType(typeof(User));

// Eject all pending changes
session.EjectAllPendingChanges();
```

## Session Listeners

Implement `IDocumentSessionListener` to hook into session lifecycle events. `AfterCommitAsync`
receives an [`IChangeSet`](#the-ichangeset) describing everything that was written in the just-committed
unit of work:

```cs
public class AuditListener : IDocumentSessionListener
{
    public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken ct)
    {
        // Called before the transaction begins. Inspect the pending work via session.PendingChanges.
        return Task.CompletedTask;
    }

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken ct)
    {
        // Called after the transaction commits. `commit` reports what changed.
        foreach (var inserted in commit.Inserted) { /* ... */ }
        foreach (var updated in commit.Updated) { /* ... */ }
        foreach (var deleted in commit.Deleted) { /* deleted.DocumentType / deleted.Id */ }
        return Task.CompletedTask;
    }
}
```

Register listeners globally or per-session:

```cs
// Global (on all sessions)
opts.Listeners.Add(new AuditListener());

// Per-session
await using var session = store.LightweightSession(new SessionOptions
{
    Listeners = { new AuditListener() }
});
```

### The IChangeSet

`IChangeSet` is a snapshot of a single committed unit of work, handed to both `IDocumentSessionListener`
(session saves) and `IChangeListener` (async daemon batches — see
[Listening for Async Daemon Events](/events/projections/async-daemon#listening-for-daemon-commits)):

```cs
public interface IChangeSet
{
    IEnumerable<object> Updated { get; }       // Update/Upsert operations
    IEnumerable<object> Inserted { get; }      // Insert operations
    IEnumerable<IDeletion> Deleted { get; }    // Deletions (DocumentType + Id)

    IEnumerable<IEvent> GetEvents();           // events appended this unit of work
    IEnumerable<StreamAction> GetStreams();    // stream actions this unit of work

    IChangeSet Clone();
}
```

::: warning
The live change set is reset immediately after each commit. If you retain it beyond the
`AfterCommitAsync` call (e.g. to hand off to background work), call `commit.Clone()` first to take an
immutable copy.
:::

## Request Counting

Track the number of database requests made by a session:

```cs
await using var session = store.LightweightSession();
await session.LoadAsync<User>(id);
Console.WriteLine(session.RequestCount); // 1
```
