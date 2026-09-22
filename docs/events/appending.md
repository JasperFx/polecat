# Appending Events

Polecat provides several ways to append events to streams.

## Starting a New Stream

Create a new event stream with initial events:

```cs
// With explicit ID
var streamId = Guid.NewGuid();
session.Events.StartStream<QuestParty>(streamId,
    new QuestStarted("Destroy the Ring"),
    new MembersJoined("Rivendell", ["Frodo", "Sam"])
);

// With auto-generated ID
var streamId = session.Events.StartStream<QuestParty>(
    new QuestStarted("Destroy the Ring")
);

// String stream IDs (when StreamIdentity = AsString)
session.Events.StartStream<QuestParty>("quest-123",
    new QuestStarted("Destroy the Ring")
);
```

`StartStream` will throw if a stream with the same ID already exists.

## Appending to an Existing Stream

```cs
session.Events.Append(streamId,
    new MembersJoined("Moria", ["Gimli", "Legolas"]),
    new MembersDeparted("Moria", ["Gandalf"])
);

await session.SaveChangesAsync();
```

## Optimistic Concurrency

Append with an expected version to detect concurrent modifications:

```cs
session.Events.Append(streamId, expectedVersion: 4,
    new MembersDeparted("Amon Hen", ["Boromir"])
);

// Throws EventStreamUnexpectedMaxEventIdException
// if current stream version != 4
await session.SaveChangesAsync();
```

## FetchForWriting

Load an aggregate and append events with built-in version checking:

```cs
var stream = await session.Events.FetchForWriting<QuestParty>(streamId);

// stream.Aggregate is the current state
// stream.CurrentVersion is the current version

stream.AppendOne(new MembersDeparted("Amon Hen", ["Boromir"]));
await session.SaveChangesAsync();
```

## FetchForExclusiveWriting

Pessimistic locking with `UPDLOCK HOLDLOCK` for exclusive access:

```cs
var stream = await session.Events.FetchForExclusiveWriting<QuestParty>(streamId);

// The stream row is locked until the transaction completes
stream.AppendOne(new MembersJoined("Gondor", ["Faramir"]));
await session.SaveChangesAsync();
```

### When the Lock Cannot Be Taken

`FetchForExclusiveWriting` and `AppendExclusive` hold a lock on the `pc_streams` row until the
transaction completes, so two callers contending for the same stream — or for two streams in
opposite order — can lose the lock. SQL Server raises a lock timeout (error 1222) or names one
transaction the deadlock victim (error 1205), and Polecat translates either into
`Polecat.Exceptions.StreamLockedException`, which derives from `JasperFx.Events.StreamLockedException`
and carries the underlying `SqlException` as its `InnerException`.

**Polecat does not retry this for you, and the count is zero, not five.** SqlClient's configurable
retry is attached to the *connection*, where it governs `Open()` only; command execution is retried
only through `SqlCommand.RetryLogicProvider`, which Polecat never sets. Nor would a command-level
retry be correct: by the time 1205 is raised the transaction is already rolled back, so the only
meaningful retry is a replay of the whole unit of work.

Retry at the application layer instead. Under Wolverine, that is a failure policy on the exception:

```cs
opts.OnException<JasperFx.Events.StreamLockedException>()
    .RetryWithCooldown(50.Milliseconds(), 100.Milliseconds(), 250.Milliseconds());
```

The message is re-handled from the top, so the exclusive read happens again against the state the
winning transaction left behind.

## Enforcing Consistency Without Appending Events

In some command handling scenarios, your business logic may evaluate the current aggregate state and decide that no new events need to be emitted. By default, if no events are appended to the stream returned by `FetchForWriting()`, Polecat will not perform any concurrency check when `SaveChangesAsync()` is called. This means that if another process has modified the stream between your fetch and save, you won't know about it.

If you need to guarantee that the stream has not been modified even when your handler doesn't emit events, you can set `AlwaysEnforceConsistency = true` on the stream:

```cs
var stream = await session.Events.FetchForWriting<Order>(command.OrderId);

// Tell Polecat to enforce the optimistic concurrency check
// even if we don't append any events
stream.AlwaysEnforceConsistency = true;

var order = stream.Aggregate;

// Business logic that may or may not produce events
if (order.NeedsUpdate(command))
{
    stream.AppendOne(new OrderUpdated(command.Data));
}

// If no events were appended, Polecat will still verify that the
// stream version hasn't changed since FetchForWriting() was called.
// Throws EventStreamUnexpectedMaxEventIdException if another process modified the stream.
await session.SaveChangesAsync();
```

When `AlwaysEnforceConsistency` is `true`:

- **If events are appended**, Polecat behaves exactly as before -- the normal optimistic concurrency check is applied.
- **If no events are appended**, Polecat issues an `AssertStreamVersion` check that reads the current stream version from the database and throws an `EventStreamUnexpectedMaxEventIdException` if it doesn't match the version that was fetched.

This is useful in workflows where:

- A command handler conditionally emits events and you need to know if another process raced ahead
- You want to implement "read-then-validate" patterns where consistency of the read matters even without writes
- You're building saga or process manager patterns where skipping an event is a valid but concurrency-sensitive outcome

## WriteToAggregate

Fetch, apply, and save in a single call:

```cs
await session.Events.WriteToAggregate<QuestParty>(streamId, stream =>
{
    stream.AppendOne(new MembersDeparted("Mordor", ["Frodo", "Sam"]));
});

await session.SaveChangesAsync();
```

## QuickAppend

Polecat uses **QuickAppend** exclusively -- all event appending is done via direct SQL `INSERT` statements with an `UPDATE...OUTPUT` pattern for atomic version management. No stored procedures are involved.

The flow:

1. `INSERT` new events into `pc_events`
2. `UPDATE pc_streams SET version = version + @count OUTPUT INSERTED.version` for version management
3. Both operations run in the same transaction as document operations via `SaveChangesAsync()`

## Event Metadata

Set correlation and causation IDs via session options:

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    CorrelationId = "request-123",
    CausationId = "command-456"
});

session.Events.Append(streamId, new QuestEnded("Destroy the Ring"));
await session.SaveChangesAsync();
```

Custom headers can be added to individual events via the `Headers` property on `StreamAction`.
