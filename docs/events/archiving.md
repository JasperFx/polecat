# Archiving Streams

Polecat supports archiving event streams to logically remove them from active queries without permanently deleting the data.

## Archiving a Stream

```cs
session.Events.ArchiveStream(streamId);
await session.SaveChangesAsync();
```

This sets `is_archived = 1` on both the `pc_streams` and `pc_events` tables for the stream.

## Effects of Archiving

When a stream is archived:

- `FetchStreamAsync` excludes the archived stream
- The async daemon's event loader skips archived events
- Attempting to append to an archived stream throws `ArchivedStreamException`

The append refusal is raised at `SaveChangesAsync()`, when Polecat reads the stream's state, and it
names the way back:

> Event stream 'a1b2…' is archived and cannot be appended to. Call UnArchiveStream to reopen it, or
> start a new stream.

`Polecat.Exceptions.ArchivedStreamException` derives from `JasperFx.Events.ArchivedStreamException`,
so store-agnostic code can catch the shared type. (It replaces
`InvalidStreamException`, which is now obsolete and never thrown.)

## Unarchiving a Stream

Restore an archived stream — this is what the refusal above points you at:

```cs
session.Events.UnArchiveStream(streamId);
await session.SaveChangesAsync();
```

This sets `is_archived = 0` on both tables, making the stream active again.

## Tombstoning (Deleting a Stream)

`ArchiveStream` hides a stream but keeps every row. When the data has to actually go, for a GDPR
erasure request, test or demo data, or a purge job, `TombstoneStream` is the hard delete: it removes
the stream and every event in it from the database.

<!-- snippet: sample_polecat_tombstone_stream -->
<a id='snippet-sample_polecat_tombstone_stream'></a>
```cs
await using var session2 = theStore.LightweightSession();

// Queues a hard DELETE of the stream, its events and their tag rows.
// Nothing touches the database until SaveChangesAsync()
session2.Events.TombstoneStream(streamId);
await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/tombstone_stream_tests.cs#L23-L30' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_tombstone_stream' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

There is an overload for each stream identity, `TombstoneStream(Guid)` and `TombstoneStream(string)`;
use the one that matches `StreamIdentity`.

::: warning
Tombstoning permanently `DELETE`s the stream record and all associated events from the database. This
cannot be undone, and unlike `ArchiveStream` there is no `UnArchiveStream` to bring it back.
:::

What to expect:

- **It is queued.** Like `ArchiveStream` and `Append`, `TombstoneStream` only adds an operation to the
  session's unit of work. The `DELETE` runs when you call `SaveChangesAsync()`, in the same transaction
  as anything else in that session, and discarding the session without saving deletes nothing.
- **The whole stream goes.** The row in `pc_streams` and every row for that stream in `pc_events` are
  deleted. The stream's [DCB tag rows](/events/dcb) and any natural key rows go with them, because
  those tables cascade from the events and streams tables.
- **It is tenant-scoped.** Only the stream in the session's tenant is deleted. Under conjoined
  tenancy, a stream with the same id in another tenant is untouched.
- **A missing stream is a no-op.** Tombstoning a stream that does not exist, or was already deleted,
  is not an error.
- **The id can be reused.** After the delete, you can start a new stream with the same identity.
- **Sequence numbers are not reused.** The deleted events leave a gap in the event sequence, and new
  events keep counting up from the high-water mark. A tombstoned stream can never hide later events
  from an async projection.
- **Projected documents are left alone.** Tombstoning deletes events, not the documents that
  projections already built from them. If an aggregate snapshot or read model should go too, delete
  it in the same session (for example with `session.Delete<T>(id)`) so both land in one transaction.

`TombstoneStream` is part of the shared `JasperFx.Events.IEventStoreOperations` contract as of
JasperFx 2.90.0, the same surface Marten implements, so store-agnostic code can call it through
`IEventStoreOperations` without referencing Polecat. Polecat's behaviour is pinned by the shared
`StreamTombstoningCompliance` suite.

## Archiving vs Tombstoning

| Operation | Reversible | Data Preserved | Use Case |
| :--- | :--- | :--- | :--- |
| Archive | Yes | Yes | Soft removal, compliance holds |
| Tombstone (`TombstoneStream`) | No | No | GDPR right to erasure, cleanup |

## Compacting a Stream

Compaction is the third option in that table: it replaces a stream's history with a single
`Compacted<T>` snapshot event and deletes the events it folded. The stream keeps its version, so
appending carries on as before, but a fold no longer has to replay everything below the compaction
point — `Compacted<T>` fast-forwards it.

```cs
await using var session = store.LightweightSession();
await session.Events.CompactStreamAsync<Freighter>(streamId);
await session.SaveChangesAsync();
```

Like every other session operation, the typed overload only *queues* the work — the replace, the
deletes and the compaction watermark all land on `SaveChangesAsync()`.

::: warning
Compaction `DELETE`s the events it folds. What survives is the snapshot inside the marker, so an
aggregate that cannot be rebuilt from that snapshot alone cannot be rebuilt at all. Use
`StreamCompactingRequest<T>.Archiver` to copy the events somewhere first if you need them.
:::

### Compacting without a compile-time type

`IEventStore.CompactStreamAsync` is the untyped overload. It reads the stream's recorded aggregate
type from `pc_streams` and closes the typed operation over it, which is the only form available to a
caller holding a runtime `Type` — a compaction policy that selects streams by aggregate type, for
instance, or a tool with a "compact this stream" button:

```cs
var streams = ((IEventStore)store).OpenReadOnlyEventStore().QueryStreamStates();

var overgrown = await streams
    .Where(x => x.AggregateType == typeof(Freighter) && x.Version - x.CompactedVersion > 500)
    .ToListAsync();

foreach (var state in overgrown)
{
    // Resolves Freighter from the stream state, and commits on its own
    await ((IEventStore)store).CompactStreamAsync(state.Id, cancellationToken);
}
```

Two differences from the typed overload are worth knowing:

- It opens and commits its own session, so there is no `SaveChangesAsync()` to call.
- It needs an aggregate type on the stream. A stream started with `StartStream<T>()` records one; a
  stream started without a type has nothing to resolve, and the call is refused rather than silently
  doing nothing. Name the type explicitly with `CompactStreamAsync<T>` in that case.

`StreamState.CompactedVersion` is the watermark the last compaction reached, so
`Version - CompactedVersion` is the stream's un-compacted growth — which is what makes a policy like
the one above idempotent instead of re-compacting the same streams on every pass.

### Compacting a stream in one tenant's scope

Both untyped overloads take an optional tenant id. The stream-state read and the compaction itself
then run on a session opened for that tenant, which is the action-side twin of
`OpenReadOnlyEventStore(tenantId)`:

```cs
var streams = ((IEventStore)store).OpenReadOnlyEventStore("blue").QueryStreamStates();

var overgrown = await streams
    .Where(x => x.Version - x.CompactedVersion > 500)
    .ToListAsync();

foreach (var state in overgrown)
{
    await ((IEventStore)store).CompactStreamAsync(state.Id, "blue", cancellationToken);
}
```

Pass this overload whenever the selector was tenanted, and the tenant-less one only for a
single-tenanted store. A policy that selects a tenant's streams through the tenanted reader and then
calls the tenant-less action is reading and writing in two different scopes: on a conjoined store the
action looks for those stream ids in the *default* tenant's partition, where they are not visible and
are reported as missing, and on a store whose default tenant is disabled the call is refused
outright. A null or empty tenant id means the default tenant, matching the reader.
