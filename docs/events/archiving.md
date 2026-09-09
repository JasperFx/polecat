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
- Attempting to append to an archived stream throws `InvalidStreamException`

## Unarchiving a Stream

Restore an archived stream:

```cs
session.Events.UnArchiveStream(streamId);
await session.SaveChangesAsync();
```

This sets `is_archived = 0` on both tables, making the stream active again.

## Tombstoning (Hard Delete)

For permanent removal of a stream and all its events:

```cs
session.Events.TombstoneStream(streamId);
await session.SaveChangesAsync();
```

::: warning
Tombstoning permanently `DELETE`s the stream record and all associated events from the database. This cannot be undone.
:::

Tombstoning works with both Guid and string stream IDs.

## Archiving vs Tombstoning

| Operation | Reversible | Data Preserved | Use Case |
| :--- | :--- | :--- | :--- |
| Archive | Yes | Yes | Soft removal, compliance holds |
| Tombstone | No | No | GDPR right to erasure, cleanup |

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
