# Asynchronous Projections

The async daemon is a background service that processes events and applies projections asynchronously, providing eventually consistent read models.

## How It Works

1. The **High Water Mark Detector** monitors `pc_events` for new events using SQL `LEAD()` window functions to detect gaps
2. The **Event Loader** fetches batches of events for processing
3. Each projection processes its batch and updates its read model
4. Progress is tracked in `pc_event_progression` via atomic `MERGE` statements

## Enabling the Async Daemon

Register projections with async lifecycle:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    opts.Projections.Snapshot<OrderSummary>(SnapshotLifecycle.Async);
    opts.Projections.Add<DashboardProjection>(ProjectionLifecycle.Async);
});
```

When wired up through `AddPolecat()`, opt the daemon into the host's lifetime
explicitly with `AddAsyncDaemon(DaemonMode)`:

```cs
builder.Services.AddPolecat(opts =>
{
    opts.Connection("...");

    opts.Projections.Snapshot<OrderSummary>(SnapshotLifecycle.Async);
    opts.Projections.Add<DashboardProjection>(ProjectionLifecycle.Async);
})
.AddAsyncDaemon(DaemonMode.Solo)        // start the daemon as IHostedService
.ApplyAllDatabaseChangesOnStartup();    // run schema migration at boot
```

::: tip
Async projections do **not** run unless you call `AddAsyncDaemon(...)`. Use
`DaemonMode.Solo` for single-node deployments and `DaemonMode.HotCold` for
multi-node deployments where only one host should own each projection shard.
:::

## Daemon Settings

Configure daemon behavior:

```cs
opts.DaemonSettings.StaleSequenceThreshold = 1000;
```

### Polling

Unlike Marten's PostgreSQL `LISTEN/NOTIFY`, Polecat uses **polling** to detect new events:

```cs
// The daemon polls for new events at a configurable interval
// Default: 500ms
```

## Waiting for Non-Stale Data

### CatchUpAsync

Wait for all projections to catch up to the current high water mark:

```cs
await store.WaitForNonStaleProjectionDataAsync(TimeSpan.FromSeconds(30));
```

### Per-Query

Wait for projections before a specific query:

```cs
var orders = await session.Query<OrderSummary>()
    .QueryForNonStaleData()
    .Where(x => x.Status == "Active")
    .ToListAsync();
```

## Event Progression

Track daemon progress:

```cs
// The pc_event_progression table stores:
// - name: Projection/subscription name
// - last_seq_id: Last processed sequence ID
// - last_updated: When last updated
```

## High Water Mark Detection

The high water mark detector uses SQL Server's `LEAD()` window function to detect sequence gaps in the event log. This prevents the daemon from processing events out of order when concurrent writers create gaps.

## Listening for Daemon Commits

Sometimes you need to run a side effect **after** an aggregate has been durably updated by the async
daemon — the canonical case is invalidating a cache key. Doing this before the commit would open a
window where a concurrent read could repopulate the cache with stale state. Subscriptions, composite
projections, and projection side effects all run *before* the batch is committed, so they can't close
that window.

Register an `IChangeListener` on `Projections.AsyncListeners` to hook the commit boundary of each
daemon projection batch:

```cs
public class CacheFlushingListener : IChangeListener
{
    // Runs AFTER the batch is committed → "at most once". Ideal for cache invalidation.
    public async Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
    {
        foreach (var party in commit.Updated.OfType<QuestParty>())
        {
            await _cache.RemoveAsync($"quest-party:{party.Id}", token);
        }
    }

    // Runs BEFORE the batch is committed → "at least once".
    public Task BeforeCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        => Task.CompletedTask;
}
```

```cs
var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = connectionString;
    opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);

    // Fires only within the async daemon, once per committed projection batch
    opts.Projections.AsyncListeners.Add(new CacheFlushingListener());
});
```

The `commit` parameter is an [`IChangeSet`](/documents/sessions#the-ichangeset) describing the projected
documents written in that batch (`Inserted` / `Updated` / `Deleted`).

Delivery semantics mirror Marten:

- **`AfterCommitAsync`** runs once, *after* the transaction commits — **at most once**. A faulting
  after-commit listener is swallowed so the batch is not reprocessed (which would re-fire the side
  effect); the data is already durable.
- **`BeforeCommitAsync`** runs *before* the commit — **at least once**. A throw here aborts the batch
  before anything is committed.

::: tip
Async listeners are **suppressed during projection rebuilds**. A full replay re-applies every event,
so firing post-commit side effects for each historical batch is almost never what you want.
:::

## Blue/Green Deployments with Projection Versioning

Every projection carries a `Version` (default `1`). The version is baked into the shard's
progression identity, so different versions of the same projection track their progress
independently:

```cs
public class TripProjection : SingleStreamProjection<Trip, Guid>
{
    public TripProjection()
    {
        // Bump the version when you change the projection's logic or shape
        Version = 3;
    }
}
```

| Projection | `Version` | Progression identity (`pc_event_progression.name`) |
|------------|-----------|-----------------------------------------------------|
| `Trip`     | `1`       | `Trips:All`                                         |
| `Trip`     | `2`       | `Trips:V2:All`                                       |
| `Trip`     | `3`       | `Trips:V3:All`                                       |

Because each version has its own progression row, you can stand up a new version alongside
the old one and let it rebuild from zero while the old version keeps serving reads — a
blue/green deployment.

### Gating Side Effects Behind the Prior Version

The catch with a blue/green rebuild is [side effects](side-effects.md). If your projection
publishes messages or emits other side effects from `RaiseSideEffects()`, a new version
replaying the full event history from zero would **re-fire every side effect** for events the
previous version already processed — duplicate emails, duplicate messages, and so on.

Opt into the **side-effect gate** to prevent that:

```cs
public class TripProjection : SingleStreamProjection<Trip, Guid>
{
    public TripProjection()
    {
        Version = 3;

        // Suppress side effects for events the prior version already processed
        Options.GateSideEffectsBehindPriorVersion = true;
    }
}
```

When the daemon starts a gated shard whose own progression is **behind** the highest prior
version's persisted mark `N`, it:

1. Replays the new version from its current position up to `N` in **Rebuild mode**, with side
   effects **suppressed** (the projected documents are still built — only the side effects are
   gated).
2. Hands off to **Continuous** execution from `N`, where side effects fire normally.

The net effect: side effects fire exactly once, only for events past `N` — the events the
previous version never saw.

### Semantics and Edge Cases

- **Trigger is "own progress `< prior mark`".** The gate keys off the new version's own
  persisted progression, so an interrupted warm-up **resumes suppressed** rather than
  re-emitting. Restarting the daemon after a crash mid-warm-up picks up from the recorded
  position with side effects still gated.
- **Failed warm-up pauses the shard.** If the suppressed replay throws (e.g. a poison event
  under a pausing error policy), the shard is left **paused** with the exception attached, no
  continuous execution starts, and no side effects fire. Restarting the daemon resumes the
  warm-up from its persisted progress.
- **`Version == 1` and the flag-off case are inert.** The gate is skipped entirely unless
  `Version > 1` **and** `GateSideEffectsBehindPriorVersion` is set — a v1 projection behaves
  exactly as it always has.
- **`SubscribeFromPresent` is incompatible.** A shard that subscribes from "present" ignores
  persisted progression, so the gate cannot reason about a prior mark. The gate is skipped and
  a warning is logged; use versioning-with-gate _or_ subscribe-from-present, not both.
- **Overlap window.** The prior mark `N` is snapshotted when the new version starts. If the
  **old** version is still running and advances past `N` while the new version warms up, the
  events in `(N, old_final]` can be processed by both versions — an accepted duplicate window.
  Stop the old version (or accept the small overlap) when you need exactly-once across the
  cutover.

::: tip
The gate suppresses **side effects**, not the projection write itself — the new version's
documents are fully rebuilt over the entire history. Only `RaiseSideEffects()` output
(published messages, emitted events, etc.) is held back during the warm-up.
:::

## Error Handling

The daemon uses Polly resilience pipelines for error handling. See [Resiliency Policies](/configuration/retries) for configuration.

## Architecture

```text
pc_events
  │ (Polling)
  ▼
High Water Mark Detector
  │ (Sequence Range)
  ▼
Event Loader
  ├──► Projection A ──► pc_doc_summary
  ├──► Projection B ──► pc_doc_dashboard
  └──► Subscription C ──► External System
         │
         ▼
  pc_event_progression (tracks progress for all)
```
