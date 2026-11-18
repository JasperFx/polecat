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
.AddAsyncDaemon(DaemonMode.Solo)        // run the daemon in this host
.ApplyAllDatabaseChangesOnStartup();    // run schema migration at boot
```

::: tip
Async projections do **not** run unless the host starts a daemon. Use
`AddAsyncDaemon(DaemonMode.Solo)` for a single node. For more than one node, see
[Running on More Than One Node](#running-on-more-than-one-node).
:::

`AddAsyncDaemon` registers the projection coordinator as a hosted service, the same as Marten's does. On every
leadership cycle (`DaemonSettings.LeadershipPollingTime`, 5 seconds by default) the coordinator:

- takes the [shard locks](#the-hotcold-shard-lock) under `DaemonMode.HotCold`, so a shard runs on one node at a
  time and moves to another node when its node goes away
- restarts a shard that paused after a failure, such as a database outage that outlasted the resilience pipeline's
  retries
- picks up tenants added to, disabled in or removed from a
  [master table](/configuration/multitenancy#dynamic-tenant-management-master-table-tenancy)

| Mode | What runs in this host |
| ---- | ---------------------- |
| `DaemonMode.Solo` | Every shard, with no locks. This node assumes it is the only one. |
| `DaemonMode.HotCold` | The shards whose locks this node holds. |
| `DaemonMode.Disabled` | Nothing. |
| `DaemonMode.ExternallyManaged` | Nothing. Something else, such as Wolverine's managed distribution, runs the shards. |

Under `DaemonMode.Solo` the agents are running by the time the host has started, and a database that can't be
reached fails the host's startup. Under `DaemonMode.HotCold` they start in the background once the node wins their
locks, so a shard may not be running anywhere yet when the host finishes starting, and a database that can't be
reached is logged and retried on the next cycle.

`AddProjectionCoordinator(mode)` registers the same coordinator, and calling both still hosts only one. The one
difference is `DaemonMode.Solo`, where `AddProjectionCoordinator` takes the shard locks anyway and starts agents in
the background, as under `DaemonMode.HotCold`.

For a store registered with `AddPolecatStore<TStore>()`, both are on the expression it returns, and a host that has
no default store needs nothing else:

```cs
builder.Services.AddPolecatStore<IInvoicingStore>(opts =>
{
    // ...
})
.AddAsyncDaemon(DaemonMode.HotCold);
```

## Running on More Than One Node

There are three ways to run the daemon:

| Option | What it does |
| ------ | ------------ |
| `DaemonMode.Solo` | One node runs every shard. Use it when the application runs on one node. |
| `DaemonMode.HotCold` | One active node, for failover only. The first node to start takes every shard, and a second node only takes over a shard when the first one dies. It does not spread work across nodes. |
| Wolverine-managed distribution | The recommended choice for more than one node. |

Polecat will not add a distribution of its own. If you want async projections and subscriptions spread
across a cluster, use Wolverine's:

```cs
builder.Services.AddPolecat(opts =>
{
    opts.Connection("...");
    opts.Projections.Add<TripProjection>(ProjectionLifecycle.Async);
})
.IntegrateWithWolverine(m =>
{
    // Wolverine runs every async projection and subscription on exactly one node,
    // and spreads them across the cluster. Don't also call AddAsyncDaemon()
    m.UseWolverineManagedEventSubscriptionDistribution = true;
});
```

Wolverine-managed distribution gives you:

- an even spread of projection and subscription agents across the running nodes
- capacity-aware placement, so a node that is already busy is not given more
- blue/green-aware placement, so a new projection version only runs on the nodes that know it
- group affinity, so every shard for one tenant database stays on one node
- operator pause, restart and pin of a single agent
- a reason for any agent it cannot place

See [Projection/Subscription Distribution](https://wolverinefx.net/guide/durability/polecat/distribution) in
Wolverine's docs for the details.

[CritterWatch](https://critterwatch.jasperfx.net) is the tool for monitoring and managing projections
and subscriptions across the whole fleet. Wolverine-managed distribution works without it too.

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

## Graceful Shutdown and the Drain Timeout

When a projection or subscription shard is stopped, the daemon does not simply cancel it. It first tries to
*drain* the agent. `StopAndDrainTimeout` bounds how long the daemon waits for that drain on **a single** shard:

```cs
// The default is 5 seconds
opts.Projections.StopAndDrainTimeout = TimeSpan.FromSeconds(30);
```

A drain does three things, in this order:

1. **The page or range in flight is allowed to finish** and mark the shard's progression, so the next start picks
   up exactly where this one left off.
2. **Work already queued behind it is dropped.** Those events are not lost — nothing marked them complete, so
   whoever runs the shard next resumes them from the progression row.
3. **If the in-flight page is still running when the timeout expires, it is cancelled.** Its transaction rolls
   back and it does **not** mark progression, so it is re-processed on the next start.

The bound applies to every stop path: stopping one agent, stopping all agents (the `SIGTERM`/host shutdown
path), and the internal stop-if-already-running replacement that happens when an agent is reassigned.

::: warning Pages slower than the timeout are re-processed
Step 3 is a redelivery, and it is the trade-off to understand before leaving the timeout at its default. A page
that legitimately takes longer than `StopAndDrainTimeout` will be cancelled and run again on the next start —
every time the shard is stopped or reassigned.

For a projection this is usually invisible: applying the same events to the same aggregate is idempotent. For a
**[subscription](/events/subscriptions)** it means your side effects run twice, because what a subscription does
with an event is typically not idempotent and not inside the rolled-back transaction. Either raise the timeout
past your slowest page, or make the subscription's handling idempotent.
:::

**Why you would raise it.** Raise the timeout when in-flight batches legitimately take longer than five seconds
— a large `BatchSize`, expensive projection code, an external call inside a subscription, heavy rebuild load, or
a slow or contended SQL Server. This is most visible shutting down a host with a large agent universe: a
[database-per-tenant](/documents/multi-tenancy) deployment with thousands of (projection × tenant) shards all
draining inside a Kubernetes termination grace window.

::: tip
A per-shard bound is only useful if the process lives long enough to spend it. Match a raised
`StopAndDrainTimeout` with the host's own `HostOptions.ShutdownTimeout` and, on Kubernetes, the pod's
`terminationGracePeriodSeconds`.
:::

**Why you would lower it.** A deployment that would rather cut a wedged shard loose quickly and take the
reprocessing hit — to keep node failover and reassignment latency low, for instance — can set it below the
default. Under `DaemonMode.HotCold` this is what stops a node that has lost a shard's lock from continuing to
work its backlog while another node already owns the shard.

**Opting out.** `Timeout.InfiniteTimeSpan`, or any non-positive value, removes the separate bound so the drain
is limited only by the daemon's own cancellation. Be aware that this means a genuinely wedged shard can hold up
shutdown indefinitely.

::: tip Version note
The first Polecat release built on JasperFx 2.81.0 is the first in which the projection and subscription
*executions* actually honour this bound. Before it they awaited their whole queued backlog regardless, so a stopping shard could keep working for
far longer than `StopAndDrainTimeout` — on a HotCold node that had already lost the shard's lock, long after
another node had taken it over.
:::

## The HotCold Shard Lock

Under `DaemonMode.HotCold`, each node takes a SQL Server `sp_getapplock` per shard set to decide which node runs
it. On a single database that is one lock per shard. With a database per tenant it is one lock per database, and
every shard for that database runs on the node that holds it. Application locks are scoped to a database, so the
lock id is built from the event store's schema, and two stores in different schemas of one database don't compete
for it. Those holds are **session-scoped**: when SQL Server ends a node's session — a failover, a gateway
reconfiguration, a killed session — every lock goes with it. So the lock runs a monitor that periodically asks
SQL Server which of its locks it still holds, and drops the ones it has lost so the daemon stops those shards
instead of running them alongside the node that took them over.

The defaults are a 5 second interval and a 5 second probe timeout. They are configurable:

```cs
opts.Events.ConfigureAdvisoryLock = lockOptions =>
{
    lockOptions.MonitoringInterval = 10.Seconds();
    lockOptions.ProbeTimeout = 3.Seconds();
};
```

The probe costs nothing while a node holds no locks, so its load tracks the shards a node actually holds,
not the databases configured.

**Keep `ProbeTimeout` under `MonitoringInterval`.** Shutdown waits behind an in-flight probe, so the probe
timeout bounds daemon shutdown as much as it bounds a query.

::: danger Turning the monitor off reintroduces split brain
A non-positive or infinite `MonitoringInterval` starts no monitor at all. A node that then loses its session
goes on reporting its locks as held while another node takes them, and **both nodes run the same shard** —
the failure [weasel#695](https://github.com/JasperFx/weasel/issues/695) fixed, which in a three-replica load
test processed 22,000 outbox events more than once. The only reason to opt out is a deployment with so many
tenant databases that one parked timer each is itself the cost.
:::

There is no release timeout to configure, unlike Marten's PostgreSQL lock. A pooled `SqlConnection` keeps its
session — and its locks — when it closes, so a release abandoned on a deadline would leave the lock held
against the next node that needs it.

## Waiting for Non-Stale Data

### CatchUpAsync

Wait for all projections to catch up to the current high water mark:

```cs
await store.WaitForNonStaleProjectionDataAsync(TimeSpan.FromSeconds(30));
```

Every shard the store configures has to report, and every row in `pc_event_progression` has to reach the mark. The one exception is a row for another version of a projection the store still configures, such as the row V2 leaves behind when V3 takes over. Nothing advances that row, and the blue/green side effect gate needs it to stay, so the wait ignores it. A row for a projection you have removed altogether still counts, and the timeout names it, so delete it once you are done with it.

With per-tenant partitioned events (`UseTenantPartitionedEvents`) each tenant has its own sequence and its own row for each shard, so this waits for every tenant that has events, each up to that tenant's own highest sequence.

### Per-Query

Wait for projections before a specific query:

```cs
var orders = await session.Query<OrderSummary>()
    .QueryForNonStaleData()
    .Where(x => x.Status == "Active")
    .ToListAsync();
```

With per-tenant partitioned events this waits for the session's tenant only.

### Reading Your Own Writes

The waits above are for the highest sequence in the store, and on a busy system that includes everybody else's
events. If you just want to read back what you wrote, wait for your own events instead.
`session.LastAppendedPosition` tells you where the last commit's events are, and `WaitForProjectionsAsync` waits
for the named asynchronous projections to get that far:

```cs
await using var session = store.LightweightSession();
session.Events.StartStream(Guid.NewGuid(), new QuestStarted("Destroy the Ring"));
await session.SaveChangesAsync();

var result = await store.WaitForProjectionsAsync(
    session.LastAppendedPosition!, [typeof(QuestParty)], TimeSpan.FromSeconds(5));

if (result.Reached)
{
    // read the projected documents
}
```

The result is a value, not an exception. `TimedOut` means the projection is still behind your events, and
`Shards` says how far each shard got. A paused or rebuilding projection returns right away with `Paused` or
`Rebuilding`. The wait polls `pc_event_progression`, so it works on any node. You can pass projection names
instead of types, and an `ArgumentException` tells you when what you passed isn't an asynchronous projection.

The position is the database, the tenant and the sequence, and with per-tenant partitioned events the wait reads
that tenant's own row. It serializes as JSON, and `ToString()` / `EventStorePosition.Parse()` give you a compact
text form to pass along in a header.

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

A gap left by an append that rolled back never fills. The detector holds the mark below it until the gap has been held for longer than `DaemonSettings.StaleSequenceThreshold`, then skips it. That age is measured from when the running detector first saw the mark held there, so a daemon restart waits one fresh threshold.

### Open Transactions and READ_COMMITTED_SNAPSHOT

An append that is still in flight holds the mark as well, and `StaleSequenceThreshold` does not release it. Only a gap with nothing in it is skipped, so by default an event is not lost however long its transaction stays open. The cost is that a transaction that appends events and then stays open holds back every async projection and subscription on that database until it commits or rolls back, so keep those transactions short. `HighWaterAgent` logs a warning if the mark sits still for longer than `DaemonSettings.HighWaterProgressStallThreshold` with committed events above it.

The detector behaves the same on every database setting, including `READ_COMMITTED_SNAPSHOT`, which is on by default in Azure SQL Database, and optimized locking. It says how to read `pc_events` in its own queries rather than taking that from the database, and it never waits for an open transaction, so detection keeps running while the mark is held.

To release a mark held by a transaction you do not expect to finish, use `AdvanceHighWaterMarkToLatestAsync` below. Async projections and subscriptions will not see an event still in flight below the new mark if its transaction commits later (see [Events that commit late](#events-that-commit-late) for what Polecat does about that).

### Capping How Long an Open Transaction Can Hold the Mark

If you would rather the daemon released the mark by itself, set a cap:

```cs
opts.Projections.SkipStaleGapsDespiteLiveTransactionsAfter = TimeSpan.FromMinutes(5);
```

Once the mark has been held at one position for that long, the daemon moves it past the append that is holding it, up to the highest event committed when the mark was first held there, and logs a warning naming the sequence range. An append that opened after that is not stepped over, and has a cap of its own to wait out. Under per-tenant event partitioning each tenant's mark has its own clock.

::: warning
This is off by default, because it trades a stall for a missed event. If the skipped append goes on to commit, its events sit below the mark, and async projections and subscriptions that are already past them will not see them. Polecat records those events and rebuilds their streams in async single-stream projections ([Events that commit late](#events-that-commit-late)), but other projections and subscriptions are not repaired. Set the cap well above the longest transaction you expect to run.
:::

The clock is kept in memory by the running detector, so a daemon restart starts it again, which errs towards holding. The cap is only considered once the mark has been held for `StaleSequenceThreshold`, so it can't take effect sooner than that.

### Advancing the High Water Mark Manually

To move the mark straight to the highest committed event sequence, for example to jump-start a daemon that is held behind a gap, or to retrofit async projections onto a large existing event store:

```cs
// Every database the store reaches
await store.Advanced.AdvanceHighWaterMarkToLatestAsync(ct);

// One tenant
await store.Advanced.AdvanceHighWaterMarkToLatestAsync("tenant-a", ct);
```

::: warning
Async projections and subscriptions will not see an event below the new mark that has not committed yet, including an append that is still in flight, when it commits. Polecat records it as a [late event](#events-that-commit-late), but reach for this when you have established that a gap is genuinely dead, not as routine maintenance.
:::

The mark only ever moves forward, and it never moves past the highest committed `seq_id`. With per-tenant partitioned events (`UseTenantPartitionedEvents`), the daemon tracks one mark per tenant:
- The tenant overload advances that tenant's mark.
- The overload without a tenant advances every registered tenant's mark.

Without partitioning, there is one mark per database, and the tenant overload advances the mark of the database that tenant lives in. A running daemon picks up the new mark on its next detection cycle.

Tools that work against any Critter Stack store (CritterWatch, for one) use the store-agnostic form on `IEventStore` instead, added in JasperFx 2.92.0:

```cs
IEventStore eventStore = store;
var database = (await eventStore.AllDatabases()).Single();

// Read-only: the stored mark, the committed ceiling, and how many sequence numbers would be skipped
var previews = await eventStore.PreviewHighWaterAdvanceAsync(database, tenantId: null, ct);

// Refuses (RefusedStaleExpectation) if the mark moved after the preview
var results = await eventStore.AdvanceHighWaterMarkToLatestAsync(
    database, tenantId: null, expectedCurrentMark: previews[0].CurrentMark, ct);
```

These are stricter about the tenant than the `Advanced` convenience methods are:
- Without per-tenant partitioning, a database keeps one mark that every tenant shares, so a named tenant is refused rather than advancing every tenant's events.
- With partitioning, a null tenant selects every tenant's mark, and `expectedCurrentMark` then needs a named tenant.

### Events that commit late {#events-that-commit-late}

Every skip is recorded in `pc_high_water_skips`, with the reason for it and the sequence numbers inside the skipped
range that had no committed event yet. For an hour after a skip, the daemon checks those sequence numbers every 10
seconds. An event that turns up there is one that async projections and subscriptions already past it did not see. For
each one the daemon:

* records it in `pc_high_water_late_events` with its sequence, tenant, stream and event type,
* logs a warning and counts it in the `polecat.daemon.late_events` metric, tagged by event type, tenant and skip
  reason,
* rebuilds the event's stream in every async single-stream projection that handles that event type, through the shard
  that runs the projection, the same way as `RebuildSingleStreamAsync()`. Side effects do not run in that rebuild.

Other projections and subscriptions are not repaired, because they cannot safely take an event out of order. Use the
ledger to decide whether to rebuild them.

A range skipped because no append that could fill it was in flight should never fill. If one does, the daemon logs it
as an error, because that is a bug in Polecat's high water detection. Please report it.

The window and the interval are configurable:

```cs
opts.Projections.LateEventDetectionWindow = TimeSpan.FromHours(1);
opts.Projections.LateEventProbeInterval = TimeSpan.FromSeconds(10);
```

`opts.Events.EnableLateEventDetection = false` (or `RestoreV5Defaults()`) turns all of this off and leaves the two
tables out of the schema.

### Health Check

`AddPolecatHighWaterHealthCheck()`, from `Polecat.AspNetCore`, reports Unhealthy when the mark sits unchanged
with later events piling up past it for longer than `staleThreshold`, which means no node is advancing it. With
`autoRestart`, an Unhealthy result also restarts the high-water agent's poll loop, through the local projection
coordinator. A host with no coordinator, which is a host with no daemon, still gets the report, and the check says
it found nothing to restart.

```cs
builder.Services.AddHealthChecks()
    .AddPolecatHighWaterHealthCheck(staleThreshold: TimeSpan.FromSeconds(30), autoRestart: true);
```

That checks the default store registered with `AddPolecat()`. For a store registered with
`AddPolecatStore<TStore>()`, use the generic overload, which probes that store and restarts through that store's
own coordinator:

```cs
builder.Services.AddPolecat(opts =>
    {
        // ...
    })
    .AddAsyncDaemon(DaemonMode.HotCold);

builder.Services.AddPolecatStore<IInvoicingStore>(opts =>
{
    // ...
})
.AddAsyncDaemon(DaemonMode.HotCold);

builder.Services.AddHealthChecks()
    .AddPolecatHighWaterHealthCheck<IInvoicingStore>(
        staleThreshold: TimeSpan.FromSeconds(30),
        autoRestart: true);
```

Call it once per store. Each store's check is registered under its own name, `HighWaterHealthCheck:IInvoicingStore`,
with its own settings, so it can sit beside the default store's check.

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
independently. Above version 1 it is also baked into the name of an aggregate projection's
document table, so each version writes its own documents:

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

| Projection | `Version` | Progression identity (`pc_event_progression.name`) | Document table |
|------------|-----------|-----------------------------------------------------|----------------|
| `Trip`     | `1`       | `Trips:All`                                         | `pc_doc_trip`   |
| `Trip`     | `2`       | `Trips:V2:All`                                      | `pc_doc_trip_2` |
| `Trip`     | `3`       | `Trips:V3:All`                                      | `pc_doc_trip_3` |

Because each version has its own progression row and its own table, you can stand up a new
version alongside the old one and let it rebuild from zero into an empty table while the old
version keeps serving reads from its table. That's a blue/green deployment. Once nothing runs
the old version any more, its table and progression row are yours to drop.

The table suffix applies to the documents of single stream and multi stream projections (the
same rule as Marten's). An `EventProjection` stores whatever documents it chooses, so their
tables keep their names.

The members of a [composite projection](composite.md) don't keep a version of their own: each
one takes the composite's `Version`, so bump the composite to rebuild all of its members into
new tables.

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

## Start Floors

A projection can be told not to process history before a point. The point can be a configured
sequence (`SubscribeFromSequence`), a time (`SubscribeFromTime`), or the moment the projection first
runs:

```cs
opts.Projections.Add(new SingleStreamProjection<QuestParty, Guid>(), ProjectionLifecycle.Async,
    asyncConfiguration: o => o.SubscribeFromFirstRun());
```

::: warning
Persisted floors are **on by default since Polecat 6**, which adds the `pc_event_projection_floors` table.
`RestoreV5Defaults()` or `EnableProjectionFloors = false` turns them off. Without them, every projection starts
as it always has, `FetchProjectionFloorAsync` returns `null`, and `SubscribeFromFirstRun()` or
`SetProjectionFloorAsync` fail with a `NotSupportedException` that names the flag.
:::

`SubscribeFromFirstRun()` pins the high water mark as the projection's **floor** the first time it
starts and stores it in `pc_event_projection_floors`. Several nodes starting the same projection at once
all agree on one stored value. Every later start begins at the larger of the stored progress and the
floor, and a rebuild replays from the floor rather than from 0, because the floor lives in a table that
rebuild teardown and `Advanced.Clean` do not touch.

A stored floor wins over a code-configured `SubscribeFromSequence` or `SubscribeFromTime` for that
database, so an operator can fix one database without a deploy:

```cs
IEventDatabase database = store.Database;

await database.FetchProjectionFloorAsync("QuestParty", tenantId: null);
await database.SetProjectionFloorAsync("QuestParty", tenantId: null, floor: 1000);
await database.SetProjectionFloorAsync("QuestParty", tenantId: null, floor: null); // clear it
```

Floors are keyed by the projection name **without** its version, so a version bump inherits the floor.
Setting or clearing a floor does not reprocess anything by itself. The next start or rebuild honours
the new value.

## Rebuilding a Single Stream <Badge type="tip" text="6.0" />

When one stream's document is wrong, you don't have to rebuild the whole projection. `RebuildSingleStreamAsync`
folds that one stream again and commits the document:

```cs
await store.Advanced.RebuildSingleStreamAsync<TripSummary>(tripId);

// Under conjoined tenancy or a database per tenant
await store.Advanced.RebuildSingleStreamAsync<TripSummary>(tripId, "tenant-b");
```

A stream that folds to nothing (its aggregate was deleted, say) deletes the document, which is what a full rebuild
would leave behind.

When the document is written by an **asynchronous** single-stream projection, Polecat takes one of three paths
([#1060](https://github.com/JasperFx/polecat/issues/1060)), the same as Marten:

1. **A daemon of this store in this process runs the projection's shard.** The rebuild goes through that daemon and is
   bound to the shard's next batch. That batch re-folds the stream from its first event and commits the document
   together with the shard's progression, so the rebuild can't race the shard. The call returns when the batch
   commits.
2. **The shard is running on another node.** If the shard's progression row was written in the last 30 seconds by a
   running agent, the call throws `IdentityRebuildRefusedException` with
   `Reason == IdentityRebuildRefusal.ShardLiveOnAnotherNode`. Do the rebuild on the node that runs the projection.
3. **Otherwise** the stream is aggregated and stored right here, outside any daemon. That is only safe when no daemon is
   running the projection. Without extended progression tracking, a shard on another node that has had nothing to do
   for 30 seconds stops rewriting its row and looks stopped, so the call isn't refused.

Inline and live projections always take the third path, since there is no shard to race.

You can also rebuild one identity through a running daemon directly. It works for any single-stream projection the
daemon runs, and refuses with `IdentityRebuildRefusedException` when it can't (the shard isn't running there, is
rebuilding, or has never committed anything):

```cs
var result = await daemon.RebuildIdentityAsync("TripSummary", tenantId: null, identity: tripId, token);

// The document is now consistent with every event of its stream up to result.BoundCeiling
```

A projection whose documents are kept in custom projection storage, like an EF Core entity from
`Polecat.EntityFrameworkCore`, can't be rebuilt one identity at a time. Use `RebuildProjectionAsync()` for those.

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
