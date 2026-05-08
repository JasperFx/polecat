# Composite Projections

Composite projections orchestrate multiple projection stages that must run in a specific order. Within each stage, projections can run in parallel.

## How It Works

A composite projection defines stages:

1. **Stage 1**: Projections A and B run in parallel
2. **Stage 2**: Projection C runs after Stage 1 completes (it depends on A and B)
3. **Stage 3**: Projections D and E run in parallel after Stage 2

All stages share a single `IProjectionBatch` that is flushed to SQL Server **once**,
after every stage has run. This is what makes a composite update atomic.

## Defining a Composite Projection

```cs
opts.Projections.CompositeProjectionFor("OrderComposite", composite =>
{
    composite.Add<OrderProjection>();                  // stage 1
    composite.Add<InventoryProjection>();              // stage 1 (parallel)
    composite.Add<DashboardProjection>(stageNumber: 2); // stage 2 (depends on stage 1)
});
```

You can also register self-aggregating snapshot projections directly:

```cs
opts.Projections.CompositeProjectionFor("OrderSnapshots", composite =>
{
    composite.Snapshot<Order>();                  // stage 1
    composite.Snapshot<OrderSummary>(stageNumber: 2);
});
```

## Lifecycle

Composite projections always run asynchronously. They flow through the standard async daemon, so you must enable it on the host:

```cs
builder.Services.AddPolecat(opts =>
{
    opts.Connection("...");
    opts.Projections.CompositeProjectionFor("OrderComposite", composite =>
    {
        composite.Add<OrderProjection>();
        composite.Add<DashboardProjection>(2);
    });
})
.AddAsyncDaemon(DaemonMode.Solo)
.ApplyAllDatabaseChangesOnStartup();
```

## Thread Safety

The `PolecatProjectionBatch` used by composite projections uses `ConcurrentBag` and `ConcurrentQueue` internally to safely handle parallel projections within a stage.

## Cross-stage document visibility

::: warning
A downstream stage **cannot** see the document writes of an upstream stage by issuing
a SQL query against `IQuerySession` — those writes are still queued on the shared
in-memory projection batch and have not been committed yet. The query goes to SQL
Server, which has not received them.
:::

All stages of a composite share one `IProjectionBatch` that flushes once, after every
stage has run. This is what makes a composite atomic, but it also means that during a
later stage's `EnrichEventsAsync`, the document writes produced by earlier stages are
still queued in memory. A query like:

```cs
// Inside a stage-2 projection's EnrichEventsAsync — DOES NOT see Order
// rows written by an upstream stage-1 projection in this same batch
var orders = await querySession.Query<Order>().ToListAsync();
```

will return only what was committed by **previous** batches. During a projection
rebuild, where every event is replayed from scratch, neither the upstream nor the
downstream documents have been committed yet, so the query returns an empty result.

Polecat (via JasperFx.Events 1.35.0+) supports four ways for a downstream stage to
consume upstream stage output:

* **`Updated<T>` and `ProjectionDeleted<T, TId>` synthetic events.** When an upstream
  `SingleStreamProjection<T>` or `MultiStreamProjection<T>` updates or deletes a
  document, JasperFx injects a synthetic event into the downstream stage's event
  stream. The current snapshot of `T` is carried directly on the event payload, so no
  database lookup is needed.
* **`EnrichWith<T>().ForEvent<E>().ForEntityId(...).AddReferences()`** (and the
  related `EnrichAsync` overloads). These walk the upstream's in-memory aggregate
  cache for `T` rather than the database, so they observe in-flight writes from
  earlier stages in the same batch.
* **`group.TryFindUpstreamCache<TId, T>(out var cache)`** for custom enrichment
  callbacks (notably inside `EnrichUsingEntityQuery`) that need to look up an
  in-flight upstream entity by id when it isn't the type of the enclosing
  `EnrichWith<T>`. Returns `false` when no upstream stage of this composite produces
  entities of that type — see [the example below](#looking-up-arbitrary-upstream-entities).
* **`group.ReferencePeerView<T>()`** for a parallel projected view that shares the
  same identity as the projection being built.

Direct use of `querySession.Query<T>()` from inside `EnrichEventsAsync` is
appropriate for **static reference data committed in earlier batches** and not for
documents produced by upstream stages of the *current* batch.

### Looking up arbitrary upstream entities

`EnrichUsingEntityQuery`'s callback receives a cache parameter typed for the
enclosing `EnrichWith<T>`. When the callback also needs to read an in-flight upstream
entity of a *different* type — for example an `OrderShippingNotification` enrichment
that needs to consult the upstream `Order` that is being projected in the same batch
— call `group.TryFindUpstreamCache<TId, T>` against the captured `SliceGroup` to
reach into the upstream stage's in-memory aggregate cache.

<!-- snippet: sample_polecat_try_find_upstream_cache -->
<!-- endSnippet -->

`TryFindUpstreamCache` returns `false` when no upstream stage of this composite is
registered as producing entities of that type, and the cache it returns is a hint —
`IAggregateCache.TryFind` may still miss for entities outside the cache window
(`Options.CacheLimitPerTenant`), in which case the caller should fall back to
whatever is appropriate for that data.

::: tip
`CacheLimitPerTenant` is a *memory* tunable, not a correctness knob. As of
JasperFx.Events 1.35.0 the upstream cache is held until every composite stage has
completed, so a tiny `CacheLimitPerTenant` no longer starves a downstream stage.
See `composite_try_find_upstream_cache_tests.tiny_upstream_cache_limit_does_not_starve_downstream_stage`
for the regression coverage.
:::

### Fan-out enrichment with `ForEntityIds`

When a single event references **multiple** entities of the same type — for example
a `BatchTransfer` event carrying a list of account ids — use the
`ForEntityIds` (or `ForEntityIdsFromEvent`) variant of `EnrichWith` to fan out the
lookup:

```cs
public class TransferEnrichingProjection : MultiStreamProjection<TransferSummary, Guid>
{
    public TransferEnrichingProjection()
    {
        EnrichWith<Account>()
            .ForEvent<BatchTransfer>()
            .ForEntityIds(e => e.AccountIds)
            .AddReferences();
    }
}
```

This produces one `References<Account>` event per id per slice, so the projection's
`Apply` / `Evolve` method can read each referenced entity directly. Duplicates within
a single event are passed through to the application callback as-is; ids are
de-duplicated only when fetching from storage to avoid redundant loads.

## Use Cases

- **Dependent read models** — Dashboard that depends on individual aggregates
- **Multi-stage processing** — Transform data through a pipeline
- **Performance optimization** — Parallelize independent projections within a stage

## Things to Know

- Composite projections can include any kind of projection (single-stream, multi-stream, event projections, flat-table projections).
- Composite projections can only run asynchronously.
- In `pc_event_progression`, you will see rows for both the parent composite and every constituent projection — they should never disagree.
- You can use as many stages as you wish, but two or three is usually enough.
- If you rebuild a composite projection, you have to rebuild every constituent projection.
