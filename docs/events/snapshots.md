# Snapshot Projections

`Projections.Snapshot<T>()` is a convenient shortcut for registering a self-aggregating
projection: it builds and registers a [`SingleStreamProjection<T, TId>`](/events/projections/single-stream-projections)
internally, with the identity type resolved automatically from the document's `Id` property.

::: tip How it works under the hood
`Snapshot<T>()` is purely a registration shortcut — it does **not** introduce a separate
"snapshot" storage path. Internally it builds a `SingleStreamProjection<T, TId>` against the
appropriate identity type and registers it through the same projection pipeline as any other
projection. The aggregate is persisted to the document table for type `T` (the standard
`pc_doc_{type}` table), not to a special snapshot column on `pc_streams`.

This mirrors Marten's `Snapshot<T>()` API exactly so the two ecosystems stay aligned.
:::

## Registration

The aggregate type **must be self-aggregating** — i.e. it must have its own static
`Create` and instance `Apply` methods (or implement the appropriate aggregation conventions).

```cs
public class QuestParty
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public List<string> Members { get; set; } = new();

    public static QuestParty Create(QuestStarted e) => new() { Name = e.Name };
    public void Apply(MembersJoined e) => Members.AddRange(e.Members);
}

var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    // Inline — snapshot updated in the same transaction as the events
    opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Inline);
});
```

If you need to subclass `SingleStreamProjection<TDoc, TId>` to override behavior, register
it directly via `opts.Projections.Add<MyProjection>(ProjectionLifecycle.Inline)` instead.

## Lifecycles

```cs
// Updated in the same transaction as the appended events
opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Inline);

// Updated asynchronously by the async projection daemon
opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
```

`SnapshotLifecycle.Live` is intentionally not supported — for live aggregation, use
[`AggregateStreamAsync<T>()`](/events/projections/live-aggregates) directly without registering
a projection.

## Reading the Aggregate

Because `Snapshot<T>()` registers a regular `SingleStreamProjection`, you read the result
the same way you would for any other projected document:

```cs
await using var session = store.QuerySession();

// Loads from the projected document table (pc_doc_questparty)
var party = await session.LoadAsync<QuestParty>(streamId);

// Or fetch the latest version from the event store
var latest = await session.Events.FetchLatest<QuestParty>(streamId);
```

For an aggregate registered with an `Inline` lifecycle, `FetchLatest<T>()` reads the projected
document rather than re-aggregating the stream — the same thing `LoadAsync<T>()` does, and the same
route Marten takes. It therefore returns `null` for a stream that exists but belongs to some other
aggregate sharing the key space, because the inline projection never wrote a document for a stream it
does not own. That makes `FetchLatest<T>(id) is null` a reliable "does this aggregate exist?" probe,
which is what code branching between `StartStream` and `Append` depends on.

This matters most for an aggregate whose only handler is a catch-all `Evolve(IEvent)`: a catch-all
accepts every event type at the method level, so live aggregation over a foreign stream would
otherwise hand back a default-constructed instance — and default property values read as real state,
not as absence.

Aggregates on the `Live` and `Async` lifecycles still aggregate the stream, since there is no
committed document to read.

## Snapshot in a Composite Projection

Composite projections support the same shortcut via `composite.Snapshot<T>()`:

```cs
opts.Projections.CompositeProjectionFor("UserLifecycle", composite =>
{
    composite.Snapshot<User>();          // stage 1 (default)
    composite.Snapshot<UserStats>(2);    // stage 2
});
```

Inside a composite projection, snapshots always run as `Async` — composite projections are
themselves async-only.

## Choosing Between Snapshot and Add

- Use **`Snapshot<T>()`** when `T` is a self-aggregating aggregate type with conventional
  `Create`/`Apply` methods. This is the simplest registration.
- Use **`Add<TProjection>(...)`** when you have a dedicated `SingleStreamProjection<TDoc, TId>`
  subclass that overrides projection behavior, or when registering any other projection type
  (multi-stream, event projections, flat tables, etc).
