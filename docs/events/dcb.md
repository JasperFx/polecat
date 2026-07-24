# Dynamic Consistency Boundary (DCB)

The Dynamic Consistency Boundary (DCB) pattern allows you to query and enforce consistency across events from multiple streams using **tags** -- strong-typed identifiers attached to events at append time. This is useful when your consistency boundary doesn't align with a single event stream.

## Concept

In traditional event sourcing, consistency is enforced per-stream using optimistic concurrency on the stream version. DCB extends this by letting you:

1. **Tag** events with one or more strong-typed identifiers
2. **Query** events across streams by those tags
3. **Aggregate** tagged events into a view (like a live aggregation, but cross-stream)
4. **Enforce consistency** at save time -- detecting if new matching events were appended since you last read

Polecat uses a single append strategy (direct `INSERT` with `OUTPUT inserted.seq_id`), so DCB tags are always persisted immediately after each event insert. There are no separate append modes to configure -- tags just work.

## Registering Tag Types

Tag types are strong-typed identifiers (typically `record` types wrapping a primitive). Register them during store configuration:

<!-- snippet: sample_polecat_dcb_registering_tag_types -->
<!-- endSnippet -->

Each tag type gets its own table (`pc_event_tag_student`, `pc_event_tag_course`, etc.) with a composite primary key of `(value, seq_id)`.

### Tag Type Requirements

Tag types should be simple wrapper records around a primitive value:

<!-- snippet: sample_polecat_dcb_tag_type_definitions -->
<!-- endSnippet -->

Supported inner value types: `Guid`, `string`, `int`, `long`, `short`.

## Tagging Events

Use `BuildEvent` and `WithTag` to attach tags before appending:

<!-- snippet: sample_polecat_dcb_tagging_events -->
<!-- endSnippet -->

Events can have multiple tags of different types. Tags are persisted to their respective tag tables in the same transaction as the event.

## Querying Events by Tags

Use `EventTagQuery` to build a query, then execute it with `QueryByTagsAsync`:

<!-- snippet: sample_polecat_dcb_query_by_single_tag -->
<!-- endSnippet -->

### Multiple Tags (OR)

<!-- snippet: sample_polecat_dcb_query_multiple_tags_or -->
<!-- endSnippet -->

### Filtering by Event Type

<!-- snippet: sample_polecat_dcb_query_by_event_type -->
<!-- endSnippet -->

Events are always returned ordered by sequence number (global append order).

### Tag Predicates in LINQ (`HasTag`)

Tag predicates can also compose directly into a LINQ `Where()` over an event query, alongside ordinary event predicates (timestamp, event type, stream). Use the `IEvent.HasTag<TTag>(value)` marker method:

<!-- snippet: sample_polecat_dcb_has_tag_linq -->
<!-- endSnippet -->

`HasTag` compiles to the same tag-table subquery that `QueryByTagsAsync` emits (a correlated `seq_id IN (SELECT seq_id FROM pc_event_tag_{suffix} WHERE value = @p)` predicate), and under conjoined tenancy it is automatically tenant-scoped. AND-ing several `HasTag` calls with normal predicates is supported:

```cs
var events = await session.Events.QueryAllRawEvents()
    .Where(e => e.HasTag<StudentId>(alice)
             && e.HasTag<CourseId>(math)
             && e.Timestamp > cutoff)
    .ToListAsync();
```

For OR-across-tags or the richer tag/event-type interplay, keep using the `EventTagQuery` builder with `QueryByTagsAsync` — that remains the rich escape hatch. `HasTag` is a marker method recognized by the LINQ provider; calling it outside a Polecat event query throws `NotSupportedException`, and using an unregistered tag type throws `InvalidOperationException`.

## Aggregating by Tags

Build an aggregate from tagged events, similar to `AggregateStreamAsync` but across streams. First define an aggregate that applies the tagged events:

<!-- snippet: sample_polecat_dcb_aggregate -->
<!-- endSnippet -->

Then aggregate across streams by tag query:

<!-- snippet: sample_polecat_dcb_aggregate_by_tags -->
<!-- endSnippet -->

Returns `null` if no matching events are found.

### Identity-less Boundary Aggregates

The `StudentCourseEnrollment` aggregate above carries an `Id` property, so Polecat's source generator emits its evolver automatically and no extra annotation is needed.

Some DCB aggregates, though, are *pure boundary aggregates*: they span streams only by tag and have **no single-stream identity** -- no `Id` property and no `[AggregateIdentity]`. For these, mark the aggregate type with `[BoundaryAggregate]` (from `JasperFx.Events.Aggregation`) so the source generator emits an evolver for it:

```csharp
using JasperFx.Events.Aggregation;

[BoundaryAggregate]
public partial class CourseEnrollmentSummary
{
    public int EnrolledCount { get; private set; }
    public List<string> Students { get; } = new();

    public void Apply(StudentEnrolled e)
    {
        Students.Add(e.StudentName);
        EnrolledCount++;
    }

    public void Apply(StudentDropped e)
    {
        EnrolledCount--;
    }
}
```

Register it exactly like any other DCB aggregate -- the registration API is unchanged:

```csharp
opts.Events.RegisterTagType<CourseId>("course")
    .ForAggregate<CourseEnrollmentSummary>();
```

**Why the marker is required.** Without a single-stream identity the source generator can't infer a `TId`, so it *intentionally* emits nothing. A bare no-`Id` aggregate is far more often a forgotten `Id` property than a deliberate boundary aggregate, and silently generating an evolver would mask that mistake -- so `[BoundaryAggregate]` is the explicit opt-in. Without it, the DCB fetch/aggregate path fails fast at runtime:

```
InvalidProjectionException: No source-generated dispatcher found
for SingleStreamProjection<CourseEnrollmentSummary, string>
```

(The `string` type argument is vestigial -- it matches the `SingleStreamProjection<T, string>` the DCB aggregator builds and is never used by boundary-aggregate dispatch.)

**Placement.** Put `[BoundaryAggregate]` on the aggregate type itself, and keep the type `partial` so the generator can attach the emitted dispatcher. The attribute must sit on the type **in its own defining assembly** -- that is the compilation the generator emits the `[assembly: GeneratedEvolver]` into, and it is the assembly the runtime scans (`typeof(T).Assembly`) when resolving the evolver.

**Aggregates with an `Id` need no marker** and keep working unchanged -- `[BoundaryAggregate]` is only for the identity-less case.

::: tip
`[BoundaryAggregate]` requires JasperFx.Events 2.0.0-alpha.21 / JasperFx.Events.SourceGenerator 2.0.0-alpha.13 or later. The marker is a JasperFx.Events source-generator concern, so its behavior is backend-agnostic -- it works identically across the Critter Stack event stores; only the surrounding `RegisterTagType<...>().ForAggregate<T>()` registration is Polecat's SQL Server-backed API.
:::

## Fetch for Writing (Consistency Boundary)

`FetchForWritingByTags` loads the aggregate and establishes a consistency boundary. At `SaveChangesAsync` time, Polecat checks whether any new events matching the query have been appended since the read, throwing `DcbConcurrencyException` if so:

<!-- snippet: sample_polecat_dcb_fetch_for_writing_by_tags -->
<!-- endSnippet -->

### Handling Concurrency Violations

<!-- snippet: sample_polecat_dcb_handling_concurrency -->
<!-- endSnippet -->

::: tip
The consistency check only detects events that match the **same tag query**. Events appended to unrelated tags or streams will not cause a violation.
:::

## Checking Event Existence

If you only need to know whether any events matching a tag query exist -- without loading or deserializing them -- use `EventsExistAsync`. This is a lightweight existence check that avoids the overhead of fetching and materializing event data:

<!-- snippet: sample_polecat_dcb_events_exist_async -->
<!-- endSnippet -->

This is useful for guard clauses and validation logic in DCB workflows where you need to check preconditions before appending new events.

`EventsExistAsync` is also available in batch queries via `batch.EventsExist(query)`.

## How It Works

### Storage

Each registered tag type creates a table:

```sql
CREATE TABLE [dbo].[pc_event_tag_student] (
    value uniqueidentifier NOT NULL,
    seq_id bigint NOT NULL,
    CONSTRAINT pk_pc_event_tag_student PRIMARY KEY (value, seq_id),
    CONSTRAINT fk_pc_event_tag_student_events
        FOREIGN KEY (seq_id) REFERENCES [dbo].[pc_events](seq_id) ON DELETE CASCADE
);
```

### Consistency Check

At `SaveChangesAsync` time, Polecat executes an `EXISTS` query checking for new events matching the tag query with `seq_id > lastSeenSequence`. This runs in the same transaction as the event appends, providing serializable consistency for the tagged boundary.

### Tag Routing

Events appended via `IEventBoundary.AppendOne()` are automatically routed to streams based on their tags. Each tag value becomes the stream identity, so events with the same tag value end up in the same stream.
