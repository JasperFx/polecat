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
<a id='snippet-sample_polecat_dcb_registering_tag_types'></a>
```cs
public override async ValueTask InitializeAsync()
{
    await StoreOptions(opts =>
    {
        // Register tag types -- each gets its own table (pc_event_tag_student, pc_event_tag_course)
        opts.Events.RegisterTagType<StudentId>("student")
            .ForAggregate<StudentCourseEnrollment>();
        opts.Events.RegisterTagType<CourseId>("course")
            .ForAggregate<StudentCourseEnrollment>();
    });
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L77-L89' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_registering_tag_types' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Each tag type gets its own table (`pc_event_tag_student`, `pc_event_tag_course`, etc.) with a composite primary key of `(value, seq_id)`.

### Tag Type Requirements

Tag types should be simple wrapper records around a primitive value:

<!-- snippet: sample_polecat_dcb_tag_type_definitions -->
<a id='snippet-sample_polecat_dcb_tag_type_definitions'></a>
```cs
// Strong-typed tag identifiers
public record StudentId(Guid Value);
public record CourseId(Guid Value);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L9-L13' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_tag_type_definitions' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Supported inner value types: `Guid`, `string`, `int`, `long`, `short`.

### Registering Tag Types under Trimming or Native AOT

`RegisterTagType<TTag>()` finds the tag's inner value by reflecting over the tag type, so it is marked
`[RequiresUnreferencedCode]` and `[RequiresDynamicCode]`, and an application analyzed for trimming or
Native AOT gets IL2026/IL3050 warnings where it calls it. Pass the unwrapper explicitly instead, which
builds the registration from `TagTypeRegistration.FromUnwrapper<TTag, TInner>` with no reflection:

```cs
opts.Events.RegisterTagType<StudentId, Guid>(x => x.Value, "student")
    .ForAggregate<StudentCourseEnrollment>();
```

The table suffix is optional and defaults to the tag type's name.

## Tagging Events

Use `BuildEvent` and `WithTag` to attach tags before appending:

<!-- snippet: sample_polecat_dcb_tagging_events -->
<a id='snippet-sample_polecat_dcb_tagging_events'></a>
```cs
var enrolled = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
enrolled.WithTag(studentId, courseId);
theSession.Events.Append(streamId, enrolled);
await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L98-L103' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_tagging_events' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Events can have multiple tags of different types. Tags are persisted to their respective tag tables in the same transaction as the event.

## Querying Events by Tags

Use `EventTagQuery` to build a query, then execute it with `QueryByTagsAsync`:

<!-- snippet: sample_polecat_dcb_query_by_single_tag -->
<a id='snippet-sample_polecat_dcb_query_by_single_tag'></a>
```cs
var query = new EventTagQuery().Or<StudentId>(studentId);
var events = await session2.Events.QueryByTagsAsync(query, TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L106-L109' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_query_by_single_tag' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### Multiple Tags (OR)

<!-- snippet: sample_polecat_dcb_query_multiple_tags_or -->
<a id='snippet-sample_polecat_dcb_query_multiple_tags_or'></a>
```cs
var query = new EventTagQuery()
    .Or<StudentId>(student1)
    .Or<StudentId>(student2);

var events = await session2.Events.QueryByTagsAsync(query, TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L135-L141' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_query_multiple_tags_or' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### Filtering by Event Type

<!-- snippet: sample_polecat_dcb_query_by_event_type -->
<a id='snippet-sample_polecat_dcb_query_by_event_type'></a>
```cs
var query = new EventTagQuery()
    .Or<AssignmentSubmitted, StudentId>(studentId);

var events = await session2.Events.QueryByTagsAsync(query, TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L162-L167' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_query_by_event_type' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Events are always returned ordered by sequence number (global append order).

### Tag Predicates in LINQ (`HasTag`)

Tag predicates can also compose directly into a LINQ `Where()` over an event query, alongside ordinary event predicates (timestamp, event type, stream). Use the `IEvent.HasTag<TTag>(value)` marker method:

<!-- snippet: sample_polecat_dcb_has_tag_linq -->
<a id='snippet-sample_polecat_dcb_has_tag_linq'></a>
```cs
var events = await session.Events.QueryAllRawEvents()
    .Where(e => e.HasTag<StudentId>(alice))
    .ToListAsync(TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_tag_linq_where_tests.cs#L50-L56' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_has_tag_linq' title='Start of snippet'>anchor</a></sup>
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
<a id='snippet-sample_polecat_dcb_aggregate'></a>
```cs
// Aggregate for DCB
public partial class StudentCourseEnrollment
{
    public Guid Id { get; set; }
    public string StudentName { get; set; } = "";
    public string CourseName { get; set; } = "";
    public List<string> Assignments { get; set; } = new();
    public bool IsDropped { get; set; }

    public void Apply(StudentEnrolled e)
    {
        StudentName = e.StudentName;
        CourseName = e.CourseName;
    }

    public void Apply(AssignmentSubmitted e)
    {
        Assignments.Add(e.AssignmentName);
    }

    public void Apply(StudentDropped e)
    {
        IsDropped = true;
    }
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L28-L54' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_aggregate' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Then aggregate across streams by tag query:

<!-- snippet: sample_polecat_dcb_aggregate_by_tags -->
<a id='snippet-sample_polecat_dcb_aggregate_by_tags'></a>
```cs
var query = new EventTagQuery()
    .Or<StudentId>(studentId)
    .Or<CourseId>(courseId);

var aggregate = await session2.Events.AggregateByTagsAsync<StudentCourseEnrollment>(query, TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L189-L195' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_aggregate_by_tags' title='Start of snippet'>anchor</a></sup>
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
<a id='snippet-sample_polecat_dcb_fetch_for_writing_by_tags'></a>
```cs
await using var session2 = theStore.LightweightSession();
var query = new EventTagQuery().Or<StudentId>(studentId);
var boundary = await session2.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, TestContext.Current.CancellationToken);

// Read current state
var aggregate = boundary.Aggregate; // may be null if no events yet
var lastSequence = boundary.LastSeenSequence;

// Append via boundary
var assignment = session2.Events.BuildEvent(new AssignmentSubmitted("HW1", 95));
assignment.WithTag(studentId, courseId);
boundary.AppendOne(assignment);

// Save -- will throw DcbConcurrencyException if another session
// appended matching events after our read
await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L214-L231' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_fetch_for_writing_by_tags' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### Handling Concurrency Violations

<!-- snippet: sample_polecat_dcb_handling_concurrency -->
<a id='snippet-sample_polecat_dcb_handling_concurrency'></a>
```cs
public static async Task handling_a_concurrency_violation(IDocumentSession session)
{
    try
    {
        await session.SaveChangesAsync();
    }
    catch (DcbConcurrencyException violation)
    {
        // Reload and retry -- the boundary's tag query had new matching events since the read.
        // violation.Query is the original tag query, violation.LastSeenSequence the sequence
        // the boundary was read at.
        Console.WriteLine($"DCB violation on {violation.Query} at {violation.LastSeenSequence}");
    }
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L274-L289' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_handling_concurrency' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

::: tip
The consistency check only detects events that match the **same tag query**. Events appended to unrelated tags or streams will not cause a violation.
:::

::: warning
A session can establish more than one consistency boundary. If **several** of them are violated in the
same `SaveChangesAsync()`, Polecat throws an `AggregateException` carrying one
`DcbConcurrencyException` per violated boundary rather than a single exception. A session with one
boundary -- the common case, and what the sample above shows -- always throws the
`DcbConcurrencyException` directly.
:::

## Checking Event Existence

If you only need to know whether any events matching a tag query exist -- without loading or deserializing them -- use `EventsExistAsync`. This is a lightweight existence check that avoids the overhead of fetching and materializing event data:

<!-- snippet: sample_polecat_dcb_events_exist_async -->
<a id='snippet-sample_polecat_dcb_events_exist_async'></a>
```cs
[Fact]
public async Task events_exist_returns_true_when_matching_events_found()
{
    var studentId = new StudentId(Guid.NewGuid());
    var courseId = new CourseId(Guid.NewGuid());
    var streamId = Guid.NewGuid();

    var enrolled = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
    enrolled.WithTag(studentId, courseId);
    theSession.Events.Append(streamId, enrolled);
    await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

    // Check existence -- lightweight, no event loading
    await using var session2 = theStore.LightweightSession();
    var query = new EventTagQuery().Or<StudentId>(studentId);
    var exists = await session2.Events.EventsExistAsync(query, TestContext.Current.CancellationToken);
    exists.ShouldBeTrue();
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/dcb_documentation_samples.cs#L291-L310' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_dcb_events_exist_async' title='Start of snippet'>anchor</a></sup>
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
    CONSTRAINT fk_pc_event_tag_student_seq_id
        FOREIGN KEY (seq_id) REFERENCES [dbo].[pc_events](seq_id) ON DELETE CASCADE
);

CREATE INDEX ix_pc_event_tag_student_seq_id ON [dbo].[pc_event_tag_student] (seq_id);
```

The primary key answers "which events carry this tag". The index answers the opposite question, which
every delete from `pc_events` asks through the foreign key. A store whose tag tables predate it gets it
from its next schema migration as a plain `CREATE INDEX`, which holds a lock on the tag table while it
builds. To avoid that on a large table, build it yourself first with `ONLINE = ON` under the same name and
columns, and the migration leaves it alone.

The table's shape follows the event store's:

| Configuration | Difference |
| :--- | :--- |
| `TenancyStyle.Conjoined` | `tenant_id` joins the primary key |
| `UseTenantPartitionedEvents` | Also carries `tenant_ordinal`, and the foreign key is on `(seq_id, tenant_ordinal)`, because `seq_id` is numbered per tenant. The tag table itself is not partitioned |
| `UseArchivedStreamPartitioning` | `is_archived` joins the primary key and there is no foreign key. Polecat deletes the tag rows itself wherever it deletes events |

### Archived Events

`QueryByTagsAsync`, `EventsExistAsync`, `AggregateByTagsAsync` and `FetchForWritingByTags` leave out
archived events, as every other event query does. Archiving or un-archiving a stream also moves the
[version](#consistency-check) of each tag on its events, so it conflicts with a boundary that read those
tags: the boundary's decision was made from a set of events that has since changed. Tombstoning a stream
conflicts for the same reason, as [described below](#what-a-tombstone-erases).

The event store explorer's tag lookup (`IEventStore.QueryByTagsAsync`) still shows archived events,
because it is a diagnostic surface, like its stream reads.

::: warning
Through 5.47.0 these reads included archived events, so an application that relied on that will see
fewer events and may see `EventsExistAsync` answer `false`.
:::

### Consistency Check

Polecat keeps a side table, `pc_dcb_tag_version`, holding a version per tag value that moves whenever an
event carrying that tag value is appended. `FetchForWritingByTags` reads the version for every tag the query
names. At `SaveChangesAsync` time, before any events are inserted, Polecat checks those versions have not
moved. A boundary's save fails with `DcbConcurrencyException` exactly when an event its query matches was
appended after it read, which is the DCB specification's append condition.

**Reading a tag is not writing it.** The check locks each row the boundary read and compares it, and does
not move it. Only the tags on the events the save appends move a version. So two boundaries over the same
tag value both commit as long as neither appends an event the other's query matches. Through 5.x the check
also moved every version it read, so any two boundaries over one tag value conflicted (#972).

A row the boundary read and the save also appends under is checked and moved by one
`UPDATE ... WHERE version = @captured`. A row it only read is read `WITH (UPDLOCK, ROWLOCK)` and compared.
Either way concurrent saves over one row queue on its lock, and the second one compares against what the
first committed. The update lock does not hold up a `FetchForWritingByTags` read of the row, and it is a
locking read, so a check under `READ_COMMITTED_SNAPSHOT` still waits for and sees an append in flight. Every
row a save checks or moves is locked in one sorted pass, so two saves that each read the tag the other
appends under lose with a `DcbConcurrencyException` and never deadlock.

Every save that appends a tagged event moves the version, boundary or not — so an ordinary
`session.Events.Append(streamId, taggedEvent)` invalidates an in-flight boundary just as another
boundary save would.

A save that appends nothing does **not** check its boundaries. A boundary is a condition on an append,
so a session that read one and then wrote nothing has nothing for it to guard.

Every version a commit moves comes from one sequence per schema, `pc_dcb_tag_version_sequence`, so a value
never repeats, even on a row that was deleted and created again. The one other value a row holds is 0, which
a boundary's check writes when the row did not exist yet, so that it has a row to lock. 0 is also what a
missing row reads as, and both mean that nothing carrying the tag has been appended since. That is what lets a tombstone delete rows. The sequence
is created by the ordinary schema migration, as long as a tag type is registered, and starts above anything
a row could have counted up to. Rows written by earlier versions keep their values until the next commit
moves them. Under `AutoCreate.None`, apply the migration before deploying: with a tag type registered, a
tagged append, an archive or a tombstone can't run without the sequence. A login with `db_datawriter` can
draw from it. One with grants on individual tables needs `UPDATE` on the sequence too.

### Conflict Scope: Event Types Count

A query condition can name event types as well as a tag value, as `Or<AssignmentSubmitted, StudentId>(id)`
and `AndEventsOfType<...>()` do. By default an event of a type the condition does **not** name is not a
conflict, even when it carries the condition's tag value. The boundary never read it, and the DCB
specification's append condition is defined by what the query matches. To do that, Polecat keeps a second
version per (tag value, event type), a row in the same table keyed `{tag suffix}:{event type name}`, and a
condition naming event types is checked against those rows.

`opts.Events.DcbConflictScope` controls this:

| `DcbConflictScope` | Checks | Maintains per-event-type versions |
| :--- | :--- | :--- |
| `TagValueAndEventType` (default) | per event type where the condition names types, per tag value where it doesn't | yes |
| `TagValueRecordingEventTypes` | per tag value | yes |
| `TagValue` | per tag value, whatever the event type (what Polecat 5 did) | no |

```cs
var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = connectionString;
    opts.Events.RegisterTagType<StudentId>("student");

    // What Polecat 5 did: any event carrying the tag value conflicts, whatever its type
    opts.Events.DcbConflictScope = DcbConflictScope.TagValue;
});
```

The per-event-type versions cost one more row write per (tag value, event type) pair a save appends. The
version table holds roughly one more row per (tag value, event type) pair ever appended. See
[`src/DcbLoadTest/RESULTS-6.0.md`](https://github.com/JasperFx/polecat/blob/main/src/DcbLoadTest/RESULTS-6.0.md)
for measurements.

::: warning Rolling out the event-type scope
A node checking per-event-type versions can't see appends from a node that doesn't maintain them: a Polecat
5 node, or one configured with `TagValue`. While both run, a conflict can be missed. Either stop every node
that appends tagged events before starting Polecat 6, or roll out Polecat 6 with
`DcbConflictScope.TagValueRecordingEventTypes` first and switch to the default once no older node is left.
Events appended before the switch have no per-event-type version, which reads as 0. That is safe, and a
boundary over them runs one more existence check on its save until the next append of that type moves the
version.
:::

### What a Tombstone Erases

A tag can be personal data, and `pc_dcb_tag_version` holds the value as text. So `TombstoneStream` deletes
the row of every tag on the stream that no other stream in the tenant carries, in the same transaction as
the events, and its tag rows go with the events. Once a tombstone commits, nothing in
`pc_event_tag_*` or `pc_dcb_tag_version` holds a value that only that stream carried.

- **A tag another stream carries keeps its row.** The row is shared by value, and the other stream still
  stores the tag, so deleting it would erase nothing. Its version moves instead. The last stream to go takes
  it. An archived stream counts as carrying its tags, because its events are still stored.
- **A tombstone never creates a row.** A tag with no row stays that way. A tag can have events and no row
  if they were written before this table existed, or tagged later with `AssignTagWhere`.
- **A boundary still can't save through it.** A boundary that captured a version finds the row moved or
  gone. A boundary that read events under a tag with no row has no row to lose against, so its save checks
  that the events it read are still there and throws `DcbConcurrencyException` if they aren't. That check
  costs one more statement, only on a save whose boundary read events and had a tag with no row.
- **A concurrent append can't be lost.** A tombstone takes the row before it asks whether another stream
  carries the tag, and a save takes the row before it inserts an event. So the two queue, and the tombstone
  sees the appended event or the append creates a new row with a new version.
- **Tenants are separate.** The row is per tenant, and only the tombstoned stream's tenant is considered.
- **Per-event-type rows go the same way.** A `{suffix}:{event type}` row is deleted once no other stream
  in the tenant carries the tag value on an event of that type, whatever `DcbConflictScope` is configured.

What this doesn't do:

- **Archiving isn't erasure.** `ArchiveStream` and `UnArchiveStream` keep the events, so they move rows and
  create missing ones.
- **Compaction doesn't erase either.** It removes events without touching rows, so a tag that only compacted
  events carried keeps its row, and tombstoning the stream afterwards can't find it. Delete such a row
  yourself. `CleanAllEventDataAsync` and `DeleteAllTenantDataAsync` take the rows too.
- **It doesn't look outside the tag tables.** A value you copied into a projected document or a backup is
  yours to remove, and the transaction log keeps what it keeps until it is truncated. For a stream you have
  to keep, use `ApplyEventDataMasking`, which doesn't touch tags.
- **It doesn't cover a rolling upgrade from 5.47.0 while it runs.** An older node still counts up from the
  current value, doesn't delete rows, and can create a row at 1. Finish the rollout before relying on this.
  A row an older node's tombstone leaves behind stays until you delete it.

::: warning Marten differs here
Marten keeps `mt_dcb_tag_version` rows through a stream delete, and its counters restart if a row is
removed. Code moving between the two should not assume a tombstone erases tag values on Marten.
:::

::: warning
Through 5.20.0 this check was an `EXISTS` query over the tag tables. That is a non-locking predicate read:
concurrent savers each ran it before any of them had committed, so none could see the others, and their
event inserts did not collide either because a boundary routinely spans distinct streams. Every racer
committed. If you rely on DCB boundaries under concurrent load, upgrade — and note that the new table is
created by the ordinary schema migration, with no data migration needed.
:::

### Tag Routing

Events appended via `IEventBoundary.AppendOne()` are automatically routed to streams based on their tags. Each tag value becomes the stream identity, so events with the same tag value end up in the same stream.
