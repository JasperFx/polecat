# ProjectLatest — Include Pending Events

`ProjectLatest<T>()` returns the projected state of an aggregate including any events that have been
appended in the current session but not yet committed. This eliminates the need for a forced
`SaveChangesAsync()` + `FetchLatest()` round-trip when you need the projected result immediately
after appending events.

## Motivation

A common pattern in command handlers looks like this:

```csharp
// Today's pattern: forced flush + re-read
session.Events.StartStream<Report>(id, new ReportCreated("Q1"));
await session.SaveChangesAsync(ct);  // forced flush
var report = await session.Events.FetchLatest<Report>(id, ct);  // re-read
return report;
```

With `ProjectLatest`, this becomes:

```csharp
// Better: project locally including pending events
session.Events.StartStream<Report>(id, new ReportCreated("Q1"));
var report = await session.Events.ProjectLatest<Report>(id, ct);
// SaveChangesAsync happens later (e.g., Wolverine AutoApplyTransactions)
return report;
```

## API

```csharp
// On IDocumentSession.Events (IEventOperations)
ValueTask<T?> ProjectLatest<T>(Guid id, CancellationToken cancellation = default);
ValueTask<T?> ProjectLatest<T>(string key, CancellationToken cancellation = default);
```

## Behavior

1. Finds any pending (uncommitted) events for that stream in the current session
2. Fetches the current committed aggregate state from the database
3. Applies the pending events on top using the aggregate's Apply/Create methods
4. Returns the result

With pending events to apply, the committed state always comes from the database and never from an
identity map, because an aggregate with void `Apply` methods is changed in place. So the instance you get
back is your own: the session's cached copy, `stream.Aggregate` and `LoadAsync` still show only what has
been committed, and calling `ProjectLatest` twice, or again after a failed `SaveChangesAsync`, applies the
pending events once.

### When No Pending Events Exist

If there are no uncommitted events for the given stream in the session, `ProjectLatest` behaves
identically to `FetchLatest` — it returns the current committed state.

## Example

<!-- snippet: sample_polecat_project_latest_example -->
<a id='snippet-sample_polecat_project_latest_example'></a>
```cs
[Fact]
public async Task includes_pending_events_from_start_stream()
{
    var streamId = Guid.NewGuid();

    await using var session = theStore.LightweightSession();

    // Append events without committing
    session.Events.StartStream(streamId,
        new ReportCreated("Q1 Report"),
        new SectionAdded("Revenue"),
        new SectionAdded("Costs")
    );

    // ProjectLatest includes the pending events above
    var report = await session.Events.ProjectLatest<Report>(streamId, TestContext.Current.CancellationToken);

    report.ShouldNotBeNull();
    report.Title.ShouldBe("Q1 Report");
    report.SectionCount.ShouldBe(2);

    // SaveChangesAsync can happen later
    await session.SaveChangesAsync(TestContext.Current.CancellationToken);
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/project_latest_tests.cs#L46-L73' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_project_latest_example' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Merging Committed and Pending Events

`ProjectLatest` also works when the stream has previously committed events and new events
are appended in the current session:

<!-- snippet: sample_polecat_project_latest_merge_example -->
<a id='snippet-sample_polecat_project_latest_merge_example'></a>
```cs
[Fact]
public async Task includes_pending_events_after_committed_events()
{
    var streamId = Guid.NewGuid();

    // First, commit some events
    await using (var session = theStore.LightweightSession())
    {
        session.Events.StartStream(streamId,
            new ReportCreated("Q1 Report"),
            new SectionAdded("Revenue")
        );
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    // In a new session, append more events without committing
    await using (var session = theStore.LightweightSession())
    {
        session.Events.Append(streamId,
            new SectionAdded("Costs"),
            new SectionAdded("Outlook"),
            new ReportPublished()
        );

        // ProjectLatest merges the committed state with pending events
        var report = await session.Events.ProjectLatest<Report>(streamId, TestContext.Current.CancellationToken);

        report.ShouldNotBeNull();
        report.Title.ShouldBe("Q1 Report");
        report.SectionCount.ShouldBe(3);       // 1 committed + 2 pending
        report.IsPublished.ShouldBeTrue();      // from pending ReportPublished
    }
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/project_latest_tests.cs#L75-L111' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_project_latest_merge_example' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## String-Keyed Streams

`ProjectLatest` supports string-keyed streams as well:

```csharp
// For stores configured with StreamIdentity.AsString
session.Events.StartStream("report-123",
    new ReportCreated("Annual Report"),
    new SectionAdded("Overview")
);

var report = await session.Events.ProjectLatest<Report>("report-123");
```

## Limitations

- **Read-only sessions**: `ProjectLatest` is only available on `IDocumentSession.Events`
  (not `IQuerySession.Events`) because it needs access to the session's pending work tracker.

## ProjectLatestForCommit <Badge type="tip" text="6.0" />

`ProjectLatest` folds the pending events as they are before the commit: their versions, timestamps and the
rest of their metadata aren't set until `SaveChangesAsync()` sets them, so an aggregate that copies any of
that from its events doesn't get it. `ProjectLatestForCommit<T>()` gives the stream's pending events now the
versions, ids, timestamps, user name, correlation id, causation id and headers the commit would give them,
and the commit then keeps them. What it returns is exactly what `FetchLatest` returns once the session has
committed. Nothing is stored.

This is what an endpoint needs when it stores its response in the same transaction as its work, as
Wolverine's `[DeduplicatedWithResponse]` endpoints that return the updated aggregate do.

<!-- snippet: sample_polecat_project_latest_for_commit_aggregate -->
<a id='snippet-sample_polecat_project_latest_for_commit_aggregate'></a>
```cs
public record CommitShipmentStarted(string Origin);

public record CommitShipmentScanned(string Location);

// Copies the metadata of the last event it applied
public partial record CommitShipment
{
    public Guid Id { get; set; }
    public int Scans { get; set; }
    public Guid LastEventId { get; set; }
    public long LastEventVersion { get; set; }
    public DateTimeOffset LastModified { get; set; }
    public string? LastModifiedBy { get; set; }
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? TenantId { get; set; }

    public static CommitShipment Create(IEvent<CommitShipmentStarted> e) => new CommitShipment().track(e);

    public void Apply(IEvent<CommitShipmentScanned> e)
    {
        Scans++;
        track(e);
    }

    private CommitShipment track(IEvent e)
    {
        LastEventId = e.Id;
        LastEventVersion = e.Version;
        LastModified = e.Timestamp;
        LastModifiedBy = e.UserName;
        CorrelationId = e.CorrelationId;
        CausationId = e.CausationId;
        TenantId = e.TenantId;
        return this;
    }
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/project_latest_for_commit_tests.cs#L8-L48' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_project_latest_for_commit_aggregate' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

<!-- snippet: sample_polecat_project_latest_for_commit -->
<a id='snippet-sample_polecat_project_latest_for_commit'></a>
```cs
var stream = await session.Events.FetchForWriting<CommitShipment>(id);
stream.AppendOne(new CommitShipmentScanned("Leeds"));

// The aggregate as SaveChangesAsync() will write it, version, timestamps and metadata included
var shipment = await session.Events.ProjectLatestForCommit<CommitShipment>(id);

// ...store a response that carries it, in the same transaction

await session.SaveChangesAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/project_latest_for_commit_tests.cs#L201-L213' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_project_latest_for_commit' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

```csharp
ValueTask<T?> ProjectLatestForCommit<T>(Guid id, CancellationToken cancellation = default);
ValueTask<T?> ProjectLatestForCommit<T>(string key, CancellationToken cancellation = default);
```

The versions have to be known before the commit, so the stream must be one of these:

- started in this session with `StartStream()`
- fetched with `FetchForWriting()`
- appended to with an expected version, `Append(id, expectedVersion, ...)`

Anything else throws an `InvalidOperationException`, and leaves the pending events as they were.

A few things to know:

- **The commit refuses a stream whose expected version changes after the call**, because the aggregate it
  returned was built from versions the commit would no longer write. Call `ProjectLatestForCommit()` again
  after the last change to the stream.
- **It takes the stream's id.** A natural key can't be resolved to the stream's pending events before the
  commit, so a `string` on a `Guid` store throws.
- **The event sequence is still assigned at commit.** An aggregate that copies `IEvent.Sequence` doesn't get
  it.
- **Events appended after the call aren't covered by it.** Call it again to include them. The events the first
  call prepared keep what they were given.
- **Session metadata set after the call** is filled in at commit for any event that doesn't have it yet, as
  usual.
