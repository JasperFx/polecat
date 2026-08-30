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

1. Fetches the current committed aggregate state from the database via `FetchLatest<T>()`
2. Finds any pending (uncommitted) events for that stream in the current session
3. Applies the pending events on top using the aggregate's Apply/Create methods
4. Returns the result

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
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/project_latest_tests.cs#L46-L73' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_project_latest_example' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/project_latest_tests.cs#L75-L111' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_project_latest_merge_example' title='Start of snippet'>anchor</a></sup>
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
