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
<!-- endSnippet -->

## Merging Committed and Pending Events

`ProjectLatest` also works when the stream has previously committed events and new events
are appended in the current session:

<!-- snippet: sample_polecat_project_latest_merge_example -->
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
