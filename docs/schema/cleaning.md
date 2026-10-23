# Tearing Down Document Storage

Polecat provides methods for cleaning up data, primarily useful during testing and development.

## Cleaning All Documents

Delete all data from all document tables:

```cs
await store.Advanced.CleanAllDocumentsAsync();
```

This executes `DELETE FROM` on every `pc_doc_*` table.

## Cleaning a Specific Type

Delete all documents of a specific type:

```cs
await store.Advanced.CleanAsync<User>();
```

## Cleaning All Event Data

Delete all events, streams, and progression data:

```cs
await store.Advanced.CleanAllEventDataAsync();
```

This cleans:

- `pc_events` -- All event records
- `pc_streams` -- All stream metadata
- `pc_event_progression` -- All daemon progression

It deliberately leaves `pc_event_projection_floors` alone. A projection's start floor is
configuration about where its history begins, so it outlives a data reset, just as it outlives a
rebuild.

## Usage in Tests

Cleaning is most commonly used in integration test setup:

```cs
public class MyTests : IAsyncLifetime
{
    private IDocumentStore _store = null!;

    public async Task InitializeAsync()
    {
        _store = DocumentStore.For(opts => { ... });

        // Clean slate for each test
        await _store.Advanced.CleanAllDocumentsAsync();
        await _store.Advanced.CleanAllEventDataAsync();
    }

    public async Task DisposeAsync()
    {
        await _store.DisposeAsync();
    }
}
```

::: warning
These methods permanently delete data. They should only be used in development and testing environments, never in production.
:::
