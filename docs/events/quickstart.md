# Event Store Quick Start

This guide walks you through setting up event sourcing with Polecat.

## Define Your Events

Events are simple .NET classes or records that describe something that happened:

```cs
public record QuestStarted(string Name);
public record MembersJoined(string Location, string[] Members);
public record MembersDeparted(string Location, string[] Members);
public record QuestEnded(string Name);
```

## Define Your Aggregate

An aggregate is a projection of events into a domain object:

```cs
public class QuestParty
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public List<string> Members { get; set; } = new();

    public void Apply(QuestStarted started)
    {
        Name = started.Name;
    }

    public void Apply(MembersJoined joined)
    {
        Members.AddRange(joined.Members);
    }

    public void Apply(MembersDeparted departed)
    {
        foreach (var member in departed.Members)
            Members.Remove(member);
    }
}
```

## Start a Stream and Append Events

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("Server=localhost,1433;Database=myapp;User Id=sa;Password=YourStrong!Password;TrustServerCertificate=True");
});

await using var session = store.LightweightSession();

// Start a new stream with initial events
var questId = session.Events.StartStream<QuestParty>(
    new QuestStarted("Destroy the Ring"),
    new MembersJoined("Rivendell", ["Frodo", "Sam", "Aragorn", "Gandalf"])
);

await session.SaveChangesAsync();
```

## Append More Events

```cs
await using var session = store.LightweightSession();

session.Events.Append(questId,
    new MembersJoined("Moria", ["Gimli", "Legolas"]),
    new MembersDeparted("Moria", ["Gandalf"])
);

await session.SaveChangesAsync();
```

## Live Aggregation

Replay all events to build the current state:

```cs
var party = await session.Events.AggregateStreamAsync<QuestParty>(questId);
// party.Name == "Destroy the Ring"
// party.Members == ["Frodo", "Sam", "Aragorn", "Gimli", "Legolas"]
```

## Inline Projections

For strong consistency, register an inline projection that updates automatically:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    opts.Projections.Add<SingleStreamProjection<QuestParty, Guid>>(ProjectionLifecycle.Inline);
});

// Now QuestParty is automatically updated in the same transaction
await using var session = store.LightweightSession();
session.Events.StartStream<QuestParty>(
    new QuestStarted("Destroy the Ring")
);
await session.SaveChangesAsync();

// Load the projection directly as a document
var party = await session.LoadAsync<QuestParty>(questId);
```

## Optimistic Concurrency

Append with an expected version to prevent lost updates:

```cs
session.Events.Append(questId, expectedVersion: 3,
    new MembersDeparted("Moria", ["Gandalf"])
);

// Throws EventStreamUnexpectedMaxEventIdException if version doesn't match
await session.SaveChangesAsync();
```

## Next Steps

- [Appending Events](/events/appending) -- Detailed append options
- [Querying Events](/events/querying) -- Loading streams and fetching events
- [Projections](/events/projections/) -- Building read models from events
- [Async Daemon](/events/projections/async-daemon) -- Background projection processing
