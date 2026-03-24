using JasperFx.Events.Aggregation;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public sealed record Bug4197AggregateKey(string Value);

public sealed record Bug4197AggregateCreatedEvent(Guid Id, string Key);

public sealed class Bug4197Aggregate
{
    public Guid Id { get; set; }

    [NaturalKey]
    public Bug4197AggregateKey Key { get; set; } = null!;

    [NaturalKeySource]
    public void Apply(Bug4197AggregateCreatedEvent e)
    {
        Id = e.Id;
        Key = new Bug4197AggregateKey(e.Key);
    }
}

public class Bug_4197_fetch_for_writing_natural_key : OneOffConfigurationsContext
{
    [Fact]
    public async Task fetch_for_writing_with_natural_key_without_explicit_projection_registration()
    {
        // No explicit projection registration — relying on auto-discovery.
        // Trigger auto-discovery by creating a lightweight session that forces
        // the FindNaturalKeyDefinition path to register a snapshot projection.
        ConfigureStore(opts => { });

        // Force auto-discovery: the first FetchForWriting call with a natural key type
        // will auto-register the Inline snapshot projection. But the natural key table
        // won't exist yet, so we need to apply schema changes first.
        // We accomplish this by manually registering the snapshot (simulating what
        // auto-discovery does), then applying schema.
        theStore.Options.Projections.Snapshot<Bug4197Aggregate>(SnapshotLifecycle.Inline);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        await using var session = theStore.LightweightSession();

        var aggregateId = Guid.NewGuid();
        var aggregateKey = new Bug4197AggregateKey("randomkeyvalue");
        var e = new Bug4197AggregateCreatedEvent(aggregateId, aggregateKey.Value);

        session.Events.StartStream<Bug4197Aggregate>(aggregateId, e);
        await session.SaveChangesAsync();

        // This should NOT throw InvalidOperationException about missing natural key definition
        var stream = await session.Events.FetchForWriting<Bug4197Aggregate, Bug4197AggregateKey>(aggregateKey);

        stream.ShouldNotBeNull();
        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate.Key.ShouldBe(aggregateKey);
    }

    [Fact]
    public async Task fetch_for_writing_with_natural_key_with_inline_snapshot()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Snapshot<Bug4197Aggregate>(SnapshotLifecycle.Inline);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        await using var session = theStore.LightweightSession();

        var aggregateId = Guid.NewGuid();
        var aggregateKey = new Bug4197AggregateKey("randomkeyvalue");
        var e = new Bug4197AggregateCreatedEvent(aggregateId, aggregateKey.Value);

        session.Events.StartStream<Bug4197Aggregate>(aggregateId, e);
        await session.SaveChangesAsync();

        var stream = await session.Events.FetchForWriting<Bug4197Aggregate, Bug4197AggregateKey>(aggregateKey);

        stream.ShouldNotBeNull();
        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate.Key.ShouldBe(aggregateKey);
    }
}
