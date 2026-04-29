using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
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
        // Trigger auto-discovery by manually registering the inline projection,
        // simulating what FindNaturalKeyDefinition auto-discovery does.
        ConfigureStore(opts => { });

        // Manually register (simulating auto-discovery) then apply schema so
        // the natural key table exists before the first FetchForWriting call.
        theStore.Options.Projections.Add<SingleStreamProjection<Bug4197Aggregate, Guid>>(ProjectionLifecycle.Inline);
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
            opts.Projections.Add<SingleStreamProjection<Bug4197Aggregate, Guid>>(ProjectionLifecycle.Inline);
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
