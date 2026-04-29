using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

#region sample_polecat_evolve_aggregates

public record AEvent;
public record BEvent;
public record CEvent;

/// <summary>
/// Mutable aggregate using void Evolve(IEvent e) — switches on IEvent envelope
/// </summary>
public class MutableIEventEvolveAggregate
{
    public Guid Id { get; set; }
    public int ACount { get; set; }
    public int BCount { get; set; }
    public int CCount { get; set; }

    public void Evolve(IEvent e)
    {
        switch (e)
        {
            case IEvent<AEvent>:
                ACount++;
                break;
            case IEvent<BEvent>:
                BCount++;
                break;
            case IEvent<CEvent>:
                CCount++;
                break;
        }
    }
}

/// <summary>
/// Mutable aggregate using void Evolve(object o) — switches on event data
/// </summary>
public class MutableObjectEvolveAggregate
{
    public Guid Id { get; set; }
    public int ACount { get; set; }
    public int BCount { get; set; }
    public int CCount { get; set; }

    public void Evolve(object o)
    {
        switch (o)
        {
            case AEvent:
                ACount++;
                break;
            case BEvent:
                BCount++;
                break;
            case CEvent:
                CCount++;
                break;
        }
    }
}

/// <summary>
/// Immutable aggregate using TDoc Evolve(IEvent e) — returns new instance
/// </summary>
public record ImmutableIEventEvolveAggregate(Guid Id, int ACount = 0, int BCount = 0, int CCount = 0)
{
    public ImmutableIEventEvolveAggregate() : this(Guid.Empty) { }

    public ImmutableIEventEvolveAggregate Evolve(IEvent e)
    {
        return e switch
        {
            IEvent<AEvent> => this with { ACount = ACount + 1 },
            IEvent<BEvent> => this with { BCount = BCount + 1 },
            IEvent<CEvent> => this with { CCount = CCount + 1 },
            _ => this
        };
    }
}

/// <summary>
/// Immutable aggregate using TDoc Evolve(object o) — returns new instance
/// </summary>
public record ImmutableObjectEvolveAggregate(Guid Id, int ACount = 0, int BCount = 0, int CCount = 0)
{
    public ImmutableObjectEvolveAggregate() : this(Guid.Empty) { }

    public ImmutableObjectEvolveAggregate Evolve(object o)
    {
        return o switch
        {
            AEvent => this with { ACount = ACount + 1 },
            BEvent => this with { BCount = BCount + 1 },
            CEvent => this with { CCount = CCount + 1 },
            _ => this
        };
    }
}

/// <summary>
/// Mutable aggregate using async Task EvolveAsync(IEvent e, IQuerySession session)
/// </summary>
public class AsyncEvolveAggregate
{
    public Guid Id { get; set; }
    public int ACount { get; set; }
    public int BCount { get; set; }

    public Task EvolveAsync(IEvent e, IQuerySession session)
    {
        switch (e)
        {
            case IEvent<AEvent>:
                ACount++;
                break;
            case IEvent<BEvent>:
                BCount++;
                break;
        }

        return Task.CompletedTask;
    }
}

/// <summary>
/// Immutable aggregate using async ValueTask&lt;TDoc&gt; EvolveAsync(IEvent e, IQuerySession session)
/// </summary>
public record ImmutableAsyncEvolveAggregate(Guid Id, int ACount = 0, int BCount = 0)
{
    public ImmutableAsyncEvolveAggregate() : this(Guid.Empty) { }

    public ValueTask<ImmutableAsyncEvolveAggregate> EvolveAsync(IEvent e, IQuerySession session)
    {
        var result = e switch
        {
            IEvent<AEvent> => this with { ACount = ACount + 1 },
            IEvent<BEvent> => this with { BCount = BCount + 1 },
            _ => this
        };

        return new ValueTask<ImmutableAsyncEvolveAggregate>(result);
    }
}

#endregion

[Collection("integration")]
public class self_aggregating_evolve_method : IntegrationContext
{
    public self_aggregating_evolve_method(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task mutable_ievent_evolve_inline()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "evolve_tests";
            opts.Projections.Add<SingleStreamProjection<MutableIEventEvolveAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new AEvent(), new BEvent(), new AEvent(), new CEvent());
        await theSession.SaveChangesAsync();

        var aggregate = await theSession.Events.AggregateStreamAsync<MutableIEventEvolveAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate.ACount.ShouldBe(2);
        aggregate.BCount.ShouldBe(1);
        aggregate.CCount.ShouldBe(1);
    }

    [Fact]
    public async Task mutable_object_evolve_inline()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "evolve_tests";
            opts.Projections.Add<SingleStreamProjection<MutableObjectEvolveAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new AEvent(), new BEvent(), new CEvent(), new CEvent());
        await theSession.SaveChangesAsync();

        var aggregate = await theSession.Events.AggregateStreamAsync<MutableObjectEvolveAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate.ACount.ShouldBe(1);
        aggregate.BCount.ShouldBe(1);
        aggregate.CCount.ShouldBe(2);
    }

    [Fact]
    public async Task immutable_ievent_evolve_inline()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "evolve_tests";
            opts.Projections.Add<SingleStreamProjection<ImmutableIEventEvolveAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new AEvent(), new AEvent(), new BEvent());
        await theSession.SaveChangesAsync();

        var aggregate = await theSession.Events.AggregateStreamAsync<ImmutableIEventEvolveAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate.ACount.ShouldBe(2);
        aggregate.BCount.ShouldBe(1);
        aggregate.CCount.ShouldBe(0);
    }

    [Fact]
    public async Task immutable_object_evolve_inline()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "evolve_tests";
            opts.Projections.Add<SingleStreamProjection<ImmutableObjectEvolveAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new BEvent(), new CEvent(), new AEvent());
        await theSession.SaveChangesAsync();

        var aggregate = await theSession.Events.AggregateStreamAsync<ImmutableObjectEvolveAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate.ACount.ShouldBe(1);
        aggregate.BCount.ShouldBe(1);
        aggregate.CCount.ShouldBe(1);
    }

    [Fact]
    public async Task async_evolve_inline()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "evolve_tests";
            opts.Projections.Add<SingleStreamProjection<AsyncEvolveAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new AEvent(), new AEvent(), new BEvent());
        await theSession.SaveChangesAsync();

        var aggregate = await theSession.Events.AggregateStreamAsync<AsyncEvolveAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate.ACount.ShouldBe(2);
        aggregate.BCount.ShouldBe(1);
    }

    [Fact]
    public async Task immutable_async_evolve_inline()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "evolve_tests";
            opts.Projections.Add<SingleStreamProjection<ImmutableAsyncEvolveAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new AEvent(), new BEvent(), new AEvent());
        await theSession.SaveChangesAsync();

        var aggregate = await theSession.Events.AggregateStreamAsync<ImmutableAsyncEvolveAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate.ACount.ShouldBe(2);
        aggregate.BCount.ShouldBe(1);
    }

    [Fact]
    public async Task mutable_ievent_evolve_with_append_to_existing_stream()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "evolve_tests";
            opts.Projections.Add<SingleStreamProjection<MutableIEventEvolveAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new AEvent(), new BEvent());
        await theSession.SaveChangesAsync();

        // Append more events
        theSession.Events.Append(streamId, new AEvent(), new CEvent());
        await theSession.SaveChangesAsync();

        var aggregate = await theSession.Events.AggregateStreamAsync<MutableIEventEvolveAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate.ACount.ShouldBe(2);
        aggregate.BCount.ShouldBe(1);
        aggregate.CCount.ShouldBe(1);
    }

    [Fact]
    public async Task live_aggregation_with_evolve()
    {
        // No snapshot — live aggregation only
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new AEvent(), new BEvent(), new CEvent());
        await theSession.SaveChangesAsync();

        var aggregate = await theSession.Events.AggregateStreamAsync<MutableIEventEvolveAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate.ACount.ShouldBe(1);
        aggregate.BCount.ShouldBe(1);
        aggregate.CCount.ShouldBe(1);
    }
}
