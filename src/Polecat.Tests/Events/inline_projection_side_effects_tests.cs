using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Events.Aggregation;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     Coverage for projection side effects on Inline projections
///     ([polecat#99](https://github.com/JasperFx/polecat/issues/99)).
///     Verifies the EnableSideEffectsOnInlineProjections gate, the
///     IMessageOutbox round-trip on the session commit path, and the
///     before/after-commit lifecycle ordering.
/// </summary>
public class inline_projection_side_effects_tests : OneOffConfigurationsContext
{
    [Fact]
    public async Task flag_off_means_raise_side_effects_is_not_invoked()
    {
        var outbox = new TrackingOutbox();
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "inline_se_off";
            opts.Events.MessageOutbox = outbox;
            // EnableSideEffectsOnInlineProjections defaults to false
            opts.Projections.Add(
                new SingleStreamProjection<InlineSeAggregate, Guid>(),
                ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, new InlineSeStarted("hello"));
        await session.SaveChangesAsync();

        outbox.CreateBatchCount.ShouldBe(0);
    }

    [Fact]
    public async Task flag_on_routes_published_message_through_outbox()
    {
        var outbox = new TrackingOutbox();
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "inline_se_on";
            opts.Events.MessageOutbox = outbox;
            opts.EventGraph.EnableSideEffectsOnInlineProjections = true;
            opts.Projections.Add(
                new InlineSeProjection(),
                ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, new InlineSeStarted("hello"));
        await session.SaveChangesAsync();

        outbox.CreateBatchCount.ShouldBe(1);
        outbox.LastBatch.ShouldNotBeNull();
        outbox.LastBatch!.Published.Count.ShouldBe(1);
        outbox.LastBatch.Published[0].Body.ShouldBeOfType<InlineSeNotice>();
        ((InlineSeNotice)outbox.LastBatch.Published[0].Body!).Label.ShouldBe("hello");
    }

    [Fact]
    public async Task before_and_after_commit_fire_in_order_around_publish()
    {
        var outbox = new TrackingOutbox();
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "inline_se_hooks";
            opts.Events.MessageOutbox = outbox;
            opts.EventGraph.EnableSideEffectsOnInlineProjections = true;
            opts.Projections.Add(
                new InlineSeProjection(),
                ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, new InlineSeStarted("evt"));
        await session.SaveChangesAsync();

        outbox.LastBatch!.Calls.ShouldBe(["publish", "before", "after"]);
    }

    public record InlineSeStarted(string Label);
    public record InlineSeNotice(string Label);

    public class InlineSeAggregate
    {
        public Guid Id { get; set; }
        public string Label { get; set; } = "";

        public void Apply(InlineSeStarted e) => Label = e.Label;
    }

    public class InlineSeProjection : SingleStreamProjection<InlineSeAggregate, Guid>
    {
        public override ValueTask RaiseSideEffects(IDocumentSession session, IEventSlice<InlineSeAggregate> slice)
        {
            if (slice.Snapshot is not null)
            {
                slice.PublishMessage(new InlineSeNotice(slice.Snapshot.Label));
            }
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TrackingOutbox : IMessageOutbox
    {
        public int CreateBatchCount { get; private set; }
        public TrackingBatch? LastBatch { get; private set; }

        public ValueTask<IMessageBatch> CreateBatch(IDocumentSession session)
        {
            CreateBatchCount++;
            LastBatch = new TrackingBatch();
            return new ValueTask<IMessageBatch>(LastBatch);
        }
    }

    private sealed class TrackingBatch : IMessageBatch
    {
        public List<(Type Type, object? Body, string TenantId)> Published { get; } = new();
        public List<string> Calls { get; } = new();

        public ValueTask PublishAsync<T>(T message, string tenantId)
        {
            Published.Add((typeof(T), (object?)message, tenantId));
            Calls.Add("publish");
            return ValueTask.CompletedTask;
        }

        public Task BeforeCommitAsync(CancellationToken token)
        {
            Calls.Add("before");
            return Task.CompletedTask;
        }

        public Task AfterCommitAsync(CancellationToken token)
        {
            Calls.Add("after");
            return Task.CompletedTask;
        }
    }
}
