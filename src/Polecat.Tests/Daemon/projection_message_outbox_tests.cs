using JasperFx.Events;
using Polecat.Events.Aggregation;
using Polecat.Events.Daemon;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     Coverage for the IMessageBatch / IMessageOutbox plumbing wired through
///     <see cref="PolecatProjectionBatch"/> ([polecat#84](https://github.com/JasperFx/polecat/issues/84)).
///     Verifies the lifecycle: lazy batch creation on first publish, message
///     forwarded to the batch, BeforeCommit + AfterCommit hooks fire after a
///     successful projection commit, and no batch is created when no projection
///     publishes.
/// </summary>
public class projection_message_outbox_tests : OneOffConfigurationsContext
{
    [Fact]
    public async Task batch_is_lazy_no_publish_means_no_create_no_hooks()
    {
        var outbox = new TrackingOutbox();
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "msg_outbox_lazy";
            opts.Events.MessageOutbox = outbox;
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph,
            ConnectionSource.ConnectionString);
        var session = batch.SessionForTenant(theStore.Options.Tenancy!.DefaultTenantId);
        session.Store(new SimpleDoc { Id = Guid.NewGuid() });

        await batch.ExecuteAsync(default);

        outbox.CreateBatchCount.ShouldBe(0);
        outbox.LastBatch.ShouldBeNull();
    }

    [Fact]
    public async Task publish_creates_a_batch_once_and_forwards_the_message()
    {
        var outbox = new TrackingOutbox();
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "msg_outbox_publish";
            opts.Events.MessageOutbox = outbox;
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph,
            ConnectionSource.ConnectionString);
        var session = batch.SessionForTenant(theStore.Options.Tenancy!.DefaultTenantId);
        session.Store(new SimpleDoc { Id = Guid.NewGuid() });

        await batch.PublishMessageAsync(new SideEffect("first"), "default");
        await batch.PublishMessageAsync(new SideEffect("second"), "default");

        outbox.CreateBatchCount.ShouldBe(1);
        outbox.LastBatch.ShouldNotBeNull();
        outbox.LastBatch!.Published.ShouldBe(
        [
            (typeof(SideEffect), (object?)new SideEffect("first"), "default"),
            (typeof(SideEffect), (object?)new SideEffect("second"), "default"),
        ]);
    }

    [Fact]
    public async Task before_and_after_commit_fire_when_a_message_was_published()
    {
        var outbox = new TrackingOutbox();
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "msg_outbox_hooks";
            opts.Events.MessageOutbox = outbox;
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph,
            ConnectionSource.ConnectionString);
        var session = batch.SessionForTenant(theStore.Options.Tenancy!.DefaultTenantId);
        session.Store(new SimpleDoc { Id = Guid.NewGuid() });

        await batch.PublishMessageAsync(new SideEffect("evt"), "default");
        await batch.ExecuteAsync(default);

        outbox.LastBatch!.BeforeCommitCalls.ShouldBe(1);
        outbox.LastBatch!.AfterCommitCalls.ShouldBe(1);
        // Order matters: publish first, then before-commit, then after-commit.
        outbox.LastBatch!.Calls.ShouldBe(["publish", "before", "after"]);
    }

    [Fact]
    public async Task metadata_overload_forwards_to_the_batch()
    {
        var outbox = new TrackingOutbox();
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "msg_outbox_metadata";
            opts.Events.MessageOutbox = outbox;
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph,
            ConnectionSource.ConnectionString);
        var session = batch.SessionForTenant(theStore.Options.Tenancy!.DefaultTenantId);
        session.Store(new SimpleDoc { Id = Guid.NewGuid() });

        var metadata = new MessageMetadata { TenantId = "tenant-z" };
        await batch.PublishMessageAsync(new SideEffect("with-meta"), metadata);
        await batch.ExecuteAsync(default);

        // The default IMessageSink.PublishAsync(T, MessageMetadata) impl
        // forwards to PublishAsync(T, metadata.TenantId), so we should see
        // the tenant-z entry in the captured publishes.
        outbox.LastBatch!.Published.ShouldBe(
        [
            (typeof(SideEffect), (object?)new SideEffect("with-meta"), "tenant-z"),
        ]);
    }

    public class SimpleDoc
    {
        public Guid Id { get; set; }
    }

    public sealed record SideEffect(string Label);

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
        public int BeforeCommitCalls { get; private set; }
        public int AfterCommitCalls { get; private set; }

        public ValueTask PublishAsync<T>(T message, string tenantId)
        {
            Published.Add((typeof(T), (object?)message, tenantId));
            Calls.Add("publish");
            return ValueTask.CompletedTask;
        }

        public Task BeforeCommitAsync(CancellationToken token)
        {
            BeforeCommitCalls++;
            Calls.Add("before");
            return Task.CompletedTask;
        }

        public Task AfterCommitAsync(CancellationToken token)
        {
            AfterCommitCalls++;
            Calls.Add("after");
            return Task.CompletedTask;
        }
    }
}
