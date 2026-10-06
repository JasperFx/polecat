using Polecat.Services;
using Polecat.Events.Aggregation;
using Polecat.Events.Daemon;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     polecat#744 / JasperFx 2.81.0 (jasperfx#953, jasperfx#980). A projection or subscription drain
///     that times out calls <c>CancelAsync</c> on the very token the projection batch was handed, and
///     it can land in the window between <c>tx.CommitAsync</c> returning and the batch's post-commit
///     hooks running. The page's data is durably committed and its progression row has advanced by
///     then, so it will never be reprocessed — a post-commit side effect abandoned there is lost, not
///     deferred.
/// </summary>
/// <remarks>
///     <para>
///         The listener half is what this pins, because it is the half that failed <b>silently</b>:
///         <c>PolecatProjectionBatch</c> suppresses every exception from an <see cref="IChangeListener" />
///         on purpose, so a cancelled token skipped the listener with no log line, no shard failure and
///         no dead letter, for a page whose documents had committed.
///     </para>
///     <para>
///         Cancelling from inside the message batch's <c>AfterCommitAsync</c> is the only in-process way
///         to reach that window: it is the first thing the batch does after the commit returns, and
///         anything earlier (a listener's <c>BeforeCommitAsync</c>, a transaction participant) cancels
///         the token while the transaction still needs it and fails the commit instead.
///     </para>
/// </remarks>
public class post_commit_hooks_survive_drain_cancellation_tests : OneOffConfigurationsContext
{
    [Fact]
    public async Task a_change_listener_still_runs_when_the_drain_cancels_after_the_commit()
    {
        using var cts = new CancellationTokenSource();
        var outbox = new CancellingOutbox(cts);
        var listener = new TokenRecordingListener();

        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "drain_post_commit";
            opts.Events.MessageOutbox = outbox;
            opts.Projections.AsyncListeners.Add(listener);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database);
        var session = batch.SessionForTenant(theStore.Options.Tenancy!.DefaultTenantId);
        session.Store(new DrainDoc { Id = Guid.NewGuid() });

        // Creates the message batch, so its AfterCommitAsync — and therefore the cancellation — runs.
        await batch.PublishMessageAsync(new DrainSideEffect("drain"), "default");

        await batch.ExecuteAsync(cts.Token);

        // The premise: the commit succeeded and the token really was cancelled inside the window.
        outbox.Batch.ShouldNotBeNull();
        outbox.Batch!.AfterCommitRan.ShouldBeTrue();
        cts.IsCancellationRequested.ShouldBeTrue();

        // The claim: the post-commit listener ran anyway, and did not observe the drain's cancellation.
        listener.AfterCommitRan.ShouldBeTrue();
        listener.AfterCommitSawCancellation.ShouldBeFalse();
    }

    public class DrainDoc
    {
        public Guid Id { get; set; }
    }

    public sealed record DrainSideEffect(string Label);

    private sealed class CancellingOutbox(CancellationTokenSource cts) : IMessageOutbox
    {
        public CancellingBatch? Batch { get; private set; }

        public ValueTask<IMessageBatch> CreateBatch(IDocumentSession session)
        {
            Batch = new CancellingBatch(cts);
            return new ValueTask<IMessageBatch>(Batch);
        }
    }

    private sealed class CancellingBatch(CancellationTokenSource cts) : IMessageBatch
    {
        public bool AfterCommitRan { get; private set; }

        public ValueTask PublishAsync<T>(T message, string tenantId) => ValueTask.CompletedTask;

        public Task BeforeCommitAsync(CancellationToken token) => Task.CompletedTask;

        public Task AfterCommitAsync(CancellationToken token)
        {
            AfterCommitRan = true;

            // Stands in for the drain deadline expiring here rather than a millisecond earlier.
            cts.Cancel();
            return Task.CompletedTask;
        }
    }

    private sealed class TokenRecordingListener : IChangeListener
    {
        public bool AfterCommitRan { get; private set; }
        public bool AfterCommitSawCancellation { get; private set; }

        public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        {
            AfterCommitRan = true;
            AfterCommitSawCancellation = token.IsCancellationRequested;
            return Task.CompletedTask;
        }

        public Task BeforeCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
            => Task.CompletedTask;
    }
}
