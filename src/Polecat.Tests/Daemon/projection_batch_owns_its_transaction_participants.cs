using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon;
using Polecat.Projections;
using Polecat.Subscriptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

public record ParticipantRequested(Guid Id);

/// <summary>
///     Records, in order, what happened to it, so "disposed before the commit" and "never ran" both
///     show up as the wrong list.
/// </summary>
public sealed class RecordingParticipant : ITransactionParticipant, IAsyncDisposable
{
    private readonly ConcurrentQueue<string> _log = new();

    public const string RanBeforeCommit = "before commit";
    public const string Disposed = "disposed";

    public Exception? FailWith { get; init; }

    public string[] Log => _log.ToArray();

    public Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        // The batch's own open transaction, on the connection it was handed with.
        transaction.Connection.ShouldBeSameAs(connection);

        _log.Enqueue(RanBeforeCommit);
        return FailWith is null ? Task.CompletedTask : Task.FromException(FailWith);
    }

    public ValueTask DisposeAsync()
    {
        _log.Enqueue(Disposed);
        return ValueTask.CompletedTask;
    }
}

/// <summary>
///     Declares no disposal at all, which is most participants. The session's drain cleared these
///     too, so losing one never had anything to do with what the participant owned.
/// </summary>
public sealed class UndisposableParticipant : ITransactionParticipant
{
    private int _commits;

    public int Commits => Volatile.Read(ref _commits);

    public Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        Interlocked.Increment(ref _commits);
        return Task.CompletedTask;
    }
}

public sealed class ThrowsOnDisposal : ITransactionParticipant, IAsyncDisposable
{
    public Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token) =>
        Task.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.FromException(new DivideByZeroException("Failed to dispose"));
}

/// <summary>
///     The shape of an event projection: JasperFx disposes the session this is handed as soon as
///     this returns.
/// </summary>
public class EnlistingProjection(ITransactionParticipant participant) : IProjection
{
    public Task ApplyAsync(IDocumentSession operations, IReadOnlyList<IEvent> events,
        CancellationToken cancellation)
    {
        operations.AddTransactionParticipant(participant);
        return Task.CompletedTask;
    }
}

public class EnlistingSubscription : SubscriptionBase
{
    public static RecordingParticipant Participant = new();

    public override Task<IChangeListener> ProcessEventsAsync(EventRange page, ISubscriptionController controller,
        IDocumentOperations operations, CancellationToken cancellationToken)
    {
        ((IDocumentSession)operations).AddTransactionParticipant(Participant);
        return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
    }
}

/// <summary>
///     #867, without EF in the picture. A participant belongs to whatever commits its transaction, so
///     in the daemon it belongs to the batch. It used to belong to the session, and JasperFx disposes
///     a session as soon as an event projection's <c>ApplyAsync</c> returns, which threw the
///     participant away before the batch went looking for it. No exception, no shard failure, and the
///     progression moved past the events.
/// </summary>
public class projection_batch_owns_its_transaction_participants : OneOffConfigurationsContext
{
    private static readonly ShardName TheShard = new(nameof(EnlistingProjection), "All", 1);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task AppendAnEventAsync()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new ParticipantRequested(Guid.NewGuid()));
        await session.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task a_participant_outlives_the_session_that_enlisted_it()
    {
        var participant = new RecordingParticipant();
        ConfigureStore(opts => opts.Projections.Add(new EnlistingProjection(participant), ProjectionLifecycle.Async));

        await AppendAnEventAsync();
        await theStore.WaitForProjectionAsync();

        participant.Log.ShouldBe([RecordingParticipant.RanBeforeCommit, RecordingParticipant.Disposed]);
    }

    [Fact]
    public async Task a_participant_with_no_disposal_still_reaches_the_commit()
    {
        var participant = new UndisposableParticipant();
        ConfigureStore(opts => opts.Projections.Add(new EnlistingProjection(participant), ProjectionLifecycle.Async));

        await AppendAnEventAsync();
        await theStore.WaitForProjectionAsync();

        participant.Commits.ShouldBe(1);
    }

    /// <summary>
    ///     <c>CompositeIProjectionExecution</c> disposes its session the same way, and it is Polecat's
    ///     own code rather than JasperFx's, so this route was never going to be fixed by a package bump.
    /// </summary>
    [Fact]
    public async Task a_composite_member_gets_the_same_treatment()
    {
        var participant = new RecordingParticipant();
        ConfigureStore(opts => opts.Projections.CompositeProjectionFor("Enlisting", composite =>
            composite.Add(new EnlistingProjection(participant))));

        await AppendAnEventAsync();
        await theStore.WaitForProjectionAsync();

        participant.Log.ShouldBe([RecordingParticipant.RanBeforeCommit, RecordingParticipant.Disposed]);
    }

    [Fact]
    public async Task a_participant_that_fails_holds_the_progression_back_and_is_still_disposed()
    {
        var participant = new RecordingParticipant { FailWith = new DivideByZeroException("The participant failed") };
        ConfigureStore(opts => opts.Projections.Add(new EnlistingProjection(participant), ProjectionLifecycle.Async));

        await AppendAnEventAsync();

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        var agent = daemon.CurrentAgents().ShouldHaveSingleItem();

        for (var attempt = 0; attempt < 120 && agent.Status == AgentStatus.Running; attempt++)
        {
            if (await theDatabase.ProjectionProgressFor(TheShard, Ct) > 0) break;
            await Task.Delay(250, Ct);
        }

        (await theDatabase.ProjectionProgressFor(TheShard, Ct)).ShouldBe(0);
        agent.Status.ShouldBe(AgentStatus.Paused);

        participant.Log.ShouldContain(RecordingParticipant.RanBeforeCommit);
        participant.Log.Last().ShouldBe(RecordingParticipant.Disposed);
    }

    /// <summary>
    ///     A subscription's session is opened outside the batch and disposed by the runner, and the
    ///     batch has to be disposed as well now that it owns what that session enlists.
    /// </summary>
    [Fact]
    public async Task a_subscriptions_participant_runs_and_is_disposed()
    {
        var participant = EnlistingSubscription.Participant = new RecordingParticipant();
        ConfigureStore(opts => opts.Projections.Subscribe<EnlistingSubscription>());

        await AppendAnEventAsync();
        await theStore.WaitForProjectionAsync();

        participant.Log.ShouldBe([RecordingParticipant.RanBeforeCommit, RecordingParticipant.Disposed]);
    }

    /// <summary>
    ///     The other half of the ownership rule, which #650 depends on: a session that commits for
    ///     itself keeps what it enlists, and disposes it itself.
    /// </summary>
    [Fact]
    public async Task a_session_that_commits_for_itself_keeps_its_participants()
    {
        var participant = new RecordingParticipant();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var session = theStore.LightweightSession())
        {
            session.AddTransactionParticipant(participant);
            await session.SaveChangesAsync(Ct);

            participant.Log.ShouldBe([RecordingParticipant.RanBeforeCommit]);
        }

        participant.Log.ShouldBe([RecordingParticipant.RanBeforeCommit, RecordingParticipant.Disposed]);
    }

    [Fact]
    public async Task disposing_a_session_leaves_its_participant_to_the_batch()
    {
        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database);
        var participant = new RecordingParticipant();

        await using (var session = batch.SessionForTenant(theStore.Options.Tenancy!.DefaultTenantId))
        {
            session.AddTransactionParticipant(participant);
        }

        participant.Log.ShouldBeEmpty();

        await batch.DisposeAsync();

        participant.Log.ShouldBe([RecordingParticipant.Disposed]);
    }

    /// <summary>
    ///     A failing composite member disposes the batch it shares, and the composite disposes that
    ///     batch again.
    /// </summary>
    [Fact]
    public async Task disposing_the_batch_twice_disposes_a_participant_once()
    {
        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database);
        var participant = new RecordingParticipant();

        batch.SessionForTenant(theStore.Options.Tenancy!.DefaultTenantId).AddTransactionParticipant(participant);

        await batch.DisposeAsync();
        await batch.DisposeAsync();

        participant.Log.ShouldBe([RecordingParticipant.Disposed]);
    }

    [Fact]
    public async Task a_participant_that_fails_to_dispose_does_not_strand_the_others()
    {
        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database);
        var first = new RecordingParticipant();
        var last = new RecordingParticipant();

        var session = batch.SessionForTenant(theStore.Options.Tenancy!.DefaultTenantId);
        session.AddTransactionParticipant(first);
        session.AddTransactionParticipant(new ThrowsOnDisposal());
        session.AddTransactionParticipant(last);

        await Should.ThrowAsync<DivideByZeroException>(async () => await batch.DisposeAsync());

        first.Log.ShouldBe([RecordingParticipant.Disposed]);
        last.Log.ShouldBe([RecordingParticipant.Disposed]);
    }
}
