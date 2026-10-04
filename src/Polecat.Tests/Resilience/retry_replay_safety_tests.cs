using System.Data;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Resilience;
using Polly;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Resilience;

/// <summary>
///     #724: what a retry strategy on Polecat's resilience pipeline must not replay.
/// </summary>
/// <remarks>
///     <para>
///         <b>Polecat has no bug here today, and these facts are what keeps that true.</b> The safety
///         argument rests on two preconditions, both of which are a line of code someone could change
///         without realising what it was holding up: the default pipeline adds no strategy, and the
///         connection-open retry list excludes the lock errors. Each gets a fact.
///     </para>
///     <para>
///         The hazard itself is marten#5528 — a <c>Serializable</c> session whose write-retry
///         classifier replayed a snapshot conflict, opened a fresh snapshot in which the conflict no
///         longer existed, and committed over a write that had already succeeded. No database is
///         needed to pin the reasoning; the one fact that needs a real
///         <see cref="SqlException" /> reuses the genuine deadlock that
///         <c>exclusive_append_lock_failure_tests</c> already forces.
///     </para>
/// </remarks>
public class retry_replay_safety_tests
{
    // ---- the preconditions ------------------------------------------------------------------------

    [Fact]
    public async Task the_default_pipeline_does_not_replay_a_failed_unit_of_work()
    {
        // ⚠️ THE precondition. Everything else in #724 is a hazard rather than a bug only because
        // this is true. Asserted behaviourally rather than by reflecting over the builder: what
        // matters is that the callback runs once, not how the pipeline is assembled.
        var builder = new ResiliencePipelineBuilder();
        var pipeline = builder.AddPolecatDefaults().Build();

        var attempts = 0;

        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await pipeline.ExecuteAsync(_ =>
            {
                attempts++;
                throw new InvalidOperationException("the unit of work failed");
            }));

        attempts.ShouldBe(1);
    }

    [Fact]
    public void the_connection_open_retry_list_excludes_every_error_that_must_not_be_replayed()
    {
        // 1205 and 1222 are filtered out of SqlClient's baseline (#652), and 3960 was never in it.
        // If a future bump to Microsoft.Data.SqlClient adds 3960 to BaselineTransientErrors, this is
        // the fact that notices.
        ConnectionFactory.TransientSqlErrors.ShouldNotContain(PolecatRetryPredicates.DeadlockVictim);
        ConnectionFactory.TransientSqlErrors.ShouldNotContain(PolecatRetryPredicates.LockRequestTimeout);
        ConnectionFactory.TransientSqlErrors
            .ShouldNotContain(PolecatRetryPredicates.SnapshotUpdateConflict);
    }

    // ---- the decision table ----------------------------------------------------------------------

    [Theory]
    [InlineData(IsolationLevel.ReadCommitted)]
    [InlineData(IsolationLevel.Snapshot)]
    [InlineData(IsolationLevel.Serializable)]
    [InlineData(IsolationLevel.ReadUncommitted)]
    public void a_snapshot_update_conflict_is_never_safe_to_replay(IsolationLevel level)
    {
        // Including levels that cannot raise 3960. Answering "safe" for an error that cannot occur
        // would be true but useless, and a caller who passed the wrong level would then get the
        // dangerous answer for the one error that can never be replayed.
        PolecatRetryPredicates
            .IsUnsafeToReplay(PolecatRetryPredicates.SnapshotUpdateConflict, level)
            .ShouldBeTrue();
    }

    [Theory]
    [InlineData(PolecatRetryPredicates.DeadlockVictim)]
    [InlineData(PolecatRetryPredicates.LockRequestTimeout)]
    public void a_lock_failure_is_safe_to_replay_at_read_committed(int number)
    {
        // The ordinary safe retry: a replay re-reads current data, so nothing stale is re-issued.
        PolecatRetryPredicates.IsUnsafeToReplay(number, IsolationLevel.ReadCommitted).ShouldBeFalse();
    }

    [Theory]
    [InlineData(PolecatRetryPredicates.DeadlockVictim, IsolationLevel.Snapshot)]
    [InlineData(PolecatRetryPredicates.DeadlockVictim, IsolationLevel.Serializable)]
    [InlineData(PolecatRetryPredicates.LockRequestTimeout, IsolationLevel.Snapshot)]
    [InlineData(PolecatRetryPredicates.LockRequestTimeout, IsolationLevel.Serializable)]
    public void a_lock_failure_is_not_safe_to_replay_under_a_snapshot(int number, IsolationLevel level)
    {
        // marten#5528's mechanism reached through a different error number: the operations being
        // replayed came from a snapshot the replay will not re-take. Marten's issue leaves this case
        // named rather than guessed, and this fork is what naming it looks like.
        PolecatRetryPredicates.IsUnsafeToReplay(number, level).ShouldBeTrue();
    }

    [Fact]
    public void an_unrelated_failure_is_safe_to_replay()
    {
        // The predicate is a veto on specific errors, not a blanket refusal -- a transient network
        // failure is exactly what a caller's retry is for.
        PolecatRetryPredicates.IsUnsafeToReplay(-2, IsolationLevel.Snapshot).ShouldBeFalse();
        PolecatRetryPredicates.IsUnsafeToReplay(new TimeoutException()).ShouldBeFalse();
    }

    [Fact]
    public void the_exception_overload_walks_the_chain()
    {
        // Same reason FindLockFailure walks it (#652): the append path throws its SqlException
        // unwrapped, while a caller-supplied pipeline may wrap it. A predicate that looked only at
        // the outer exception would answer "safe to replay" for a wrapped 3960 -- the failure mode
        // #652 already found once on the lock-failure guard.
        //
        // Asserted here with a non-Sql chain, since SqlException cannot be constructed; the
        // SqlException leg is covered against a real 1205 in exclusive_append_lock_failure_tests.
        var nested = new InvalidOperationException("outer", new TimeoutException("inner"));

        PolecatRetryPredicates.IsUnsafeToReplay(nested).ShouldBeFalse();
    }

    [Fact]
    public void the_predicate_refuses_a_null_exception_rather_than_answering_safe()
    {
        // ShouldHandle hands over an Outcome whose Exception is nullable. "No exception" is not
        // "safe to replay", and silently answering false would read as a green light.
        Should.Throw<ArgumentNullException>(() => PolecatRetryPredicates.IsUnsafeToReplay(null!));
    }
}
