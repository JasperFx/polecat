using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests;

// Regression for the Wolverine GH-2941 root cause: DocumentSessionBase.SaveChangesAsync used to
// early-return when _workTracker.HasOutstandingWork() was false. _workTracker only tracks
// document operations and event streams, not ITransactionParticipants - so a session that has
// ONLY registered participants (no doc ops, no streams) had SaveChangesAsync skip the entire
// pipeline, including the participants' BeforeCommitAsync. Adding the participant-count check to
// the early-return guard ensures the transaction runs and participants fire.
public class Bug_save_changes_runs_participants_only : OneOffConfigurationsContext
{
    [Fact]
    public async Task save_changes_runs_participants_when_only_participants_are_pending()
    {
        ConfigureStore(_ => { });
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var participant = new RecordingParticipant();

        await using (var session = theStore.LightweightSession())
        {
            session.AddTransactionParticipant(participant);
            await session.SaveChangesAsync();
        }

        // Before the fix the participant was silently skipped (BeforeCommitAsync never ran).
        participant.BeforeCommitInvocations.ShouldBe(1);
        participant.LastTransactionWasNotNull.ShouldBeTrue(
            "The participant must run inside an active SQL transaction so BeforeCommitAsync can attach commands to the same commit.");
    }

    private sealed class RecordingParticipant : ITransactionParticipant
    {
        public int BeforeCommitInvocations { get; private set; }

        public bool LastTransactionWasNotNull { get; private set; }

        public Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
        {
            BeforeCommitInvocations++;
            LastTransactionWasNotNull = transaction is not null;
            return Task.CompletedTask;
        }
    }
}
