using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat;
using Polecat.Services;
using Polecat.Subscriptions;

namespace JasperFx.Events.ComplianceTests;

/// <summary>
///     The Polecat half of the shared <c>ComplianceSubscription</c> partial (compliance wave 8).
///     The library owns the recording, the locking and the pinned name; each consumer supplies
///     <c>ProcessEventsAsync</c>, because both products declare an <c>ISubscription</c> with a
///     member of identical shape but over their own <c>IChangeListener</c> and
///     <c>IDocumentOperations</c> types, so the signature cannot be written once upstream.
///     <para>
///     Lives in <c>Polecat.Tests</c> beside the fixture. Marten had to put its half in
///     <c>Marten.Testing</c> because two assemblies there compile the source-only package and both
///     therefore need the completing half; Polecat.Tests is the only assembly here that references
///     it, so there is no such constraint.
///     </para>
///     <para>
///     Marten's half also supplies <c>ValueTask DisposeAsync() =&gt; default;</c>. Polecat's
///     <see cref="ISubscription" /> declares only <c>ProcessEventsAsync</c> and its registration
///     path never asks for disposal, so that member is deliberately not carried across.
///     </para>
///     <para>
///     The namespace is the library's, not Polecat's, because this completes a type the library
///     declares.
///     </para>
/// </summary>
public partial class ComplianceSubscription : ISubscription
{
    public Task<IChangeListener> ProcessEventsAsync(
        EventRange page,
        ISubscriptionController controller,
        IDocumentOperations operations,
        CancellationToken cancellationToken)
    {
        Record(page.Events);

        // Wave 15 / jasperfx#768. The session the daemon hands over is the guarantee that
        // distinguishes a subscription from a webhook: its writes land in the batch's transaction
        // alongside the progression row, so they are exactly-once against Polecat's own database and
        // a subscription cannot advance past a range whose writes were rolled back. The suite pins
        // that by loading one note per delivered event -- a store that hands out a session it never
        // commits passes every delivery fact and loses these silently. The Store call is the half
        // that cannot be shared, because IDocumentOperations is Polecat's own type.
        foreach (var note in NotesFor(page))
        {
            operations.Store(note);
        }

        // NullChangeListener would satisfy the signature and lose the other half of the same
        // guarantee: the listener contract is not "it ran" but "it ran AFTER the commit", and only a
        // real listener can report that.
        return Task.FromResult<IChangeListener>(new ComplianceSubscriptionListener(this));
    }
}

/// <summary>
///     The change listener Polecat's <see cref="ComplianceSubscription" /> returns.
/// </summary>
/// <remarks>
///     A separate type rather than the subscription itself, because Polecat's
///     <see cref="IChangeListener" /> takes <c>(IDocumentSession, IChangeSet, CancellationToken)</c>
///     while Marten's and Fisher's differ -- which is why the library asks each consumer to declare
///     its own and only shares <c>RecordCommitAsync</c>. Only the after-commit hook records: the
///     suite's probe reads committed state, and the before-commit hook runs inside the transaction
///     that has not committed yet.
/// </remarks>
internal sealed class ComplianceSubscriptionListener : IChangeListener
{
    private readonly ComplianceSubscription _subscription;

    public ComplianceSubscriptionListener(ComplianceSubscription subscription)
    {
        _subscription = subscription;
    }

    public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        => _subscription.RecordCommitAsync();

    public Task BeforeCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        => Task.CompletedTask;
}
