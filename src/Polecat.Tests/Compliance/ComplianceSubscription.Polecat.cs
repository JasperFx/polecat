using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat;
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
        // Polecat already ships a public no-op listener for subscriptions that need no commit
        // hooks, so there is nothing to hand-roll here.
        return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
    }
}
