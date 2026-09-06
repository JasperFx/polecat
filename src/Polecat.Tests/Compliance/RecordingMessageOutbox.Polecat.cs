using Polecat;
using Polecat.Events.Aggregation;

namespace JasperFx.Events.ComplianceTests;

/*
 * The Polecat half of the shared RecordingMessageOutbox / RecordingMessageBatch partials
 * (jasperfx#763, compliance wave 15). Third type in the library that an alias cannot reach, after
 * ComplianceFlatTableProjection and ComplianceSubscription, and for the same reason as the second:
 * IMessageOutbox and IMessageBatch are per-product interfaces whose members genuinely differ.
 * Marten's IMessageBatch derives from IChangeListener, so its commit hooks take
 * (IDocumentSession, IChangeSet, CancellationToken); Polecat declares its own two-member interface
 * taking a bare CancellationToken, which is the shape below.
 *
 * The library owns the recording, the locking, the ordering and the commit probe. Everything here is
 * a forward into it -- deliberately, because the point of the suite is that the STORE reaches the
 * outbox at all, not that the recording works.
 *
 * The namespace is the library's, not Polecat's, because these complete types the library declares.
 */

public partial class RecordingMessageOutbox : IMessageOutbox
{
    public ValueTask<IMessageBatch> CreateBatch(IDocumentSession session)
        => new(NewBatch());
}

public partial class RecordingMessageBatch : IMessageBatch
{
    public Task BeforeCommitAsync(CancellationToken token) => RecordBeforeCommitAsync();

    public Task AfterCommitAsync(CancellationToken token) => RecordAfterCommitAsync();
}
