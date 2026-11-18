using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 17 -- the projection-status suite (#591 / jasperfx#818), the follow-on to #589. Enrolled on
 * its own because it is the definition of done for one field's meaning rather than the adoption of a
 * suite for a whole capability.
 */

/// <summary>
///     <c>GetProjectionStatusesAsync</c> — the snapshot a monitoring console's projections page
///     renders before it subscribes to <c>ShardStatesChanged</c>, and what its five fields mean.
/// </summary>
/// <remarks>
///     <para>
///         #589 closed most of the gap: Polecat answers <c>Unknown</c> rather than <c>Stopped</c>
///         when no daemon is visible, <c>EventStoreSequence</c> is the head of the event store rather
///         than the high-water progression row, and the <c>State</c> slot no longer carries a
///         lifecycle on some rows and a daemon state on others.
///     </para>
///     <para>
///         The fact this wave is actually for is
///         <c>a_reachable_running_daemon_reports_the_real_shard_state</c>, the other half of the same
///         ruling: <c>Unknown</c> has to mean "there is no daemon here to ask" and nothing else, so a
///         store that can reach one reports what it says. It runs against the coordinator host rather
///         than the fixture's own store, because a hand-built daemon is precisely the case
///         <c>Unknown</c> describes.
///     </para>
/// </remarks>
public class polecat_projection_status_compliance
    : ProjectionStatusCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
