using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 22 -- JasperFx.Events.ComplianceTests 2.102.0 (jasperfx#1150).
 *
 *   - PendingEventAgeCompliance: IEventDatabase.FetchFirstEventTimestampsAfterAsync, the timestamp of the
 *     first live event past each shard's position, answered in one round trip. PolecatDatabase looks every
 *     position up in one OPENJSON + OUTER APPLY TOP (1) command over pc_events' clustered key.
 */

public class polecat_pending_event_age_compliance
    : PendingEventAgeCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
