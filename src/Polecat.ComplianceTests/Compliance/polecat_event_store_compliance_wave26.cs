using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 26 -- JasperFx.Events.ComplianceTests 3.7.0 (jasperfx#1215 / #1222).
 *
 *   - EventRangeMarkCompliance: the event-range marks on IEventDatabase, over pc_event_range_marks.
 *   - EventRewriteCompliance: IEventStore.RewriteStoredEventsAsync, each page written with its cursor in one
 *     transaction, through a deliberately non-idempotent transform.
 *
 * BackfillCompliance is excused in the inventory until polecat#1055.
 */

public class event_range_mark_compliance
    : EventRangeMarkCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class event_rewrite_compliance
    : EventRewriteCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
