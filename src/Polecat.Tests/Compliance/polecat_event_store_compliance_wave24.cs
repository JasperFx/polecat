using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 24 -- JasperFx.Events.ComplianceTests 3.2.0 (jasperfx#1183, polecat#975).
 *
 *   - DcbAppendConditionCompliance: the DCB append condition under real concurrency, lifted from
 *     marten#5887. It found the two bugs Marten fixed there, both in Polecat too. #972: the boundary
 *     check bumped the version of every tag it read, so two boundaries over one tag value always
 *     conflicted. #973: the check ignored AndEventsOfType, so an event of a type the query does not
 *     select failed the save. DcbConflictScope.TagValueAndEventType, the 6.0 default, is the fix for
 *     the second.
 *
 *     PolecatComplianceFixture maps ComplianceStoreConfig.DisableWriteRetries to an empty resilience
 *     pipeline. A retried deadlock commits, so with retries on the deadlock facts pass on a store that
 *     deadlocks.
 */

public class dcb_append_condition_compliance
    : DcbAppendConditionCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
