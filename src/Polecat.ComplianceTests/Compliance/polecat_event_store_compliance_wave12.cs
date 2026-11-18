using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 12 -- the suite JasperFx.Events 2.62.0 added (jasperfx#737, polecat#532).
 *
 * EventQueryCompliance is the cross-stream query surface -- IReadOnlyEventStore.QueryEventsAsync
 * over the broadened EventQuery: every filter field (multi-type union, inclusive timestamp and
 * sequence windows, folded DCB tag conditions, the metadata trio), the sequence-ascending
 * ordering contract, and the paging/TotalCount contract. Every fact asserts the filter actually
 * FILTERED -- exact membership and totals -- never that the call succeeded, because the failure
 * mode the design refuses is a silently-ignored filter reading as a filtered result. Polecat's
 * declaration lives in QueryEventStore.SupportedEventQueryFilters(): everything structural always,
 * the metadata filters exactly when the store captures the column (EnableCorrelationId /
 * EnableCausationId / EnableUserName), refused via EventQuery.AssertFiltersAreSupported otherwise.
 *
 * The suite turns on ComplianceStoreConfig.EnableUserNameTracking (the jasperfx#737 fixture seam,
 * mapped to Events.EnableUserName) and drives the user_name column through the fixture's
 * SetUserName -> session.LastModifiedBy. Tag conditions ride Polecat's existing DCB tag tables:
 * each EventTagQuerySpec condition compiles to the same correlated seq_id IN (SELECT ... FROM
 * pc_event_tag_*) subquery as the HasTag() LINQ path, OR'd per condition, AND-combined with the
 * rest of the query.
 *
 * The EventQuery.TenantId filter is deliberately NOT here -- its two facts live in
 * ConjoinedEventTenancyCompliance (wave 8), which owns the conjoined store configuration.
 *
 * Enrolling this suite surfaced an upstream defect in the suite itself, fixed before the pin
 * landed here: paging_composes_with_filtering originally seeded with `i % 3 == 0` over i = 0..9
 * (four CargoLoaded, six CargoInspected) while asserting seven matches per its own "Seven
 * Inspected events interleaved with three Loaded" intent. Polecat correctly answered 6. Fixed in
 * jasperfx PR #739, included in the published JasperFx.Events.ComplianceTests 2.62.0 this repo
 * pins, so the suite runs 41/41 here.
 */

public class polecat_event_query_compliance
    : EventQueryCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
