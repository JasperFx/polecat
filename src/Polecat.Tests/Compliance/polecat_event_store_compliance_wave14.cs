using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 14 -- the last two shipped suites nobody had enrolled in, and Polecat enrolls in both.
 *
 * CompositeProjectionCompliance is jasperfx#725: several members sharing one shard, one progression
 * row and one event batch, executed in stage order and torn down together on rebuild. Opt-in through
 * the AddCompositeProjection seam member (throwing default), which PolecatComplianceFixture now
 * implements by forwarding to Projections.CompositeProjectionFor. Polecat already guards the
 * teardown-on-rebuild half locally (Bug_439_composite_member_teardown), which is exactly the "same
 * regression test in two products" smell the shared suite exists to absorb.
 *
 * SingleTenantedEventSlicingCompliance is jasperfx#724 (wolverine#2053 / marten#4085): on a
 * single-tenanted store, events whose tenant_id values disagree must still fold into ONE async
 * aggregate rather than being sliced per tenant into partial documents. Polecat's fix was #526.
 * Its one fact currently SELF-SKIPS on Polecat, by the suite's own design: Polecat is
 * QuickAppend-only, and the quick-append metadata path (the shared
 * StreamAction.applyQuickMetadata stamps session.TenantId unconditionally, plus
 * EventOperations.Append's action.TenantId assignment, whose setter restamps every event)
 * normalizes disagreeing per-event tenant ids to the session tenant before they reach storage --
 * so the mixed-tenancy precondition cannot be constructed through the shared surface. The suite
 * detects that and skips rather than passing vacuously. Enrollment still arms the assertion
 * should the append path ever start preserving per-event tenant ids.
 */

public class composite_projection_compliance
    : CompositeProjectionCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class single_tenanted_event_slicing_compliance
    : SingleTenantedEventSlicingCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
