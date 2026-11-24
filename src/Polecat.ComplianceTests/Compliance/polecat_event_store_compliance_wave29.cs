using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 29 -- JasperFx.Events.ComplianceTests 3.15.1 (jasperfx#684, polecat#1147).
 *
 *   - CompositeKeepUnchangedMembersCompliance: with KeepUnchangedMembers on, a rebuild of a composite whose own
 *     version was bumped leaves every member that kept its version standing, documents and progression row both.
 *     Polecat's composite teardown asks CompositeProjection.MembersToTearDownAsync which members to tear down, on
 *     the store-global and the per-tenant paths. No store had run this suite before: Marten excuses it under
 *     marten#5247, and Fisher under fisher#109.
 */

public class composite_keep_unchanged_members_compliance
    : CompositeKeepUnchangedMembersCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
