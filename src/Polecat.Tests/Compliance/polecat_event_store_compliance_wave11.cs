using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 11 -- the suite JasperFx 2.52.0 added.
 *
 * DocumentCommitListenerCompliance is jasperfx#679 / #485: IDocumentCommitListener, the
 * store-agnostic post-commit session hook, and the IDocumentChangeSet / IDocumentDeletion pair it
 * hands over. Not opt-in in the sense the wave 9 and 10 suites are -- it needs nothing but
 * documents -- but it does need the fixture to replay DocumentComplianceConfig.CommitListeners onto
 * StoreOptions.CommitListeners, which PolecatDocumentComplianceFixture now does.
 *
 * ⚠️ This suite is the ONLY evidence the contract is satisfied, and it is a different situation from
 * every earlier wave. Nothing in IDocumentChangeSet carries a default implementation, so a Polecat
 * type that declares it wrongly is a compile error rather than a member silently bound to a throwing
 * default -- the jasperfx#669 trap cannot bite here. The wiring is what the compiler cannot see: a
 * store that declares both interfaces perfectly and never invokes a listener from
 * DocumentSessionBase.SaveChangesAsync builds clean and passes every other suite in the library.
 *
 * Two behaviours it deliberately does NOT assert, because the contract permits either answer: a
 * SaveChangesAsync with nothing enlisted need not raise a callback, and a session enlisted in a
 * caller's ambient transaction may or may not fire. Polecat's own session tests own those.
 */

public class polecat_document_commit_listener_compliance
    : DocumentCommitListenerCompliance<PolecatDocumentComplianceFixture>;
