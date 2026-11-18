using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 25 -- JasperFx.Events.ComplianceTests 3.6.0 (polecat#1060, jasperfx#1201).
 *
 *   - IdentityRebuildCompliance: one identity of a single-stream projection re-folded inside the shard that
 *     runs it. Polecat's re-fold loader (IdentityRebuildEventLoader), HardDeleteById on its projection storage,
 *     and the empty-range batch an idle shard carries the rebuild in. The port of marten#6071.
 */

public class identity_rebuild_compliance
    : IdentityRebuildCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
