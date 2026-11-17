using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 21 (JasperFx 2.98.0 -> 2.100.0, polecat#930). The three new facts need no subclass of their own:
 * NaturalKeyCompliance's tombstone-reuse facts, AsyncDaemonCompliance's in-flight append fact and
 * ConjoinedEventTenancyCompliance's single-save two-tenant fact all ride suites Polecat already
 * enrolls. What this wave adds is the inventory guard (jasperfx#1123 / #1091): a compliance suite can
 * no longer be left out silently.
 */
public class polecat_compliance_suite_inventory
{
    [Fact]
    public void every_event_compliance_suite_is_enrolled_or_excused()
    {
        ComplianceSuiteInventory.AssertEnrolled(typeof(polecat_compliance_suite_inventory).Assembly,
            new PolecatComplianceFixture(), new Dictionary<Type, string>
            {
                // JasperFx 3.6.0 added the suite (jasperfx#1201). Polecat adopts the per-identity rebuild in
                // #1060, after the Marten adoption (marten#6071), and enrolls the suite then
                [typeof(IdentityRebuildCompliance<,,>)] =
                    "per-identity rebuild is not adopted yet (polecat#1060, after marten#6071)"
            });
    }

    [Fact]
    public void every_document_compliance_suite_is_enrolled_or_excused()
    {
        ComplianceSuiteInventory.AssertEnrolled(typeof(polecat_compliance_suite_inventory).Assembly,
            new PolecatDocumentComplianceFixture(), new Dictionary<Type, string>());
    }
}
