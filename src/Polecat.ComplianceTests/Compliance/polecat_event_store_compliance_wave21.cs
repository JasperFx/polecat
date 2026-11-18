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
    /// <summary>
    ///     #1063: the suites compile into Polecat.Tests.Shared, beside the fixtures, and the enrollments are
    ///     in this assembly, so the inventory reads both. Reading this assembly alone found no suites at all,
    ///     which passes the document inventory without checking anything.
    /// </summary>
    private static Type[] ComplianceTypes()
    {
        var suites = typeof(PolecatComplianceFixture).Assembly.GetTypes();
        suites.ShouldContain(x => x.IsAbstract && x.Name.EndsWith("Compliance`3"));

        return suites.Concat(typeof(polecat_compliance_suite_inventory).Assembly.GetTypes()).ToArray();
    }

    private static void AssertEnrolled(object fixture, Dictionary<Type, string> excused)
    {
        var problems = ComplianceSuiteInventory.Check(ComplianceTypes(), fixture, excused);
        problems.ShouldBeEmpty($"Compliance suite enrollment problems for {fixture.GetType().Name}");
    }

    [Fact]
    public void every_event_compliance_suite_is_enrolled_or_excused()
    {
        AssertEnrolled(new PolecatComplianceFixture(), new Dictionary<Type, string>
        {
            [typeof(BackfillCompliance<,,>)] =
                "Projection backfills wait on the table naming for version-bumped projections (polecat#1055, marten#5632)"
        });
    }

    [Fact]
    public void every_document_compliance_suite_is_enrolled_or_excused()
    {
        AssertEnrolled(new PolecatDocumentComplianceFixture(), new Dictionary<Type, string>());
    }
}
