using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 7 enrollments. Separate file only while the suites are in flight upstream.
 */

public class rebuild_and_catch_up_compliance
    : RebuildAndCatchUpCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class dead_letter_compliance
    : DeadLetterCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
