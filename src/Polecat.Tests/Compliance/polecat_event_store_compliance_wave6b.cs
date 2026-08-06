using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Compliance wave 6 (JasperFx 2.43.0) -- the two suites the 2.41.0 lifts existed to enable.
 * Separate file only while the suites are in flight upstream; fold into the main enrollment file
 * once the package ships them.
 */

public class stream_compacting_compliance
    : StreamCompactingCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class event_data_masking_compliance
    : EventDataMaskingCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
