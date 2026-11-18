using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 4 enrollments. Separate file only while the suites are still in flight upstream; fold into
 * polecat_event_store_compliance.cs once the JasperFx package ships them.
 */

public class fetch_for_writing_compliance
    : FetchForWritingCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class stream_read_compliance
    : StreamReadCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class event_metadata_compliance
    : EventMetadataCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class live_aggregation_compliance
    : LiveAggregationCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
