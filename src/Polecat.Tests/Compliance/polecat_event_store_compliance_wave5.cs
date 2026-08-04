using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 5 enrollments. Separate file only while the suites are in flight upstream; fold into
 * polecat_event_store_compliance.cs once the JasperFx package ships them.
 */

public class fetch_latest_compliance
    : FetchLatestCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class stream_archiving_compliance
    : StreamArchivingCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class event_store_explorer_compliance
    : EventStoreExplorerCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
