using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Polecat's enrollment in the cross-store event sourcing compliance suites. Each class below is
 * empty on purpose: the behavior lives once in JasperFx.Events.ComplianceTests and is closed here
 * over Polecat's IEventStore<IDocumentSession, IQuerySession> session pair through
 * PolecatComplianceFixture. Marten enrolls the same way, so drift between the two products' copies
 * of these tests is no longer possible.
 */

public class self_aggregating_evolve_compliance
    : SelfAggregatingEvolveCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class dcb_tag_query_and_consistency_compliance
    : DcbTagQueryAndConsistencyCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class assign_tag_where_compliance
    : AssignTagWhereCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class async_daemon_compliance
    : AsyncDaemonCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class auto_discovered_aggregate_compliance
    : AutoDiscoveredAggregateCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class event_projection_registration_compliance
    : EventProjectionRegistrationCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class event_projection_enrichment_compliance
    : EventProjectionEnrichmentCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class rebuild_concurrency_cap_compliance
    : RebuildConcurrencyCapCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class activity_correlation_compliance
    : ActivityCorrelationCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class string_identity_single_stream_compliance
    : StringIdentitySingleStreamCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
