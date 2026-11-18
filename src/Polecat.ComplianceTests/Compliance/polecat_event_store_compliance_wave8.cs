using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 8 enrollments -- the LAST wave. With these two the cross-store event sourcing compliance
 * backlog (marten#5118) is empty and Polecat is at exact parity with Marten: 28 suites.
 */

public class conjoined_event_tenancy_compliance
    : ConjoinedEventTenancyCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class subscription_compliance
    : SubscriptionCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
