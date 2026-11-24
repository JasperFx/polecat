using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 28 -- JasperFx.Events.ComplianceTests 3.14.0 (jasperfx#1282, marten#6152, polecat#1130).
 *
 *   - AggregateStreamStatsCompliance: IEventStore.RegisteredAggregateTypes() answers from the projection graph,
 *     and PolecatDatabase.FetchAggregateStreamStatsAsync counts pc_streams by aggregate type in one round trip,
 *     leaving archived streams out unless asked for and narrowing by tenant under conjoined tenancy.
 */

public class aggregate_stream_stats_compliance
    : AggregateStreamStatsCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

/*
 * Also new in 3.13 (marten 696141240), and found unenrolled by polecat_compliance_suite_inventory:
 *
 *   - NaturalKeyRegistrationCompliance: the natural key registrations a store refuses while it is built, and the
 *     TKey / TKey? mixes it accepts. The owned-key facts run; the mapped-key facts skip.
 *   - MappedNaturalKeyCompliance: every fact skips, because Polecat has no mapped natural keys yet
 *     (SupportsMappedNaturalKeys is false on PolecatComplianceFixture, with the reason beside it).
 */

public class natural_key_registration_compliance
    : NaturalKeyRegistrationCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

public class mapped_natural_key_compliance
    : MappedNaturalKeyCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
