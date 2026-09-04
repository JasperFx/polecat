using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 13 -- the suite the jasperfx#740 wave added (polecat#534), sibling of wave 12's
 * EventQueryCompliance.
 *
 * StreamStateQueryCompliance is IReadOnlyEventStore.QueryStreamStates(): the pc_streams table as a
 * real IQueryable<StreamState>, executed through the shared JasperFx.Events.Documents terminators
 * (Polecat's StreamStateLinqQueryProvider implements IDocumentQueryExecutor, the same hook the
 * document read tier uses). One Where() fact per public get member -- including the
 * x.AggregateType == typeof(X) form, translated to the stored aggregate-type alias via
 * EventGraph.AggregateAliasFor, and the new CompactedVersion watermark, which Polecat's
 * CompactStreamAsync now WRITES (SetCompactedVersionOperation: partial compaction records the
 * cutoff version, full compaction the stream version, never-compacted reads the column's
 * DEFAULT 0) -- plus the compaction-policy shape itself
 * (AggregateType == typeof(X) && Version - CompactedVersion > N && !IsArchived), the stated
 * ordering (Created ascending, Id tiebreak) with Skip/Take paging, and truthful empty answers.
 *
 * Two refusal shapes the suite deliberately CANNOT pin (both current stores translate everything)
 * live in Polecat's own stream_state_query_refusals tests instead: an untranslatable member throws
 * naming it, and a tenantId on a store without conjoined tenancy is refused.
 *
 * The tenant-scoped overload's happy path lives in ConjoinedEventTenancyCompliance (wave 8), which
 * owns the conjoined store configuration.
 */

public class polecat_stream_state_query_compliance
    : StreamStateQueryCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
