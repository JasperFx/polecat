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

/*
 * #443 / jasperfx#647 -- the DOCUMENT compliance suites, the slice JasperFx.Events did not cover
 * before. Same model as the event sourcing enrollment above: the behavior lives once in
 * JasperFx.Events.ComplianceTests and runs here against Polecat through the shared
 * JasperFx.Events.Documents contracts, which Polecat's own session types implement directly rather
 * than through an adapter.
 */

public class polecat_document_session_compliance
    : DocumentSessionCompliance<PolecatDocumentComplianceFixture>;

public class polecat_document_load_and_store_compliance
    : DocumentLoadAndStoreCompliance<PolecatDocumentComplianceFixture>;

public class polecat_document_delete_compliance
    : DocumentDeleteCompliance<PolecatDocumentComplianceFixture>;

public class polecat_document_query_compliance
    : DocumentQueryCompliance<PolecatDocumentComplianceFixture>;

/*
 * #559 / jasperfx#785 -- numeric revision semantics. Enrolled as part of adopting the ruled
 * strictly-greater contract: an explicit revision is the version the caller is asking the document
 * to BECOME, accepted only when it exceeds what is stored, and honoured verbatim on insert. Polecat
 * previously read it as an equality expectation and always auto-incremented, and hard-coded the
 * insert to 1; the suite is what holds all three SQL sites to the same rule.
 */

public class polecat_numeric_revision_compliance
    : NumericRevisionCompliance<PolecatDocumentComplianceFixture>;

/*
 * #592 / jasperfx#819 -- Guid optimistic concurrency, the document-concurrency suite that did not
 * exist for ANY store before 2.69.0. The whole shared coverage of document concurrency was
 * NumericRevisionCompliance above; grepping the 2.68.0 suites for IVersioned returned nothing.
 *
 * Two stores shipped the same field broken in two different ways and neither was caught by anything
 * shared -- fisher#245 (the guard fed from the session's own version tracker, so a document loaded
 * in one session and stored through another failed its guard EVERY time) and marten#5372 (a mapped
 * version member invisible to the session, so the upsert bound DBNull into its guard). Fisher passed
 * all fifty enrolled suites throughout. Polecat's behaviour here was simply unverified, which is the
 * point of enrolling.
 */

public class polecat_guid_optimistic_concurrency_compliance
    : GuidOptimisticConcurrencyCompliance<PolecatDocumentComplianceFixture>;

/*
 * jasperfx#842 / #843 -- the shared SEARCH suite, and the first time those facts have run against
 * Polecat's engine rather than against a design argument. Polecat implements
 * IDocumentSearchOperations already (#633), so this is enrollment rather than a feature.
 *
 * Both halves are enrolled. The fixture flips SupportsVectorSearch and SupportsHybridSearch, and it
 * has to replay DocumentComplianceConfig.VectorIndexes and FullTextIndexes as well -- a vector
 * search reads a DECLARED index, which on Polecat is what creates the persisted computed VECTOR(n)
 * column the ORDER BY reads, so a fixture that flipped the flags and dropped the replay fails every
 * fact rather than skipping them.
 *
 * ⚠️ The filter facts (jasperfx#843) are the cheap ones here and that is worth saying out loud.
 * They assert that a predicate excluding every globally-nearest row still returns the full limit,
 * which an exact scan gives for free and an approximate index does not -- pgvector applies the
 * predicate after an index scan bounded by hnsw.ef_search. Polecat scans the computed column
 * exactly, so there is no candidate bound for a filtered row to fall outside of. Passing these says
 * nothing about Marten, where they are the hard ones.
 *
 * The suite is deliberately NOT a relevance suite. What it holds is nearest-first, that the score is
 * a DISTANCE on the vector side and the OPPOSITE on the hybrid side, that the store's implicit
 * predicates (tenancy, soft delete, hierarchy) apply as they do to Query<T>(), and that a filter
 * narrows BEFORE the limit. Polecat's own tests own the tokenizer and the BM25 constants, which
 * nothing shared could state.
 */

public class polecat_document_search_compliance
    : DocumentSearchCompliance<PolecatDocumentComplianceFixture>;
