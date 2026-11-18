using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 15 -- the nine suites JasperFx 2.64.0 added (jasperfx#752 aside, which is the upcasting
 * suite and rides its own issue).
 *
 * ⚠️ Every one of these had NEVER been executed against a real event store before this enrollment.
 * The JasperFx repo enrolls only the document suites, so until now they were compile-checked and
 * design-reasoned only. Read a failure here as evidence first and a suite bug second -- that is what
 * the adoption is for, and two of them found real Polecat bugs on the first run (#549, #553).
 *
 * Eight of the nine gate on a Supports... flag that defaults FALSE, so enrolling them is not the
 * work; implementing the fixture members they guard is. See PolecatComplianceFixture's wave 15
 * block -- every gate there is true except SupportsCommitVisibilityProbe, which is a deliberate
 * divergence documented at the property.
 */

/// <summary>
///     jasperfx#764 / #549. The natural key lookup table and the FetchForWriting /
///     FetchForExclusiveWriting / FetchLatest triple that resolves through it: wrapped value-type
///     keys and bare string keys, renames that retire the superseded key, conjoined-tenant
///     isolation, both stream identities, archive and clean removal, and repopulation on rebuild.
/// </summary>
/// <remarks>
///     The suite is what settled the one open product question in the cluster and settled it AGAINST
///     Polecat: a key already mapped to a live stream is refused to a second claimant, not repointed.
///     #549 carries that behavioural change, and
///     <c>a_second_stream_cannot_claim_a_live_natural_key</c> is the fact that pins it -- both
///     halves, the throw AND the original mapping surviving the attempt, because asserting only the
///     throw would pass on a store that repoints the row and then fails the transaction.
/// </remarks>
public class natural_key_compliance
    : NaturalKeyCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

/// <summary>
///     jasperfx#754 / #364. <c>AggregateToAsync&lt;T&gt;</c> -- folding an ad hoc raw-event query
///     into one aggregate, with and without an initial state, over both stream identities.
/// </summary>
public class aggregate_to_linq_operator_compliance
    : AggregateToLinqOperatorCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

/// <summary>
///     jasperfx#754 / #364. <c>AggregateToManyAsync&lt;T&gt;</c> -- the same query fanned out through
///     the REGISTERED multi-stream projection, one aggregate per resulting identity.
/// </summary>
/// <remarks>
///     The point of pinning it cross-store is that the operator's whole promise is "the same answer
///     the projection would give": its identity routing, its custom grouper reading reference data
///     off the live session, and its ShouldDelete decisions. A store that reimplemented any of those
///     inline would return plausible aggregates that quietly diverge from the persisted ones. The
///     projections are registered Async and the daemon is never started, so every aggregate asserted
///     can only have come from the live fold.
/// </remarks>
public class aggregate_to_many_compliance
    : AggregateToManyCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

/// <summary>
///     The <c>IEvent.HasTag&lt;TTag&gt;()</c> DCB marker inside a raw-event LINQ query: alone,
///     composed with an ordinary event predicate and with a timestamp predicate, ANDed with a second
///     tag, isolated by tenant, and throwing for an unregistered tag type.
/// </summary>
/// <remarks>
///     The single-Where contract on the fixture seam is load-bearing rather than tidiness: the
///     composition facts assert that a tag predicate composes with ordinary predicates INSIDE ONE
///     predicate tree, which two chained Where() calls would not exercise.
/// </remarks>
public class dcb_has_tag_linq_compliance
    : DcbHasTagLinqCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

/// <summary>
///     jasperfx#763 / #420. <c>RaiseSideEffects</c> -- the projection hook that appends events of its
///     own and publishes messages, and the store-side plumbing underneath that has to carry both.
/// </summary>
/// <remarks>
///     Not opt-in in the sense the others are: the raised-event and rebuild-suppression facts run
///     unconditionally, and only the outbox facts gate on SupportsMessageOutbox. The hook itself is
///     shared; what is not shared is the half each store supplies underneath, and that half has been
///     shipped stubbed empty twice (fisher#61, polecat#420), both times dropping every raised event
///     with no error and no log. Hence the nonzero publish count on every message fact -- "the
///     projection produced the right document" and "nothing threw" are both vacuously true of a store
///     that discarded the side effects.
/// </remarks>
public class projection_side_effect_compliance
    : ProjectionSideEffectCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

/// <summary>
///     <c>IEventStream.AlwaysEnforceConsistency</c> -- opting an individual stream into a version
///     check even when the unit of work appends nothing to it.
/// </summary>
/// <remarks>
///     Baseline rather than gated, because the flag is on the shared IEventStream and every store
///     therefore has to answer for it. The load-bearing fact is the one where nothing is appended, so
///     only the flag can produce a failure.
/// </remarks>
public class always_enforce_consistency_compliance
    : AlwaysEnforceConsistencyCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

/// <summary>
///     #370 / marten#5053. The two stream-fetch query plans -- FetchStreamStatePlan and
///     FetchStreamPlan -- run BOTH standalone and inside a batched query.
/// </summary>
/// <remarks>
///     The [Theory] batched parameter is the reason the suite exists: the two routes compose their
///     SQL separately on every product, so a plan that is correct standalone and wrong in a batch is
///     precisely what this is looking for.
/// </remarks>
public class stream_query_plan_compliance
    : StreamQueryPlanCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

/// <summary>
///     jasperfx#769. The shared <c>ProjectionScenario</c> test harness driven through Polecat's own
///     documented <c>Advanced.EventProjectionScenario</c> entry point.
/// </summary>
/// <remarks>
///     The fixture's RunProjectionScenarioAsync is a FORWARD into that entry point, deliberately not
///     a re-implementation: all three products spell it construct-configure-ExecuteAsync, and a
///     fixture that inlined those three lines would pass the whole suite while the store's advertised
///     entry point was missing or wired to the wrong store. The route is under test as much as the
///     harness behind it.
/// </remarks>
public class projection_scenario_compliance
    : ProjectionScenarioCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

/// <summary>
///     jasperfx#732. That Polecat's DOCUMENTED DI registration produces a reachable
///     <c>IProjectionCoordinator</c> -- resolvable, the same instance as the hosted service, holding
///     its daemons, and still projecting after a pause/resume.
/// </summary>
/// <remarks>
///     Deliberately UNGATED, unlike every other suite in this wave. Every other daemon suite drives a
///     daemon the fixture built by hand, which can never observe whether the registration a user
///     actually writes yields a coordinator at all -- fisher#138 shipped exactly that gap and passed
///     all 37 suites while it did. A skippable registration check would recreate the silent gap the
///     suite exists to close, so a store that has not implemented StartCoordinatorHostAsync fails
///     every fact rather than skipping.
/// </remarks>
public class projection_coordinator_compliance
    : ProjectionCoordinatorCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
