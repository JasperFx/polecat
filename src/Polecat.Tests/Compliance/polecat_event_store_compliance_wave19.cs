using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 19 -- conjoined tenancy, both halves (JasperFx.Events.ComplianceTests 2.75.0, jasperfx#898 /
 * jasperfx#899; Polecat #681, #682).
 *
 * One new suite is enrolled here, and it is the smaller part of the wave. The larger part arrives as
 * six new facts on ConjoinedEventTenancyCompliance, which Polecat has been enrolled in since wave 8
 * -- so they are live the moment the package version moves, with nothing to add here. What they
 * needed from Polecat was product work rather than enrollment:
 *
 *   - Schema.For<T>().MultiTenanted() (#682): document tenancy was read off Events.TenancyStyle
 *     store-wide, so there was no per-type opt-in and no way to have conjoined documents at all
 *     without a conjoined event store.
 *   - IDocumentStore.LightweightSession(string) / QuerySession(string), both tiers, including the
 *     explicit non-generic forwarders -- the covariance near-miss jasperfx#898 warns about, where a
 *     product-typed overload satisfies the generic interface and leaves the CONTRACT member on its
 *     throwing default.
 *   - IEventStore.OpenReadOnlyEventStore(tenantId) (#678), which was the throwing default.
 *
 * The recurring idea across every fact in both suites is worth stating once: they all reuse ONE id
 * across two tenants. Every tenanted fact in the compliance library before this wave used distinct
 * ids per tenant, and a store that keys on the id alone -- folding two tenants' writes into one row
 * -- passes all of them. It does not fail loudly either; the second write silently overwrites the
 * first and both tenants then read the same row "correctly".
 */

/// <summary>
///     Conjoined <b>document</b> tenancy: one database, many tenants, every document row scoped to a
///     tenant id — the document mirror of <see cref="ConjoinedEventTenancyCompliance{TFixture,TOperations,TQuerySession}" />.
/// </summary>
/// <remarks>
///     <para>
///         Enrolled on <see cref="PolecatDocumentComplianceFixture" /> rather than in a wave file's
///         usual company, and that is the exception the repository's own note describes: a document
///         suite shares nothing with the event-sourcing work it arrived beside. It sits here anyway
///         because the product changes it needed are the wave's, not the document side's.
///     </para>
///     <para>
///         Ten facts, and both concurrency ones run — <c>SupportsOptimisticConcurrency</c> and
///         <c>SupportsNumericRevisions</c> were already true on that fixture. So are the two escape
///         facts: Polecat spells <c>AnyTenant()</c> / <c>TenantIsOneOf(...)</c> as operators on the
///         queryable, which is the seam's Fisher-shaped arm rather than Marten's element predicate.
///     </para>
/// </remarks>
public class polecat_document_conjoined_tenancy_compliance
    : DocumentConjoinedTenancyCompliance<PolecatDocumentComplianceFixture>;
