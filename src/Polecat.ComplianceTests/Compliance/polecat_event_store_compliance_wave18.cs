using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 18 -- the two multi-database explorer arms (#593 / jasperfx#810). Split upstream because the
 * gaps live on independent axes and one configuration cannot express both: database-per-tenant has
 * no co-located tenants, so it cannot see a dropped tenant_id predicate; sharded tenancy has them,
 * so it cannot tell "the wrong database answered" from "the right database answered about the wrong
 * tenant".
 *
 * Both are the first suites in the set to make this fixture build REAL extra databases -- see
 * SupportsMultipleDatabases and the TenantDatabases replay on PolecatComplianceFixture. Both also
 * open with a precondition fact, because a fixture that claims the capability and builds one
 * database satisfies every "and not the other one's data" assertion by having no other one.
 */

/// <summary>
///     Database-per-tenant: each tenant has a database of its own, and a store-global read has to
///     say so rather than answering from one of them.
/// </summary>
/// <remarks>
///     Polecat's store-global reads were already brought into line by #584 — the recent-streams
///     listing fans out and merges, and the rest refuse and name the database overload. The suite
///     accepts either remedy, so this arm is the shared confirmation of a decision Polecat had
///     already made rather than a change.
/// </remarks>
public class polecat_database_per_tenant_explorer_compliance
    : DatabasePerTenantExplorerCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;

/// <summary>
///     Sharded tenancy — a pool of databases with many tenants co-located in each, conjoined. The arm
///     that pins the <c>tenant_id</c> predicate.
/// </summary>
/// <remarks>
///     <para>
///         Three tenants over two databases: two co-located so a leak has somewhere to come from, and
///         a third alone in the second shard so the store's cardinality is genuinely not
///         <c>Single</c> — which is the whole precondition, since it is the cardinality test that
///         turns the predicate off on the store that gets this wrong.
///     </para>
///     <para>
///         Polecat filters by tenant wherever events are conjoined and decides that from tenancy
///         style rather than from cardinality, so this arm is the rule to PRESERVE while adding the
///         database dimension rather than one to fix. What it did change here is the routing: a
///         tenant-scoped read used to refuse on a multi-database store, and a tenant names the
///         database it lives in, so it now resolves to that one database and keeps the predicate on
///         top of it.
///     </para>
/// </remarks>
public class polecat_sharded_tenancy_explorer_compliance
    : ShardedTenancyExplorerCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
