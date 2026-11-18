using Microsoft.Data.SqlClient;
using Polecat.TestUtils;

namespace Polecat.Tests.Harness;

/// <summary>
///     Serializes the per-tenant physical-partitioning tests. SQL Server partition functions and
///     schemes are database-global objects (their names — e.g. <c>pf_pc_events_tenant_ordinal</c> —
///     carry no schema), so two tenant-partitioned <c>pc_events</c> tables in different schemas of the
///     same database would collide if their tests ran concurrently. DisableParallelization keeps them
///     strictly sequential; <see cref="PartitionTestCleanup" /> wipes the shared objects between runs.
/// </summary>
[CollectionDefinition("tenant-partitioning", DisableParallelization = true)]
public class TenantPartitioningCollection;
