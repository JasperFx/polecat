using JasperFx.Events;
using JasperFx.Descriptors;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Events.Daemon.Coordination;

/// <summary>
///     Concrete <see cref="IProjectionCoordinator"/> for Polecat. Hosted as
///     an <see cref="IHostedService"/> so the runtime starts/stops it with
///     the application lifetime.
/// </summary>
/// <remarks>
///     The leadership-election + agent-lifecycle loop lives in the lifted
///     <see cref="ProjectionCoordinatorBase"/> (jasperfx#326) — Polecat's old
///     <c>ExecuteAsync</c> was a slot-for-slot mirror of Marten's, so this
///     subclass only supplies the store-specific seams: the distributor
///     (chosen by <see cref="ITenancy.Cardinality"/>), the per-database daemon
///     cache, and the three tenancy-aware daemon accessors. Adopting the base
///     also gives Polecat Marten's resilient agent-start — a failed
///     <c>StartAgentAsync</c> now ejects the paused shard and releases the
///     set's lock so another node can pick it up (Polecat previously started
///     agents raw with no lock-release-on-failure).
/// </remarks>
internal class ProjectionCoordinator : ProjectionCoordinatorBase, IProjectionCoordinator
{
    private readonly DocumentStore _store;
    private readonly StoreOptions _options;
    private readonly ILoggerFactory _loggerFactory;

    // Per-database daemon cache. Kept in the subclass per the base's contract (each store keeps its own
    // cache + concurrency model). Every access takes the dictionary's lock: the coordinator loop is
    // single-threaded but it is not the only caller. DaemonForDatabase is public and a health check's
    // autoRestart and #591's status read both reach it, and under a dynamic tenancy the set grows while
    // they do, so an unguarded read in the loop's own pause check could throw and end the loop for good.
    private readonly Dictionary<string, PolecatProjectionDaemon> _daemons = new();

    public ProjectionCoordinator(DocumentStore store, ILoggerFactory? loggerFactory = null)
        : base(
            BuildDistributor(store, loggerFactory ?? NullLoggerFactory.Instance),
            (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<ProjectionCoordinator>(),
            store.Options.ResiliencePipeline,
            TimeProvider.System,
            // LeadershipPollingTime is an int (milliseconds); the other two are already TimeSpans.
            TimeSpan.FromMilliseconds(store.Options.DaemonSettings.LeadershipPollingTime),
            store.Options.DaemonSettings.AgentPauseTime,
            store.Options.DaemonSettings.HealthCheckPollingTime)
    {
        _store = store;
        _options = store.Options;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;

        // #591: hand the store a way back to its coordinator. GetProjectionStatusesAsync reports
        // ShardStatus.State from the running daemon when one is reachable, and the store has no
        // container to resolve a coordinator from -- but the coordinator is always constructed with
        // the store it coordinates, so the reference travels in this direction instead.
        store.AttachCoordinator(this);
    }

    protected override IProjectionDaemon ResolveDaemon(IProjectionSet set)
    {
        // set.Database is typed IProjectionDatabase on the lifted contract;
        // Polecat's distributors only ever publish PolecatDatabase-backed sets,
        // so the cast is safe.
        return DaemonFor((PolecatDatabase)set.Database);
    }

    protected override IReadOnlyList<IProjectionDaemon> ResolvedDaemons()
    {
        lock (_daemons)
        {
            return _daemons.Values.Cast<IProjectionDaemon>().ToList();
        }
    }

    // marten#5055: StopAsync disposes everything ResolvedDaemons() hands back, so the cache must be
    // purged too — otherwise a second Pause/Stop fans StopAllAsync out over disposed daemons and a
    // later ResumeAsync hands back a dead instance instead of building a fresh one.
    protected override void ClearResolvedDaemons()
    {
        lock (_daemons)
        {
            _daemons.Clear();
        }
    }

    public override IProjectionDaemon DaemonForMainDatabase()
    {
        var main = _options.Tenancy?.AllDatabases().FirstOrDefault()
            ?? throw new InvalidOperationException(
                "No databases registered on the active tenancy; cannot resolve a main projection daemon.");
        return DaemonFor(main);
    }

    public override ValueTask<IProjectionDaemon> DaemonForDatabase(string databaseIdentifier)
    {
        var match = _options.Tenancy?.AllDatabases()
            .FirstOrDefault(d => string.Equals(d.Identifier, databaseIdentifier, StringComparison.Ordinal))
            ?? throw new InvalidOperationException(
                $"No database registered with identifier '{databaseIdentifier}' on the active tenancy.");
        return ValueTask.FromResult<IProjectionDaemon>(DaemonFor(match));
    }

    public override ValueTask<IReadOnlyList<IProjectionDaemon>> AllDaemonsAsync()
    {
        var databases = _options.Tenancy?.AllDatabases() ?? [];
        IReadOnlyList<IProjectionDaemon> daemons = databases.Select(DaemonFor).Cast<IProjectionDaemon>().ToList();
        return ValueTask.FromResult(daemons);
    }

    private PolecatProjectionDaemon DaemonFor(PolecatDatabase database)
    {
        lock (_daemons)
        {
            if (!_daemons.TryGetValue(database.Identifier, out var daemon))
            {
                daemon = database.StartProjectionDaemon(_store, _loggerFactory);
                _daemons[database.Identifier] = daemon;
            }

            return daemon;
        }
    }

    /// <summary>
    ///     sp_getapplock is session-bound and shared by both multi-node distributors.
    ///     Weasel.SqlServer.AdvisoryLock (weasel#284 / α7) satisfies the lifted
    ///     JasperFx.Events.Daemon.IAdvisoryLock contract directly, so this closure is the only
    ///     Polecat-side bridge needed.
    /// </summary>
    /// <remarks>
    ///     #749: the lock's monitor settings come from <see cref="EventStoreOptions.ConfigureAdvisoryLock" />,
    ///     built ONCE here rather than per lock, so the action runs once however many databases the
    ///     coordinator spans. AdvisoryLock reads the values in its constructor and keeps no reference, so
    ///     sharing the instance is safe.
    /// </remarks>
    internal static Func<IProjectionDatabase, IAdvisoryLock> BuildLockFactory(StoreOptions options,
        ILoggerFactory loggerFactory)
    {
        var lockLogger = loggerFactory.CreateLogger<AdvisoryLock>();
        var lockOptions = options.Events.BuildAdvisoryLockOptions();

        return db => new AdvisoryLock(
            () => new SqlConnection(((PolecatDatabase)db).ConnectionString),
            lockLogger,
            db.Identifier,
            lockOptions);
    }

    private static IProjectionDistributor? BuildDistributor(DocumentStore store, ILoggerFactory loggerFactory)
    {
        // Mirrors Marten's pick: tenancy with multiple databases, static or
        // dynamic → MultiTenantedProjectionDistributor; single-database tenancy →
        // SingleTenantProjectionDistributor (per-shard hot-cold locks via
        // sp_getapplock).
        //
        // Polecat#119 (Critter Stack 2026 dedupe pillar): the distributor
        // concretes themselves were lifted into JasperFx.Events.Daemon in
        // jasperfx#318/#319/#320. Polecat wires them with closures over
        // store-specific state (tenancy / shards / advisory-lock factory /
        // ProjectionSet factory / schema / base lock id) rather than
        // shipping its own near-identical concretes.
        var options = store.Options;

        // Polecat#141 (jasperfx#352): nothing to coordinate when the async daemon
        // is disabled, so don't build a distributor that would never be used. The
        // lifted ProjectionCoordinatorBase now tolerates a null distributor — its
        // ctor no longer throws, StartAsync no-ops, and StopAsync guards
        // ReleaseAllLocks — so returning null here is the "nothing to coordinate"
        // signal. Mirrors Marten's intended end state (marten#4537).
        if (options.DaemonSettings.AsyncMode == DaemonMode.Disabled)
        {
            return null;
        }

        var cardinality = options.Tenancy?.Cardinality ?? DatabaseCardinality.Single;

        // Shared closures — both branches need shard accessor + set factory
        // backed by Polecat's IProjectionSet impl (ProjectionSet.cs, retained
        // per Polecat#117 / PR #118).
        Func<IEnumerable<ShardName>> allShards =
            () => options.Projections.AllShards().Select(s => s.Name);

        Func<IProjectionDatabase, IReadOnlyList<ShardName>, int, IProjectionSet> setFactory =
            (db, names, lockId) => new ProjectionSet(lockId, (PolecatDatabase)db, names);

        var lockFactory = BuildLockFactory(options, loggerFactory);

        var baseLockId = options.DaemonSettings.DaemonLockId;

        // #697 / jasperfx#489: under per-tenant event sequencing each tenant has its own seq_id space,
        // so a store-global agent per shard cannot represent the store — the distributor has to expand
        // each shard name into one name per tenant (PerTenantShardExpansion, fed by PolecatDatabase's
        // ICrossTenantRebuildSource). Both distributors below kept the ORIGINAL constructor overload,
        // whose body is `: this(..., distributesAgentsPerTenant: false)`, so the expansion existed and
        // was simply never asked for: a tenant-partitioned store started one agent for the whole store
        // and no tenant's projections ever advanced. Lock granularity is unchanged either way — still
        // one advisory lock per store-global shard, with the winning node running that shard's tenant
        // agents.
        var distributesAgentsPerTenant = ((IEventStore)store).DistributesAgentsPerTenant;

        // #847: DynamicMultiple belongs here too. It used to fall through to the single-database
        // distributor below, whose AllDatabases().Single() threw on every cycle with none or several
        // tenants, so master table tenancy never started an agent. Everything below the Single check is
        // "more than one database", which is what this distributor is for.
        if (cardinality != DatabaseCardinality.Single)
        {
            return new MultiTenantedProjectionDistributor(
                // BuildDatabasesAsync rather than AllDatabases(): a dynamic tenancy re-reads its master
                // table there, and this runs on every leadership cycle, so that is how a tenant added
                // while the host runs is found. AllDatabases() only answers from what is already cached.
                databaseSource: async () =>
                    (await options.Tenancy!.BuildDatabasesAsync().ConfigureAwait(false))
                    .Cast<IProjectionDatabase>().ToList(),
                allShards: allShards,
                lockFactory: lockFactory,
                setFactory: setFactory,
                baseLockId: baseLockId,
                distributesAgentsPerTenant: distributesAgentsPerTenant);
        }

        // Single-database tenancy → exactly one PolecatDatabase. The lifted
        // SingleTenant distributor takes a `Func<IProjectionDatabase>` (single,
        // not list); .Single() asserts the cardinality invariant — a
        // misconfigured deployment surfaces here with a useful exception
        // rather than silently no-opping.
        return new SingleTenantProjectionDistributor(
            databaseAccessor: () => options.Tenancy!.AllDatabases().Single(),
            allShards: allShards,
            lockFactory: lockFactory,
            setFactory: setFactory,
            schemaQualifier: options.DatabaseSchemaName,
            baseLockId: baseLockId,
            distributesAgentsPerTenant: distributesAgentsPerTenant);
    }
}

/// <summary>
///     Typed marker variant of <see cref="ProjectionCoordinator"/> for
///     ancillary store registrations in DI.
/// </summary>
internal sealed class ProjectionCoordinator<T> : ProjectionCoordinator, IProjectionCoordinator<T>
    where T : class, IDocumentStore
{
    public ProjectionCoordinator(DocumentStore store, ILoggerFactory? loggerFactory = null)
        : base(store, loggerFactory)
    {
    }
}
