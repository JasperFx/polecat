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

    // Per-database daemon cache. Kept in the subclass per the base's contract
    // (each store keeps its own cache + concurrency model); the coordinator loop
    // is single-threaded so a plain dictionary is sufficient.
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
    }

    protected override IProjectionDaemon ResolveDaemon(IProjectionSet set)
    {
        // set.Database is typed IProjectionDatabase on the lifted contract;
        // Polecat's distributors only ever publish PolecatDatabase-backed sets,
        // so the cast is safe.
        return DaemonFor((PolecatDatabase)set.Database);
    }

    protected override IReadOnlyList<IProjectionDaemon> ResolvedDaemons()
        => _daemons.Values.Cast<IProjectionDaemon>().ToList();

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
        if (!_daemons.TryGetValue(database.Identifier, out var daemon))
        {
            daemon = database.StartProjectionDaemon(_store, _loggerFactory);
            _daemons[database.Identifier] = daemon;
        }
        return daemon;
    }

    private static IProjectionDistributor BuildDistributor(DocumentStore store, ILoggerFactory loggerFactory)
    {
        // Mirrors Marten's pick: tenancy with multiple databases →
        // MultiTenantedProjectionDistributor; single-database tenancy →
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
        var cardinality = options.Tenancy?.Cardinality ?? DatabaseCardinality.Single;

        // Shared closures — both branches need shard accessor + set factory
        // backed by Polecat's IProjectionSet impl (ProjectionSet.cs, retained
        // per Polecat#117 / PR #118).
        Func<IEnumerable<ShardName>> allShards =
            () => options.Projections.AllShards().Select(s => s.Name);

        Func<IProjectionDatabase, IReadOnlyList<ShardName>, int, IProjectionSet> setFactory =
            (db, names, lockId) => new ProjectionSet(lockId, (PolecatDatabase)db, names);

        // sp_getapplock is session-bound and shared by both multi-node
        // distributors. Weasel.SqlServer.AdvisoryLock (weasel#284 / α7)
        // satisfies the lifted JasperFx.Events.Daemon.IAdvisoryLock contract
        // directly, so this closure is the only Polecat-side bridge needed.
        var lockLogger = loggerFactory.CreateLogger<AdvisoryLock>();
        Func<IProjectionDatabase, IAdvisoryLock> lockFactory =
            db => new AdvisoryLock(
                () => new SqlConnection(((PolecatDatabase)db).ConnectionString),
                lockLogger,
                db.Identifier);

        var baseLockId = options.DaemonSettings.DaemonLockId;

        if (cardinality == DatabaseCardinality.StaticMultiple)
        {
            return new MultiTenantedProjectionDistributor(
                databaseSource: () => ValueTask.FromResult<IReadOnlyList<IProjectionDatabase>>(
                    options.Tenancy?.AllDatabases().Cast<IProjectionDatabase>().ToList() ?? []),
                allShards: allShards,
                lockFactory: lockFactory,
                setFactory: setFactory,
                baseLockId: baseLockId);
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
            baseLockId: baseLockId);
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
