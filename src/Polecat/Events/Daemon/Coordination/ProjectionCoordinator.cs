using System.Runtime.CompilerServices;
using JasperFx.Core;
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
internal class ProjectionCoordinator : ProjectionCoordinatorBase, IProjectionCoordinator, IAsyncDisposable, IDisposable
{
    private readonly DocumentStore _store;
    private readonly StoreOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private int _disposed;

    // Per-database daemon cache. Kept in the subclass per the base's contract (each store keeps its own
    // cache + concurrency model). Every access takes the dictionary's lock: the coordinator loop is
    // single-threaded but it is not the only caller. DaemonForDatabase is public and a health check's
    // autoRestart and #591's status read both reach it, and under a dynamic tenancy the set grows while
    // they do, so an unguarded read in the loop's own pause check could throw and end the loop for good.
    private readonly Dictionary<string, PolecatProjectionDaemon> _daemons = new();
    private readonly Lock _daemonsLock = new();

    /// <param name="soloTakesNoLocks">
    ///     #847: true from <c>AddAsyncDaemon</c>, which has never taken a lock in
    ///     <see cref="DaemonMode.Solo" /> and which Marten's equivalent does not either. It also starts the
    ///     agents with the host: see <see cref="IHostedService.StartAsync" /> below.
    ///     <c>AddProjectionCoordinator</c> leaves it false, so what it does in that mode has not moved.
    /// </param>
    public ProjectionCoordinator(DocumentStore store, ILoggerFactory? loggerFactory = null,
        bool soloTakesNoLocks = false)
        : this(store, loggerFactory ?? NullLoggerFactory.Instance, soloTakesNoLocks,
            new StrongBox<ProjectionCoordinator?>())
    {
    }

    // The distributor has to exist before the base constructor runs, and its database source needs this
    // instance, so the box is how the one reaches the other.
    private ProjectionCoordinator(DocumentStore store, ILoggerFactory loggerFactory, bool soloTakesNoLocks,
        StrongBox<ProjectionCoordinator?> self)
        : base(
            BuildDistributor(store, loggerFactory, soloTakesNoLocks, self),
            loggerFactory.CreateLogger<ProjectionCoordinator>(),
            store.Options.ResiliencePipeline,
            TimeProvider.System,
            // LeadershipPollingTime is an int (milliseconds); the other two are already TimeSpans.
            TimeSpan.FromMilliseconds(store.Options.DaemonSettings.LeadershipPollingTime),
            store.Options.DaemonSettings.AgentPauseTime,
            store.Options.DaemonSettings.HealthCheckPollingTime)
    {
        _store = store;
        _options = store.Options;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<ProjectionCoordinator>();
        self.Value = this;

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
        lock (_daemonsLock)
        {
            return _daemons.Values.Cast<IProjectionDaemon>().ToList();
        }
    }

    // marten#5055: StopAsync disposes everything ResolvedDaemons() hands back, so the cache must be
    // purged too — otherwise a second Pause/Stop fans StopAllAsync out over disposed daemons and a
    // later ResumeAsync hands back a dead instance instead of building a fresh one.
    protected override void ClearResolvedDaemons()
    {
        lock (_daemonsLock)
        {
            _daemons.Clear();
        }
    }

    /// <summary>
    ///     #847: <c>AddAsyncDaemon(DaemonMode.Solo)</c> has always had its agents running by the time the host
    ///     finishes starting, and failed the host if it couldn't. Callers read the daemon straight after
    ///     <c>StartAsync</c>, so that stays true. The loop then finds them running and only supervises.
    /// </summary>
    /// <remarks>
    ///     A re-implementation rather than an override, because the base's <c>StartAsync</c> isn't virtual
    ///     and <c>ResumeAsync</c> has to go on meaning the loop alone. HotCold can't do this at all: which
    ///     shards a node runs is what the loop is there to find out.
    /// </remarks>
    async Task IHostedService.StartAsync(CancellationToken cancellationToken)
    {
        if (Distributor is SoloProjectionDistributor)
        {
            foreach (var database in await _options.Tenancy!.BuildDatabasesAsync(cancellationToken).ConfigureAwait(false))
            {
                await database.EnsureStorageExistsAsync(typeof(IEvent), cancellationToken).ConfigureAwait(false);
                await DaemonFor(database).StartAllAsync().ConfigureAwait(false);
            }
        }

        await StartAsync(cancellationToken).ConfigureAwait(false);
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
        return ValueTask.FromResult(AllDaemons());
    }

    /// <summary>
    ///     A daemon for every database the tenancy currently has, built on first ask, so this answers
    ///     before the leadership loop has reached a database. <see cref="ResolvedDaemons" /> would not.
    /// </summary>
    internal IReadOnlyList<IProjectionDaemon> AllDaemons()
    {
        var databases = _options.Tenancy?.AllDatabases() ?? [];
        return databases.Select(DaemonFor).Cast<IProjectionDaemon>().ToList();
    }

    private PolecatProjectionDaemon DaemonFor(PolecatDatabase database)
    {
        lock (_daemonsLock)
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
    ///     #871 item 3: stop and drop the daemon of any database the tenancy no longer has, and give its
    ///     lock back. A dynamic tenancy evicts a disabled or removed tenant when it re-reads its master
    ///     table, which the distributor's database source does on every cycle, and that source is the one
    ///     place in the base's loop that runs before the sets are built.
    /// </summary>
    /// <remarks>
    ///     The base can't do this. It only ever acts on the sets it was handed, and a tenant that has gone
    ///     is not in them, so its agents would run on, against a tenant the rest of the node now refuses.
    ///     jasperfx#489's reaping is the same idea one level down, for a tenant's agents within a database
    ///     that is still there.
    /// </remarks>
    private async Task RetireDepartedDatabasesAsync(IReadOnlyList<PolecatDatabase> current)
    {
        var identifiers = current.Select(x => x.Identifier).ToHashSet(StringComparer.Ordinal);

        List<KeyValuePair<string, PolecatProjectionDaemon>> departed;
        lock (_daemonsLock)
        {
            departed = _daemons.Where(x => !identifiers.Contains(x.Key)).ToList();
            foreach (var pair in departed)
            {
                _daemons.Remove(pair.Key);
            }
        }

        foreach (var (identifier, daemon) in departed)
        {
            _logger.LogInformation(
                "Database {Database} has left the tenancy. Stopping its projection agents", identifier);

            // Best effort on both counts. The database may be going away for good, and one that can't be
            // reached must not cost the databases that can be their leadership cycle.
            try
            {
                await daemon.StopAllAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error trying to stop the agents of departed database {Database}", identifier);
            }

            daemon.SafeDispose();

            try
            {
                await Distributor!.ReleaseLockAsync(new ProjectionSet(LockIdForEveryDatabase(_options),
                    (PolecatDatabase)daemon.Database, [])).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error trying to release the lock on departed database {Database}", identifier);
            }
        }
    }

    /// <summary>
    ///     Drain the leadership loop and release the locks, for a host disposed without being stopped.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         #871 item 5, marten#4915. <see cref="IHostedService" /> only promises a StopAsync on a
    ///         graceful shutdown and <c>IHost.Dispose()</c> doesn't stop hosted services, so the common
    ///         <c>using var host</c> shape left this polling, and the session behind its locks open, in a
    ///         process that thought the host was gone. The container built this from the store, so it
    ///         disposes this first, while the store can still reach the database.
    ///     </para>
    ///     <para>
    ///         Unconditional even after a graceful stop, as in Marten: StopAsync is idempotent, and nothing
    ///         distinguishes "stopped" from "stopped, then ResumeAsync started a new loop".
    ///     </para>
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

        GC.SuppressFinalize(this);

        try
        {
            await StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error while draining the ProjectionCoordinator during disposal");
        }
    }

    /// <summary>
    ///     For containers torn down through <see cref="IDisposable" />. The loop runs on the thread pool
    ///     with no captured context, so there is nothing here for the continuation to deadlock against.
    /// </summary>
    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    /// <summary>
    ///     #871 item 6: the lock id of a multi-database set. An application lock is scoped to a database, so
    ///     two event stores in different schemas of one tenant database asked for the same lock, and
    ///     whichever got there second never ran. The single-database distributor already mixes the schema
    ///     in, through <see cref="ProjectionLockIds.Compute" />.
    /// </summary>
    internal static int LockIdForEveryDatabase(StoreOptions options)
    {
        var schema = options.Events.DatabaseSchemaName ?? options.DatabaseSchemaName;
        return Math.Abs(schema.GetDeterministicHashCode()) + options.DaemonSettings.DaemonLockId;
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
    ///     <para>
    ///         #916: the database is looked up each time the lock connects, not captured here. The
    ///         multi-database distributor caches this lock by identifier for as long as it runs, with no way
    ///         to evict one, and under a dynamic tenancy the identifier is the tenant's. A tenant that left
    ///         and came back on another database gets the lock it left behind, and that lock has to follow
    ///         it. What stays cached for a tenant that doesn't come back is the lock's idle monitor timer,
    ///         until the coordinator stops: letting that go needs a way into the distributor from JasperFx.
    ///     </para>
    /// </remarks>
    internal static Func<IProjectionDatabase, IAdvisoryLock> BuildLockFactory(StoreOptions options,
        ILoggerFactory loggerFactory)
    {
        var lockLogger = loggerFactory.CreateLogger<AdvisoryLock>();
        var lockOptions = options.Events.BuildAdvisoryLockOptions();

        return db => new AdvisoryLock(
            () => new SqlConnection(CurrentConnectionString(options, (PolecatDatabase)db)),
            lockLogger,
            db.Identifier,
            lockOptions);
    }

    private static string CurrentConnectionString(StoreOptions options, PolecatDatabase database)
    {
        var current = options.Tenancy?.AllDatabases()
            .FirstOrDefault(x => string.Equals(x.Identifier, database.Identifier, StringComparison.Ordinal));

        return (current ?? database).ConnectionString;
    }

    private static IProjectionDistributor? BuildDistributor(DocumentStore store, ILoggerFactory loggerFactory,
        bool soloTakesNoLocks, StrongBox<ProjectionCoordinator?> self)
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
        // signal. Mirrors Marten's intended end state (marten#4537). ExternallyManaged (jasperfx#490) has
        // the same answer: something else, such as Wolverine's managed distribution, runs the agents.
        if (options.DaemonSettings.AsyncMode is DaemonMode.Disabled or DaemonMode.ExternallyManaged)
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

        // BuildDatabasesAsync rather than AllDatabases(): a dynamic tenancy re-reads its master table there,
        // and this runs on every leadership cycle, so that is how a tenant added while the host runs is
        // found, and how one disabled or removed on another node is lost. AllDatabases() only answers from
        // what is already cached.
        Func<ValueTask<IReadOnlyList<IProjectionDatabase>>> databaseSource = async () =>
        {
            var databases = await options.Tenancy!.BuildDatabasesAsync().ConfigureAwait(false);
            await self.Value!.RetireDepartedDatabasesAsync(databases).ConfigureAwait(false);
            return databases.Cast<IProjectionDatabase>().ToList();
        };

        if (soloTakesNoLocks && options.DaemonSettings.AsyncMode == DaemonMode.Solo)
        {
            return new SoloProjectionDistributor(databaseSource, allShards, setFactory, baseLockId,
                distributesAgentsPerTenant);
        }

        var lockFactory = BuildLockFactory(options, loggerFactory);

        // #847: DynamicMultiple belongs here too. It used to fall through to the single-database
        // distributor below, whose AllDatabases().Single() threw on every cycle with none or several
        // tenants, so master table tenancy never started an agent. Everything below the Single check is
        // "more than one database", which is what this distributor is for.
        if (cardinality != DatabaseCardinality.Single)
        {
            return new MultiTenantedProjectionDistributor(
                databaseSource: databaseSource,
                allShards: allShards,
                lockFactory: lockFactory,
                setFactory: setFactory,
                baseLockId: LockIdForEveryDatabase(options),
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
    public ProjectionCoordinator(DocumentStore store, ILoggerFactory? loggerFactory = null,
        bool soloTakesNoLocks = false)
        : base(store, loggerFactory, soloTakesNoLocks)
    {
    }
}
