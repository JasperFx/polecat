using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Polecat.Events.Daemon;

/// <summary>
///     #1105 (marten#6122, fisher#653): an ASP.NET Core health check over the parked identities of a store's async
///     projections (#1053). It reports <see cref="HealthStatus.Degraded" /> while any identity is parked, and
///     <see cref="HealthStatus.Unhealthy" /> while a shard is paused because it reached the parking cap
///     (<see cref="ShardFailureCategory.ParkedLimitReached" />).
/// </summary>
/// <remarks>
///     <para>
///         A parked identity's document is stuck at its last good state until someone replays or discards it, but the
///         rest of the projection keeps going, so that is Degraded. A shard that stopped parking and paused instead is
///         not projecting anything, so that is Unhealthy.
///     </para>
///     <para>
///         The parked counts come from <c>pc_parked_identities</c> in every database of the store, so any node can
///         run the check. The paused shards come from two places: the daemons this process runs, through its
///         projection coordinator, and, with <c>Events.EnableExtendedProgressionTracking</c> on, the
///         <c>failure_category</c> column of <c>pc_event_progression</c>, which a node that doesn't run the shard can
///         read too.
///     </para>
/// </remarks>
public static class ParkedIdentitiesHealthCheckExtensions
{
    private static readonly string[] Tags = ["Polecat", "AsyncDaemon", "Parked"];

    /// <summary>
    ///     Adds the parked identities health check for the store registered with <c>AddPolecat()</c>.
    /// </summary>
    public static IHealthChecksBuilder AddPolecatParkedIdentitiesHealthCheck(this IHealthChecksBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.Add(new HealthCheckRegistration(nameof(ParkedIdentitiesHealthCheck),
            services => new ParkedIdentitiesHealthCheck(services.GetRequiredService<IDocumentStore>(),
                services.GetService<JasperFx.Events.Daemon.IProjectionCoordinator>()),
            failureStatus: null,
            tags: Tags));
    }

    /// <summary>
    ///     Adds the parked identities health check for a store registered with <c>AddPolecatStore&lt;TStore&gt;()</c>.
    ///     Call it once per store. Each store's check is registered as <c>ParkedIdentitiesHealthCheck:&lt;store type&gt;</c>.
    /// </summary>
    /// <typeparam name="TStore">The store's marker interface, as passed to <c>AddPolecatStore&lt;TStore&gt;()</c>.</typeparam>
    public static IHealthChecksBuilder AddPolecatParkedIdentitiesHealthCheck<TStore>(this IHealthChecksBuilder builder)
        where TStore : class, IDocumentStore
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.Add(new HealthCheckRegistration(ParkedIdentitiesHealthCheckNameFor<TStore>(),
            services => new ParkedIdentitiesHealthCheck(services.GetRequiredService<TStore>(),
                services.GetService<Polecat.Events.Daemon.Coordination.IProjectionCoordinator<TStore>>()),
            failureStatus: null,
            tags: [..Tags, typeof(TStore).Name]));
    }

    /// <summary>
    ///     The name a store's parked identities check is registered under, so a caller filtering health check results
    ///     can find it.
    /// </summary>
    public static string ParkedIdentitiesHealthCheckNameFor<TStore>() where TStore : class, IDocumentStore =>
        $"{nameof(ParkedIdentitiesHealthCheck)}:{typeof(TStore).Name}";

    /// <summary>
    ///     Degraded while any identity is parked, in any of the store's databases. Unhealthy while a shard is paused at
    ///     the parking cap.
    /// </summary>
    public sealed class ParkedIdentitiesHealthCheck: IHealthCheck
    {
        private readonly IDocumentStore _store;
        private readonly JasperFx.Events.Daemon.IProjectionCoordinator? _coordinator;

        /// <summary>
        ///     Construct the check over a store, and the projection coordinator of this process when it runs the
        ///     store's daemon. Without one, only the persisted progression can say a shard is paused at the cap.
        /// </summary>
        public ParkedIdentitiesHealthCheck(IDocumentStore store,
            JasperFx.Events.Daemon.IProjectionCoordinator? coordinator = null)
        {
            _store = store;
            _coordinator = coordinator;
        }

        /// <inheritdoc />
        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var databases = _store is IEventStore eventStore
                    ? await eventStore.AllDatabases().ConfigureAwait(false)
                    : Array.Empty<IEventDatabase>();

                var capped = await pausedAtTheCapAsync(databases, cancellationToken).ConfigureAwait(false);

                var parked = new List<string>();
                var total = 0;
                foreach (var database in databases)
                {
                    foreach (var count in await database.FetchParkedCountsAsync(null, cancellationToken)
                                 .ConfigureAwait(false))
                    {
                        total += count.Count;
                        parked.Add($"{count.ProjectionName} (tenant {count.TenantId}): {count.Count}");
                    }
                }

                var parkedText = total == 0
                    ? "No projection identities are parked."
                    : $"{total} projection {(total == 1 ? "identity is" : "identities are")} parked: {string.Join(", ", parked)}";

                if (capped.Count > 0)
                {
                    return HealthCheckResult.Unhealthy(
                        $"{capped.Count} projection {(capped.Count == 1 ? "shard is" : "shards are")} paused at the parking cap: {string.Join(", ", capped)}. Replay or discard parked identities, or fix the projection, then restart the shard. {parkedText}");
                }

                return total == 0
                    ? HealthCheckResult.Healthy(parkedText)
                    : HealthCheckResult.Degraded(parkedText);
            }
            catch (Exception e)
            {
                return HealthCheckResult.Unhealthy($"Unhealthy: {e.Message}", e);
            }
        }

        private async Task<List<string>> pausedAtTheCapAsync(IReadOnlyList<IEventDatabase> databases,
            CancellationToken token)
        {
            var capped = new SortedSet<string>(StringComparer.Ordinal);

            // The daemons this process runs. AllDaemonsAsync and never DaemonForDatabase: a health check must not
            // start a daemon by asking (see DocumentStore.shardStatesAsync).
            if (_coordinator != null)
            {
                foreach (var daemon in await _coordinator.AllDaemonsAsync().ConfigureAwait(false))
                {
                    var database = daemon is PolecatProjectionDaemon polecat ? polecat.Database.Identifier : null;
                    foreach (var agent in daemon.CurrentAgents())
                    {
                        if (agent.Status == AgentStatus.Paused &&
                            agent.Failure?.Category == ShardFailureCategory.ParkedLimitReached)
                        {
                            capped.Add(describe(agent.Name.Identity, database, databases.Count));
                        }
                    }
                }
            }

            // What every node wrote, when the progression table carries the failure columns. A Started state clears
            // them, so a row still saying ParkedLimitReached is a shard that has not run since.
            if (_store.Options.Events.EnableExtendedProgressionTracking)
            {
                foreach (var database in databases)
                {
                    foreach (var state in await database.AllProjectionProgress(token).ConfigureAwait(false))
                    {
                        if (state.Failure?.Category == ShardFailureCategory.ParkedLimitReached)
                        {
                            capped.Add(describe(state.ShardName, database.Identifier, databases.Count));
                        }
                    }
                }
            }

            return capped.ToList();
        }

        private static string describe(string shard, string? database, int databaseCount) =>
            database == null || databaseCount <= 1 ? shard : $"{shard} (database {database})";
    }
}
