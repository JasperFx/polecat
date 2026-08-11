using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Polecat.Storage;

namespace Polecat.Events.Daemon;

/// <summary>
///     Health check that detects a stalled or dead high-water agent — the failure mode in
///     marten#4961 where the async daemon's high-water agent dies, the mark freezes, projections
///     catch up to the frozen value, and any lag-based check reports Healthy. This check instead
///     detects that the high-water agent has stopped and, optionally, restarts it.
///     <para>
///         This is the Polecat parity implementation of Marten's <c>AddMartenHighWaterHealthCheck</c>
///         (marten#4982 / polecat#339, revised by marten#5181 / polecat#434).
///     </para>
///     <para>
///         <b>The signal is the sequence gap: the mark sitting unchanged while later events pile up
///         past it.</b> That is the only honest persisted signal Polecat has, and the check no longer
///         pretends otherwise.
///     </para>
///     <para>
///         It used to treat the ExtendedProgression <c>heartbeat</c> column on the
///         <c>HighWaterMark</c> row as its <em>primary</em> signal, falling back to the gap heuristic
///         only when no heartbeat was present. Nothing ever wrote that column for that row:
///         <c>JasperFx.Events.Daemon.ExtendedProgressionWriter.OnNext</c> returns early for
///         <c>ShardState.HighWaterMark</c> states outright, and Polecat's own high-water persist
///         (<c>PolecatHighWaterDetector.MarkHighWaterAsync</c>) writes only <c>last_seq_id</c> and
///         <c>last_updated</c>. So the primary branch was unreachable in every real deployment and the
///         check silently degraded to the gap heuristic — while its own tests passed against a state no
///         daemon ever produces, because they seeded <c>heartbeat</c> with raw SQL. A check that
///         silently degrades is worse than an honest heuristic, which is the whole point of
///         marten#4961.
///     </para>
///     <para>
///         Marten's replacement reads the <c>last_updated</c> age of the per-tenant
///         <c>HighWaterMark:{tenant}</c> rows, which its vectorized poll re-stamps on every cycle
///         whether or not the mark advances. <b>Polecat has no equivalent</b>: it never overrides
///         <c>IHighWaterDetector.MarkHighWaterForTenantAsync</c>, so no per-tenant high-water row is
///         ever written here, and on the store-global row <c>last_updated</c> moves only when the mark
///         actually advances (<c>Detect</c> calls <c>MarkHighWaterAsync</c> only under
///         <c>stats.HasChanged</c>) — so its age says nothing about liveness on a quiet store.
///         Rather than add a second unreachable branch, the gap heuristic stands alone. For a
///         liveness signal that does not depend on the mark advancing, use the in-process surface the
///         daemon already exposes: <c>IProjectionDaemon.HighWaterLastPolledAt</c> and
///         <c>IsHighWaterStale</c> (jasperfx#539).
///     </para>
/// </summary>
public static class HighWaterHealthCheckExtensions
{
    /// <summary>
    ///     Adds a health check that reports <see cref="HealthCheckResult.Unhealthy" /> when the
    ///     store-global high-water mark has sat unchanged, with later events piling up past it, for at
    ///     least <paramref name="staleThreshold" /> (marten#4961 / polecat#341, revised by
    ///     polecat#434).
    /// </summary>
    /// <param name="builder"><see cref="IHealthChecksBuilder" /></param>
    /// <param name="staleThreshold">
    ///     How long the mark may sit unchanged while behind the latest event sequence before the store
    ///     is considered unhealthy. Defaults to 30 seconds.
    /// </param>
    /// <param name="minimumGap">
    ///     The gap (highest event sequence minus high-water mark) that is treated as "caught up" and
    ///     never trips the check, absorbing the normal safe-harbor lag. Defaults to 1.
    /// </param>
    /// <param name="autoRestart">
    ///     When <c>true</c>, an Unhealthy result also asks the local projection coordinator to
    ///     restart the high-water agent's poll loop for the affected database
    ///     (<see cref="IProjectionDaemon.RestartHighWaterAgentAsync" />). The restart never advances
    ///     the mark and is capped to once per <paramref name="staleThreshold" /> window per database
    ///     to avoid churn; the cycle is still reported <b>Unhealthy</b> so an alert fires. Defaults
    ///     to <c>false</c> (detection only). Intended for single-writer (Solo) or leader nodes —
    ///     the process running the health check must be the one hosting the daemon.
    /// </param>
    public static IHealthChecksBuilder AddPolecatHighWaterHealthCheck(
        this IHealthChecksBuilder builder,
        TimeSpan? staleThreshold = null,
        long minimumGap = 1,
        bool autoRestart = false
    )
    {
        builder.Services.AddSingleton(new HighWaterHealthCheckSettings(
            staleThreshold ?? TimeSpan.FromSeconds(30), minimumGap, autoRestart));
        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<HighWaterStateTracker>();
        return builder.AddCheck<HighWaterHealthCheck>(
            nameof(HighWaterHealthCheck),
            tags: new[] { "Polecat", "AsyncDaemon", "HighWater" }
        );
    }

    /// <summary>
    ///     DI-injected settings for <see cref="HighWaterHealthCheck" />.
    /// </summary>
    public record HighWaterHealthCheckSettings(TimeSpan StaleThreshold, long MinimumGap, bool AutoRestart = false);

    /// <summary>
    ///     Tracks, per database, the gap heuristic's "first observed a stuck mark" reading
    ///     and (when <c>autoRestart</c> is on) the last auto-restart moment, so a <em>sustained</em>
    ///     non-advance can be distinguished from a transient safe-harbor gap and restarts can be
    ///     capped to once per staleness window across health check invocations.
    /// </summary>
    public class HighWaterStateTracker
    {
        public ConcurrentDictionary<string, (DateTimeOffset FirstObservedAt, long HighWaterMark)> Readings { get; } =
            new();

        public ConcurrentDictionary<string, DateTimeOffset> Restarts { get; } = new();
    }

    /// <summary>
    ///     Health check implementation.
    /// </summary>
    public class HighWaterHealthCheck: IHealthCheck
    {
        private const string HighWaterMarkShard = "HighWaterMark";

        private readonly IDocumentStore _store;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan _staleThreshold;
        private readonly long _minimumGap;
        private readonly bool _autoRestart;
        private readonly HighWaterStateTracker _tracker;
        private readonly IServiceProvider _services;

        public HighWaterHealthCheck(IDocumentStore store, HighWaterHealthCheckSettings settings,
            TimeProvider timeProvider, HighWaterStateTracker tracker, IServiceProvider services)
        {
            _store = store;
            _timeProvider = timeProvider;
            _staleThreshold = settings.StaleThreshold;
            _minimumGap = settings.MinimumGap;
            _autoRestart = settings.AutoRestart;
            _tracker = tracker;
            _services = services;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context,
            CancellationToken cancellationToken = default
        )
        {
            try
            {
                var options = _store.Options;

                // Gate: the high-water mark is only expected to advance when this store is actually
                // responsible for running the async daemon. Otherwise a frozen mark is legitimate
                // and asserting on it would be a false positive.

                // No async projections or subscriptions -> no high-water agent runs anywhere.
                if (!options.Projections.HasAnyAsyncProjections())
                {
                    return HealthCheckResult.Healthy("No async projections or subscriptions are registered");
                }

                // Disabled / ExternallyManaged -> this store hosts no daemon, so it must not assert
                // that some local agent is advancing the mark.
                var asyncMode = options.DaemonSettings.AsyncMode;
                if (asyncMode is not (DaemonMode.Solo or DaemonMode.HotCold))
                {
                    return HealthCheckResult.Healthy(
                        $"Async daemon mode is {asyncMode}; high-water is not advanced by this store");
                }

                // Store-agnostic database enumeration (no Marten-style store.Storage in Polecat).
                var databases = _store is IEventStore eventStore
                    ? await eventStore.AllDatabases().ConfigureAwait(false)
                    : Array.Empty<IEventDatabase>();

                foreach (var database in databases)
                {
                    var result = await checkDatabaseAsync(database, cancellationToken).ConfigureAwait(false);
                    if (result.Status != HealthStatus.Healthy)
                    {
                        return result;
                    }
                }

                return HealthCheckResult.Healthy("Healthy");
            }
            catch (Exception ex)
            {
                return HealthCheckResult.Unhealthy($"Unhealthy: {ex.Message}", ex);
            }
        }

        private async Task<HealthCheckResult> checkDatabaseAsync(IEventDatabase database, CancellationToken token)
        {
            // polecat#434: read the ONE row this check is about rather than pulling every projection x
            // tenant row on every probe through AllProjectionProgress, which is what the old
            // heartbeat-first shape needed and which grows with the fleet.
            var highWater = await readHighWaterRowAsync(database, token).ConfigureAwait(false);

            // No HighWaterMark progression row yet -> the daemon has not started here. Nothing to assert.
            if (highWater is null)
            {
                clearTracking(database.Identifier);
                return HealthCheckResult.Healthy("Healthy");
            }

            var now = _timeProvider.GetUtcNow();

            // The sequence-gap heuristic, and deliberately the only signal — see the class remarks for
            // why the ExtendedProgression `heartbeat` column is not consulted (nothing writes it for
            // this row) and why `last_updated` is not either (it moves only when the mark advances, so
            // its age says nothing on a quiet store). A non-zero gap is normal transiently: the detector
            // holds the mark inside a "safe harbor" behind in-flight/gapped sequences, so this trips
            // only on a sustained non-advance while events pile up past the mark.
            var highest = await database.FetchHighestEventSequenceNumber(token).ConfigureAwait(false);
            var gap = highest - highWater.Sequence;

            // Caught up (within the normal safe-harbor gap). Clear any stalled-mark tracking.
            if (gap <= _minimumGap)
            {
                clearTracking(database.Identifier);
                return HealthCheckResult.Healthy("Healthy");
            }

            // Track the first time we saw a gap at this mark value; if the mark moves, reset the clock; if
            // it stays put past the threshold, the high-water agent has almost certainly died or wedged.
            var reading = _tracker.Readings.GetOrAdd(database.Identifier, _ => (now, highWater.Sequence));

            if (reading.HighWaterMark != highWater.Sequence)
            {
                _tracker.Readings[database.Identifier] = (now, highWater.Sequence);
                _tracker.Restarts.TryRemove(database.Identifier, out _);
                return HealthCheckResult.Healthy("Healthy");
            }

            if (now - reading.FirstObservedAt >= _staleThreshold)
            {
                var restartNote = await tryAutoRestartAsync(database.Identifier, now, token).ConfigureAwait(false);
                return HealthCheckResult.Unhealthy(
                    $"Unhealthy: the high-water mark for database '{database.Identifier}' has been stuck at {highWater.Sequence} with {gap} later event(s) unprocessed (highest sequence {highest}) for at least {_staleThreshold}. The high-water agent may have stopped (see marten#4961).{restartNote}");
            }

            return HealthCheckResult.Healthy("Healthy");
        }

        /// <summary>
        ///     polecat#434: read just the store-global <c>HighWaterMark</c> progression row. This used to
        ///     go through <see cref="IEventDatabase.AllProjectionProgress(CancellationToken)" /> and filter
        ///     in memory, which pulled every projection x tenant row on every probe to keep one of them —
        ///     on a thousand-tenant fleet that is thousands of rows per health probe. Mirrors marten#5181.
        /// </summary>
        private async Task<HighWaterRow?> readHighWaterRowAsync(IEventDatabase database, CancellationToken token)
        {
            await database.EnsureStorageExistsAsync(typeof(IEvent), token).ConfigureAwait(false);

            var connectionString = database is PolecatDatabase polecat
                ? polecat.ConnectionString
                : _store.Options.ConnectionString;

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(token).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            // HighWaterMarkShard is a compile-time constant, so no pattern grammar reaches this from
            // user input; it is still bound rather than interpolated.
            cmd.CommandText =
                $"SELECT last_seq_id FROM {_store.Options.EventGraph.ProgressionTableName} WHERE name = @name;";
            var parameter = cmd.Parameters.Add("@name", System.Data.SqlDbType.VarChar, 200);
            parameter.Value = HighWaterMarkShard;

            await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
            if (!await reader.ReadAsync(token).ConfigureAwait(false))
            {
                return null;
            }

            return new HighWaterRow(await reader.GetFieldValueAsync<long>(0, token).ConfigureAwait(false));
        }

        private sealed record HighWaterRow(long Sequence);

        private void clearTracking(string databaseIdentifier)
        {
            _tracker.Readings.TryRemove(databaseIdentifier, out _);
            _tracker.Restarts.TryRemove(databaseIdentifier, out _);
        }

        // polecat#341 item 1: opt-in remediation. Ask the local coordinator's daemon to restart the
        // high-water poll loop for this database — loop only, never advancing the mark. Best-effort and
        // capped to once per staleness window so the (faster) health-check cadence can't thrash a loop
        // that legitimately needs longer to re-establish. The cycle is still reported Unhealthy by the
        // caller so an alert fires regardless.
        private async Task<string> tryAutoRestartAsync(string databaseIdentifier, DateTimeOffset now,
            CancellationToken token)
        {
            if (!_autoRestart)
            {
                return string.Empty;
            }

            if (_tracker.Restarts.TryGetValue(databaseIdentifier, out var lastRestart) &&
                now - lastRestart < _staleThreshold)
            {
                return " An auto-restart was already attempted within the current staleness window.";
            }

            var coordinator = _services.GetService<IProjectionCoordinator>();
            if (coordinator is null)
            {
                return " (autoRestart is enabled but no IProjectionCoordinator is registered to restart the agent.)";
            }

            try
            {
                var daemon = await coordinator.DaemonForDatabase(databaseIdentifier).ConfigureAwait(false);
                await daemon.RestartHighWaterAgentAsync(token).ConfigureAwait(false);
                _tracker.Restarts[databaseIdentifier] = now;
                return " An auto-restart of the high-water agent was triggered (the mark was NOT advanced).";
            }
            catch (Exception e)
            {
                return $" An auto-restart of the high-water agent was attempted but failed: {e.Message}.";
            }
        }
    }
}
