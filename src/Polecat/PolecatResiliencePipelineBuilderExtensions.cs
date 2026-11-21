using Polecat.Resilience;
using Polly;
using Polly.Retry;

namespace Polecat;

/// <summary>
///     Polecat's default Polly policies that you can build on in your own pipelines.
/// </summary>
public static class PolecatResiliencePipelineBuilderExtensions
{
    /// <summary>
    ///     How many times <see cref="AddPolecatDaemonDefaults" /> builds a batch again before the shard pauses
    ///     (jasperfx#1256).
    /// </summary>
    internal const int DaemonRetryAttempts = 3;

    /// <summary>
    ///     The first wait before a batch is built again. It doubles on each attempt, with jitter.
    /// </summary>
    internal static readonly TimeSpan DaemonRetryDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     Add Polecat's default retry policy for the async daemon building a projection batch, the pipeline
    ///     <see cref="StoreOptions.ConfigureDaemonPolly" /> replaces. Use it there to start from the default and add
    ///     to it.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         It retries a failure that is expected to clear on its own: a SQL Server error on the transient list
    ///         (a deadlock, a lock timeout, a command timeout, a dropped connection, an Azure SQL throttle or
    ///         failover), SqlClient's connection pool timeout, a <see cref="TimeoutException" /> or a dropped
    ///         socket. It does not retry any other failure, so a missing table or column fails on the first
    ///         attempt. It retries up to 3 times, so a batch is built at most 4 times, waiting 100ms before the first
    ///         retry and doubling, with jitter. When the retries run out, the shard pauses with
    ///         <c>ShardFailure.IsTransient</c> true, so a supervisor can restart it later.
    ///     </para>
    ///     <para>
    ///         Building a batch again is safe in a way that replaying a session command is not: nothing has been
    ///         written when the build fails, and the batch is sliced again from the same events. Every command in
    ///         the build already runs through the store's own pipeline (<see cref="StoreOptions.ConfigurePolly" />),
    ///         so the count here stays small to keep the two from multiplying into a long stall (marten#5587).
    ///     </para>
    /// </remarks>
    public static ResiliencePipelineBuilder AddPolecatDaemonDefaults(this ResiliencePipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // ShouldHandle is SqlServerTransientFailures.IsWorthBuildingAgain, which covers SqlClient's pool timeout. That
        // arrives as a bare InvalidOperationException, so nothing keyed on SqlException would see it.
        return builder.AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = DaemonRetryAttempts,
            Delay = DaemonRetryDelay,
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
            ShouldHandle = static args => ValueTask.FromResult(
                args.Outcome.Exception is { } exception && SqlServerTransientFailures.IsWorthBuildingAgain(exception))
        });
    }
}
