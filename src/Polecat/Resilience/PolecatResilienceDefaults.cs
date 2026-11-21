using Polly;
using Polly.Retry;

namespace Polecat.Resilience;

internal static class PolecatResilienceDefaults
{
    /// <summary>
    ///     Polecat's default resilience pipeline, which adds <b>no strategy at all</b>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>The emptiness is the feature, and #724 is why it is written down.</b> The
    ///         pipeline wraps a unit of work that has <i>already been computed</i> — the operations a
    ///         session accumulated from application code that ran earlier, against a snapshot that
    ///         has since been invalidated. A retry here replays those operations; it does not re-run
    ///         the code that produced them.
    ///     </para>
    ///     <para>
    ///         marten#5528 is what that costs. A <c>Serializable</c> session raised PostgreSQL
    ///         <c>40001</c>, Marten's write-retry classifier treated it as safe, and the replay opened
    ///         a <i>fresh</i> snapshot in which the conflict no longer existed — so the second write
    ///         landed over the first and a committed write was silently lost. SQL Server's equivalent
    ///         is <b>3960</b>, and the perverse part generalizes: a snapshot update conflict means
    ///         precisely that the snapshot those operations were derived from is no longer a valid
    ///         basis for them, so the one error class where the rollback is most certain is the one
    ///         where replaying is least safe.
    ///     </para>
    ///     <para>
    ///         Both halves of marten#5528's precondition are reachable here —
    ///         <c>SessionOptions.IsolationLevel</c> is public and honours <c>Snapshot</c> /
    ///         <c>Serializable</c>, and <see cref="StoreOptions.ConfigurePolly" /> /
    ///         <see cref="StoreOptions.ExtendPolly" /> invite exactly the retry that would do it. So
    ///         anyone adding a default strategy here has to answer <see cref="PolecatRetryPredicates" />
    ///         first. <c>the_default_pipeline_does_not_replay_a_failed_unit_of_work</c> is what stops
    ///         that happening by accident.
    ///     </para>
    /// </remarks>
    public static ResiliencePipelineBuilder AddPolecatDefaults(this ResiliencePipelineBuilder builder)
    {
        return builder;
    }

    /// <summary>
    ///     How many times the async daemon builds a projection batch again after a transient failure before the
    ///     shard pauses (jasperfx#1256).
    /// </summary>
    internal const int DaemonRetryAttempts = 3;

    /// <summary>
    ///     The first wait before a batch is built again. It doubles on each attempt, with jitter.
    /// </summary>
    internal static readonly TimeSpan DaemonRetryDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     The default of <see cref="StoreOptions.ConfigureDaemonPolly" />: the pipeline the async daemon builds
    ///     each projection batch through (jasperfx#1256). A batch whose build fails transiently, like a connection
    ///     pool timeout under a load spike, a deadlock or a dropped connection, is built again up to
    ///     <see cref="DaemonRetryAttempts" /> times with a short exponential backoff. When that runs out the shard
    ///     pauses with <c>ShardFailure.IsTransient</c> true, so a supervisor can restart it later.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>Why this is its own pipeline and not <see cref="AddPolecatDefaults" />.</b> Building a batch
    ///         runs session commands (loading snapshots, the projection's own loads) through
    ///         <see cref="StoreOptions.ResiliencePipeline" />. A retry on that pipeline would run inside this one,
    ///         so the two would multiply (marten#5587), and it would also replay one command of a unit of work, which
    ///         #724 rules out. A batch build is different: nothing has been written when it fails, so building it
    ///         again from the same events is safe. JasperFx re-slices the page first and only hands this pipeline a
    ///         failure it already judged transient.
    ///     </para>
    ///     <para>
    ///         <c>ShouldHandle</c> is <see cref="SqlServerTransientFailures.IsWorthBuildingAgain" />, which covers
    ///         SqlClient's pool timeout. That arrives as a bare <see cref="InvalidOperationException" />, so nothing
    ///         keyed on <see cref="Microsoft.Data.SqlClient.SqlException" /> would see it.
    ///     </para>
    /// </remarks>
    public static ResiliencePipelineBuilder AddPolecatDaemonDefaults(this ResiliencePipelineBuilder builder)
    {
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
