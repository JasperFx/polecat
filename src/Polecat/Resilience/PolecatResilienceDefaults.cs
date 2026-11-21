using Polly;

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
}
