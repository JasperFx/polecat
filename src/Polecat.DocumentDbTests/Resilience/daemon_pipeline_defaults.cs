using Microsoft.Data.SqlClient;
using Polly;
using Polly.Retry;

namespace Polecat.Tests.Resilience;

/// <summary>
///     #1111 (marten#6129): the daemon's default batch pipeline is public as
///     <see cref="PolecatResiliencePipelineBuilderExtensions.AddPolecatDaemonDefaults" />, so a
///     <see cref="StoreOptions.ConfigureDaemonPolly" /> can start from it and add to it. These run the pipeline
///     directly, without a daemon. <see cref="connection_pool_timeout_tests" /> covers the daemon using it.
/// </summary>
public class daemon_pipeline_defaults
{
    // A deadlock victim, which SQL Server expects to clear on its own
    private static Exception deadlock() => SqlExceptions.WithErrors(1205);

    private static async Task<(int Attempts, Exception Thrown)> runAsync(ResiliencePipeline pipeline,
        Func<Exception> failure)
    {
        var attempts = 0;
        var thrown = await Should.ThrowAsync<Exception>(async () =>
            await pipeline.ExecuteAsync(_ =>
            {
                attempts++;
                throw failure();
            }, TestContext.Current.CancellationToken));

        return (attempts, thrown);
    }

    [Fact]
    public void the_default_is_public_api()
    {
        typeof(PolecatResiliencePipelineBuilderExtensions).IsPublic.ShouldBeTrue();
    }

    [Fact]
    public async Task the_default_retries_a_transient_failure_3_times_then_gives_it_back()
    {
        var pipeline = new ResiliencePipelineBuilder().AddPolecatDaemonDefaults().Build();

        var (attempts, thrown) = await runAsync(pipeline, deadlock);

        // The first build, then the 3 retries the XML docs and retries.md promise
        attempts.ShouldBe(4);
        PolecatResiliencePipelineBuilderExtensions.DaemonRetryAttempts.ShouldBe(3);
        thrown.ShouldBeOfType<SqlException>().Number.ShouldBe(1205);
    }

    [Fact]
    public async Task the_default_does_not_retry_a_failure_that_is_not_transient()
    {
        var pipeline = new ResiliencePipelineBuilder().AddPolecatDaemonDefaults().Build();

        // Invalid object name: a missing table fails the same way every time
        var (attempts, thrown) = await runAsync(pipeline, () => SqlExceptions.WithErrors(208));

        attempts.ShouldBe(1);
        thrown.ShouldBeOfType<SqlException>().Number.ShouldBe(208);
    }

    [Fact]
    public async Task configure_daemon_polly_can_start_from_the_default_and_add_to_it()
    {
        var opts = new StoreOptions();

        #region sample_configure_daemon_polly_from_the_default

        // Keep Polecat's default retries, and when they run out, wait 5 seconds
        // and run them once more before the shard pauses
        opts.ConfigureDaemonPolly(builder => builder
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 1,
                Delay = TimeSpan.FromSeconds(5)
            })
            .AddPolecatDaemonDefaults());

        #endregion

        var (attempts, thrown) = await runAsync(opts.DaemonResiliencePipeline, deadlock);

        // Two rounds of Polecat's first build plus 3 retries
        attempts.ShouldBe(8);
        thrown.ShouldBeOfType<SqlException>().Number.ShouldBe(1205);
    }
}
