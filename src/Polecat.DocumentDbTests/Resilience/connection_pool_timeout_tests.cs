using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Projections;
using Polecat.Resilience;
using Polecat.Tests.Harness;
using Polly;
using Polly.Retry;
using Polly.Telemetry;

namespace Polecat.Tests.Resilience;

/// <summary>
///     #845. SqlClient raises pool exhaustion as a bare <see cref="InvalidOperationException" />: not a
///     <see cref="DbException" />, nothing inside it, and with a message that comes out of a satellite
///     assembly. Every exception in this class that claims to be a pool timeout is a real one, produced
///     by actually running a pool dry, because the shape was the bug: the older test named
///     <c>"pool exhausted"</c> wraps a <see cref="TimeoutException" /> in a <see cref="SqlException" />,
///     which SqlClient never does here, and so passed while the real thing went unrecognized.
/// </summary>
public class connection_pool_timeout_tests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public connection_pool_timeout_tests()
    {
        // Building a store is what registers the classifier; nothing here connects
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
    }

    [Fact]
    public async Task a_real_pool_timeout_from_an_async_open_is_transient()
    {
        var timeout = await PoolTimeouts.CaptureAsync(Ct);

        shouldBeTheRealShape(timeout);
        shouldReadAsTransientEverywhere(timeout);
    }

    [Fact]
    public async Task a_real_pool_timeout_from_a_synchronous_open_is_transient()
    {
        // The two paths raise it from different places in SqlClient, so they have different stacks
        var timeout = await PoolTimeouts.CaptureAsync(Ct, synchronousOpen: true);

        shouldBeTheRealShape(timeout);
        shouldReadAsTransientEverywhere(timeout);
    }

    [Theory]
    [InlineData("de")]
    [InlineData("ja")]
    [InlineData("ru")]
    [InlineData("tr")]
    [InlineData("fr-CA")]    // no satellite of its own: resolves through its parent culture
    [InlineData("pt-PT")]    // only pt-BR ships, so this one falls back to the English resources
    [InlineData("zh-CN")]    // reaches zh-Hans through the parent chain
    public async Task a_real_pool_timeout_is_recognized_in_every_language_sqlclient_can_raise_it_in(string culture)
    {
        InvalidOperationException timeout;
        using (CultureScope.Resources(culture))
        {
            timeout = await PoolTimeouts.CaptureAsync(Ct);
        }

        shouldBeTheRealShape(timeout);
        shouldReadAsTransientEverywhere(timeout);
    }

    [Fact]
    public async Task the_language_is_not_a_property_of_the_thread_doing_the_classifying()
    {
        InvalidOperationException german;
        using (CultureScope.Resources("de"))
        {
            german = await PoolTimeouts.CaptureAsync(Ct);
        }

        // The negative control: this really is a different message than the English one, so a match
        // on the English text would miss it
        german.Message.ShouldContain("Timeout abgelaufen");

        // A shard failure is classified on whatever thread reports it, not the one that raised it. And
        // SqlClient raises an async pool timeout from its own background waiter, whose culture is that
        // of whichever open started the waiter, not necessarily this caller's.
        using (CultureScope.Resources("ja"))
        {
            TransientFailures.IsTransient(german).ShouldBeTrue();
        }
    }

    [Fact]
    public async Task a_real_pool_timeout_keeps_its_shape_through_a_polecat_session()
    {
        var connectionString = PoolTimeouts.LimitedPool(maxPoolSize: 1);
        using var store = DocumentStore.For(connectionString);

        await using var holder = new SqlConnection(connectionString);
        await holder.OpenAsync(Ct);

        await using var session = store.QuerySession();

        var started = TimeProvider.System.GetTimestamp();
        var timeout = await Should.ThrowAsync<Exception>(() => session.LoadAsync<PoolTimeoutTarget>(Guid.NewGuid(), Ct));
        var elapsed = TimeProvider.System.GetElapsedTime(started);

        shouldBeTheRealShape(timeout);
        shouldReadAsTransientEverywhere(timeout);

        // ConnectionFactory's five-try open retry only ever looks at SqlExceptions, so it does not see
        // a pool timeout: one wait for the pool (2s here), not five
        elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(6));
    }

    [Fact]
    public async Task a_pool_timeout_wrapped_by_application_code_is_still_found()
    {
        var timeout = await PoolTimeouts.CaptureAsync(Ct);

        var wrapped = new InvalidOperationException("the projection failed", timeout);

        TransientFailures.IsTransient(wrapped).ShouldBeTrue();
        ProjectionExceptions.IsExceptionTransient(wrapped).ShouldBeTrue();
        TransientFailures.IsTransient(new AggregateException(new Exception("other"), wrapped)).ShouldBeTrue();
    }

    [Fact]
    public async Task the_systemic_check_agrees_with_the_classifier()
    {
        var timeout = await PoolTimeouts.CaptureAsync(Ct);

        // jasperfx#1029 pauses a shard over a PERMANENT database failure, one the next event would hit
        // too. A pool timeout is not that: it clears by itself, and IsSystemicDatabaseFailure reads
        // "transient" from the same TransientFailures this classifier feeds, so the two cannot disagree.
        ProjectionExceptions.IsSystemicDatabaseFailure(timeout).ShouldBeFalse();
        ShardFailure.For(timeout, DateTimeOffset.UtcNow).IsTransient.ShouldBeTrue();

        // And the classifier does not change a verdict on a real permanent failure wrapped beside nothing
        ProjectionExceptions.IsSystemicDatabaseFailure(SqlExceptions.WithErrors(208)).ShouldBeTrue();
    }

    // ---- what it must not sweep in ------------------------------------------------------------------

    [Fact]
    public async Task sqlclients_other_invalid_operations_are_not_pool_timeouts()
    {
        await using var closed = new SqlConnection(ConnectionSource.ConnectionString);
        await using var command = new SqlCommand("select 1", closed);

        var real = await Should.ThrowAsync<InvalidOperationException>(() => command.ExecuteScalarAsync(Ct));

        real.GetType().ShouldBe(typeof(InvalidOperationException));
        real.Source.ShouldBe("Microsoft.Data.SqlClient");

        shouldNotBeTransient(real);
    }

    [Fact]
    public async Task polecats_own_result_set_guard_is_not_a_pool_timeout()
    {
        var schema = $"pool_timeout_{Guid.NewGuid():N}"[..24];
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        await using var session = store.LightweightSession();
        session.QueueSqlCommand("SELECT 1");
        session.Store(new PoolTimeoutTarget { Id = Guid.NewGuid() });

        var guard = await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        guard.Message.ShouldContain(nameof(IDocumentOperations.QueueSqlCommand));
        shouldNotBeTransient(guard);
    }

    [Fact]
    public void a_disposed_object_is_not_a_pool_timeout_whatever_it_says()
    {
        // ObjectDisposedException derives from InvalidOperationException, so a check written with "is"
        // would have matched on the type and left only the message standing between them. Its Message
        // adds the object name, so test the type check on a derived type that does not.
        new ObjectDisposedException("session").ShouldBeAssignableTo<InvalidOperationException>();

        shouldNotBeTransient(new ObjectDisposedException("session"));
        shouldNotBeTransient(new DerivedInvalidOperation(PoolTimeouts.EnglishMessage));
    }

    [Fact]
    public void session_transaction_unusable_is_left_to_what_is_inside_it()
    {
        // Not an InvalidOperationException, and it carries the failure that ended the transaction, so
        // the verdict belongs to that failure: a deadlock is transient, and an empty refusal is not
        shouldNotBeTransient(new SessionTransactionUnusableException(null));

        TransientFailures.IsTransient(new SessionTransactionUnusableException(SqlExceptions.WithErrors(1205)))
            .ShouldBeTrue();
    }

    [Theory]
    [InlineData("Timeout expired.")]
    [InlineData("Timeout expired.  The timeout period elapsed prior to completion of the operation or the server is not responding.")]
    [InlineData("the pool timed out")]
    public void a_message_that_merely_resembles_it_is_not_a_pool_timeout(string message)
    {
        shouldNotBeTransient(new InvalidOperationException(message));
    }

    [Fact]
    public void a_message_that_quotes_it_is_not_a_pool_timeout()
    {
        // Whole-message equality, not containment: an application exception that repeats SqlClient's
        // text in a sentence of its own is an application exception, and has an inner exception if the
        // real one was involved
        shouldNotBeTransient(new InvalidOperationException($"Could not save: {PoolTimeouts.EnglishMessage}"));
    }

    // ---- the identification itself ------------------------------------------------------------------

    [Fact]
    public void every_language_sqlclient_ships_contributes_its_message()
    {
        // 13 satellites and the neutral English resources. Fewer means the lookup broke on a SqlClient
        // bump (a renamed resource or key), which would otherwise show up only as a pool timeout on a
        // localized host being dead-lettered again.
        SqlServerTransientFailures.PoolTimeoutMessages.Count.ShouldBe(14);
        SqlServerTransientFailures.PoolTimeoutMessages.ShouldContain(PoolTimeouts.EnglishMessage);
    }

    [Fact]
    public void the_language_list_covers_every_satellite_actually_deployed()
    {
        var deployed = Directory.GetFiles(AppContext.BaseDirectory, "Microsoft.Data.SqlClient.resources.dll",
                SearchOption.AllDirectories)
            .Select(x => Path.GetFileName(Path.GetDirectoryName(x))!)
            .ToArray();

        deployed.ShouldNotBeEmpty();
        SqlServerTransientFailures.SqlClientResourceCultures.ShouldBe(deployed, ignoreOrder: true);
    }

    // ---- retries ------------------------------------------------------------------------------------

    [Theory]
    [InlineData(IsolationLevel.ReadCommitted)]
    [InlineData(IsolationLevel.Snapshot)]
    [InlineData(IsolationLevel.Serializable)]
    public async Task a_pool_timeout_is_safe_to_replay_at_every_isolation_level(IsolationLevel level)
    {
        var timeout = await PoolTimeouts.CaptureAsync(Ct);

        // Unlike 1205 or 1222 this does not fork on the level: no connection was ever obtained, so no
        // command ran and no transaction or snapshot existed for a replay to be stale against
        PolecatRetryPredicates.IsUnsafeToReplay(timeout, level).ShouldBeFalse();
    }

    [Fact]
    public async Task a_retry_composed_from_the_predicate_rides_out_a_real_pool_timeout()
    {
        var connectionString = PoolTimeouts.LimitedPool(maxPoolSize: 1);
        var holder = new SqlConnection(connectionString);
        await holder.OpenAsync(Ct);

        var retries = new List<Exception>();

        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = connectionString;
            opts.DatabaseSchemaName = "pool_timeout_retry";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            opts.ExtendPolly(builder => builder.AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 2,
                Delay = TimeSpan.FromMilliseconds(50),
                ShouldHandle = args => ValueTask.FromResult(
                    args.Outcome.Exception is not null
                    && !PolecatRetryPredicates.IsUnsafeToReplay(args.Outcome.Exception)),
                OnRetry = async args =>
                {
                    retries.Add(args.Outcome.Exception!);

                    // The pressure lifts, as it does after a load spike
                    await holder.DisposeAsync();
                }
            }));
        });

        // Reaching the database at all is the assertion, so read something that needs no schema
        await using var session = store.QuerySession();
        var answer = await session.AdvancedSql.QueryAsync<int>("select 1", Ct);

        answer.ShouldHaveSingleItem().ShouldBe(1);
        shouldBeTheRealShape(retries.ShouldHaveSingleItem());
    }

    [Fact]
    public async Task the_daemon_pipeline_builds_a_batch_again_after_a_real_pool_timeout_until_it_gives_up()
    {
        var timeout = await PoolTimeouts.CaptureAsync(Ct);
        var pipeline = new ResiliencePipelineBuilder().AddPolecatDaemonDefaults().Build();

        var attempts = 0;
        var thrown = await Should.ThrowAsync<InvalidOperationException>(async () =>
            await pipeline.ExecuteAsync(_ =>
            {
                attempts++;
                throw timeout;
            }, Ct));

        // jasperfx#1256: the first build and every retry, then the real failure goes back to the daemon to pause on
        attempts.ShouldBe(1 + PolecatResilienceDefaults.DaemonRetryAttempts);
        thrown.ShouldBeSameAs(timeout);
    }

    [Fact]
    public async Task the_daemon_pipeline_does_not_build_again_after_a_failure_that_is_not_transient()
    {
        var pipeline = new ResiliencePipelineBuilder().AddPolecatDaemonDefaults().Build();

        var attempts = 0;
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await pipeline.ExecuteAsync(_ =>
            {
                attempts++;
                throw new InvalidOperationException("Not a pool timeout");
            }, Ct));

        attempts.ShouldBe(1);
    }

    [Fact]
    public void the_store_hands_the_daemon_its_own_pipeline_and_not_the_session_one()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
        var daemonPipeline = ((IEventStore<IDocumentSession, IQuerySession>)store).ResiliencePipeline;

        // A retry here and one on the session pipeline would multiply, since a batch build runs session commands
        daemonPipeline.ShouldBeSameAs(store.Options.DaemonResiliencePipeline);
        daemonPipeline.ShouldNotBeSameAs(store.Options.ResiliencePipeline);
        daemonPipeline.ShouldNotBeSameAs(ResiliencePipeline.Empty);

        using var replaced = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.ConfigureDaemonPolly(_ => { });
        });

        ((IEventStore<IDocumentSession, IQuerySession>)replaced).ResiliencePipeline
            .ShouldBeSameAs(replaced.Options.DaemonResiliencePipeline);
    }

    private static void shouldBeTheRealShape(Exception timeout)
    {
        timeout.GetType().ShouldBe(typeof(InvalidOperationException));
        timeout.InnerException.ShouldBeNull();
        timeout.ShouldNotBeAssignableTo<DbException>();
    }

    private static void shouldReadAsTransientEverywhere(Exception timeout)
    {
        TransientFailures.IsTransient(timeout).ShouldBeTrue();
        ProjectionExceptions.IsExceptionTransient(timeout).ShouldBeTrue();
        ShardFailure.For(timeout, DateTimeOffset.UtcNow).IsTransient.ShouldBeTrue();
    }

    private static void shouldNotBeTransient(Exception exception)
    {
        SqlServerTransientFailures.Classify(exception).ShouldBeNull();
        TransientFailures.IsTransient(exception).ShouldBeFalse();
        ProjectionExceptions.IsExceptionTransient(exception).ShouldBeFalse();
    }

    private sealed class DerivedInvalidOperation(string message) : InvalidOperationException(message);
}

/// <summary>
///     #845's consequence, end to end against a running daemon and a pool that has really run out.
///     With <c>SkipApplyErrors = true</c> (the default before Polecat 6) a projection that lost that race used to have the
///     failure wrapped as <see cref="ApplyEventException" />, because nothing called it transient, and
///     the wrapper is what the daemon dead-letters: the event was skipped for good and the read model
///     silently diverged, over a load spike that had already cleared.
/// </summary>
/// <remarks>
///     jasperfx#1256: since JasperFx 3.11 the daemon builds a batch that fails transiently again through the store's
///     daemon pipeline (<see cref="StoreOptions.ConfigureDaemonPolly" />), under every error setting. So a spike that
///     clears within the retries builds the batch with nothing paused, and only one that outlasts them pauses the shard.
/// </remarks>
public class connection_pool_timeout_in_a_projection_tests : OneOffConfigurationsContext
{
    private const int MaxPoolSize = 5;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static TheoryData<string> ProjectionShapes => ["aggregation", "event projection"];

    public static TheoryData<string, string> ProjectionShapesAndErrors => new()
    {
        { "aggregation", "park" },
        { "aggregation", "skip" },
        { "event projection", "park" },
        { "event projection", "skip" }
    };

    [Theory]
    [MemberData(nameof(ProjectionShapesAndErrors))]
    public async Task a_real_pool_timeout_does_not_dead_letter_and_skip_the_event(string shape, string errors)
    {
        var connectionString = PoolTimeouts.LimitedPool(MaxPoolSize);
        var exhaustion = new PoolExhaustion(connectionString, MaxPoolSize);
        var retries = new RetryCounter();

        ConfigureStore(opts =>
        {
            opts.ConnectionString = connectionString;

            // Skip is the configuration the issue is about, which was the default before #1053 made parking the
            // default. A transient failure is built again under either.
            opts.Projections.Errors.SkipApplyErrors = errors == "skip";

            // Polecat's own daemon pipeline, watched
            opts.ConfigureDaemonPolly(builder =>
            {
                builder.TelemetryListener = retries;
                builder.AddPolecatDaemonDefaults();
            });

            addPoolStarvedProjection(opts, shape, exhaustion);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await theStore.Advanced.CleanAsync<DeadLetterEvent>(Ct);

        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new PoolPressureEvent(1), new PoolPressureEvent(2),
                new PoolPressureEvent(3));
            await session.SaveChangesAsync(Ct);
        }

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        var agent = daemon.CurrentAgents().ShouldHaveSingleItem();
        await waitForTheViewsAsync(agent, [streamId], applied: 3);

        // It really happened, and it was the real thing rather than a test double
        var witnessed = exhaustion.Witnessed.ShouldNotBeNull();
        witnessed.GetType().ShouldBe(typeof(InvalidOperationException));
        witnessed.InnerException.ShouldBeNull();

        // The batch was built again once, after the one pool timeout, and nothing paused, parked or dead-lettered
        retries.Count.ShouldBe(1);
        agent.Status.ShouldBe(AgentStatus.Running);
        agent.Failure.ShouldBeNull();
        (await deadLetterCountAsync()).ShouldBe(0);
        (await parkedAsync(agent)).ShouldBeEmpty();
    }

    [Theory]
    [MemberData(nameof(ProjectionShapes))]
    public async Task a_pool_timeout_that_outlasts_the_retries_pauses_the_shard_as_transient(string shape)
    {
        var connectionString = PoolTimeouts.LimitedPool(MaxPoolSize);
        await using var exhaustion = new SustainedPoolExhaustion(connectionString, MaxPoolSize);
        var retries = new RetryCounter();

        ConfigureStore(opts =>
        {
            opts.ConnectionString = connectionString;

            // The Polecat 6 default, parking, which a transient failure must not trigger
            opts.Projections.Errors.SkipApplyErrors.ShouldBeFalse();

            opts.ConfigureDaemonPolly(builder =>
            {
                builder.TelemetryListener = retries;
                builder.AddPolecatDaemonDefaults();
            });

            if (shape == "aggregation")
            {
                opts.Projections.Add(new PoolStarvedAggregation(exhaustion), ProjectionLifecycle.Async);
            }
            else
            {
                opts.Projections.Add(new PoolStarvedEventProjection(exhaustion), ProjectionLifecycle.Async);
            }
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await theStore.Advanced.CleanAsync<DeadLetterEvent>(Ct);

        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new PoolPressureEvent(1), new PoolPressureEvent(2),
                new PoolPressureEvent(3));
            await session.SaveChangesAsync(Ct);
        }

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        var agent = daemon.CurrentAgents().ShouldHaveSingleItem();
        var outcome = await waitForAnOutcomeAsync(agent);

        exhaustion.Witnessed.ShouldNotBeNull().GetType().ShouldBe(typeof(InvalidOperationException));

        // Every retry was spent on the dry pool, and only then did the shard pause, with a label that tells a
        // supervisor it is worth restarting. Nothing was dead-lettered or parked.
        retries.Count.ShouldBe(PolecatResilienceDefaults.DaemonRetryAttempts);
        outcome.DeadLetters.ShouldBe(0);
        agent.Status.ShouldBe(AgentStatus.Paused);
        agent.Failure.ShouldNotBeNull().IsTransient.ShouldBeTrue();

        // Which, once the pressure has gone, loses nothing: it picks up from persisted progress
        await exhaustion.ReleaseAsync();
        await daemon.StartAgentAsync(agent.Name.Identity, Ct);
        await daemon.WaitForNonStaleData(TimeSpan.FromSeconds(30));

        await using var query = observer.QuerySession();
        (await query.LoadAsync<PoolPressureView>(streamId, Ct)).ShouldNotBeNull().Applied.ShouldBe(3);

        (await deadLetterCountAsync()).ShouldBe(0);
        (await parkedAsync(agent)).ShouldBeEmpty();
    }

    [Fact]
    public async Task several_slices_timing_out_together_dead_letter_nothing_either()
    {
        // What a load spike really looks like: slices run concurrently, so more than one fails, and the
        // daemon gets an AggregateException. That is a different catch block from the single failure's,
        // and the one IsSystemicDatabaseFailure guards. Nothing in it is an ApplyEventException any more,
        // so nothing is skipped or parked, and the daemon builds the batch again through its pipeline.
        var connectionString = PoolTimeouts.LimitedPool(MaxPoolSize);
        var exhaustion = new PoolExhaustion(connectionString, MaxPoolSize, parties: 2);
        var retries = new RetryCounter();

        ConfigureStore(opts =>
        {
            opts.ConnectionString = connectionString;

            // The Polecat 6 default, parking. Before jasperfx#1256 only skip's build loop went round again here.
            opts.Projections.Errors.SkipApplyErrors.ShouldBeFalse();

            opts.ConfigureDaemonPolly(builder =>
            {
                builder.TelemetryListener = retries;
                builder.AddPolecatDaemonDefaults();
            });

            opts.Projections.Add(new PoolStarvedAggregation(exhaustion), ProjectionLifecycle.Async);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await theStore.Advanced.CleanAsync<DeadLetterEvent>(Ct);

        var streams = new[] { Guid.NewGuid(), Guid.NewGuid() };
        await using (var session = theStore.LightweightSession())
        {
            foreach (var stream in streams)
            {
                session.Events.StartStream(stream, new PoolPressureEvent(1));
            }

            await session.SaveChangesAsync(Ct);
        }

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        var agent = daemon.CurrentAgents().ShouldHaveSingleItem();
        await waitForTheViewsAsync(agent, streams, applied: 1);

        exhaustion.AllWitnessed.Length.ShouldBe(2);
        retries.Count.ShouldBe(1);
        agent.Failure.ShouldBeNull();
        (await deadLetterCountAsync()).ShouldBe(0);
        (await parkedAsync(agent)).ShouldBeEmpty();
    }

    private static void addPoolStarvedProjection(StoreOptions opts, string shape, PoolExhaustion exhaustion)
    {
        // An event projection stores the view without declaring it, and the test reads it while the daemon runs
        opts.Schema.For<PoolPressureView>();

        if (shape == "aggregation")
        {
            opts.Projections.Add(new PoolStarvedAggregation(exhaustion), ProjectionLifecycle.Async);
        }
        else
        {
            opts.Projections.Add(new PoolStarvedEventProjection(exhaustion), ProjectionLifecycle.Async);
        }
    }

    private async Task waitForTheViewsAsync(ISubscriptionAgent agent, Guid[] streams, int applied)
    {
        for (var attempt = 0; attempt < 120; attempt++)
        {
            // A dead letter or a pause is the failure this looks for, so stop as soon as one shows up
            if (agent.Status == AgentStatus.Paused || await deadLetterCountAsync() > 0) return;

            await using var query = observer.QuerySession();
            var views = await query.LoadManyAsync<PoolPressureView>(streams, Ct);
            if (views.Count == streams.Length && views.All(x => x.Applied == applied)) return;

            await Task.Delay(250, Ct);
        }

        throw new TimeoutException(
            $"The batch never got built. Status is {agent.Status}, and the failure was "
            + (agent.Failure is null ? "not reported" : agent.Failure.Detail));
    }

    private async Task<(AgentStatus Status, long DeadLetters)> waitForAnOutcomeAsync(ISubscriptionAgent agent)
    {
        for (var attempt = 0; attempt < 200; attempt++)
        {
            var deadLetters = await deadLetterCountAsync();
            if (deadLetters > 0 || agent.Status == AgentStatus.Paused)
            {
                return (agent.Status, deadLetters);
            }

            await Task.Delay(250, Ct);
        }

        throw new TimeoutException(
            $"The shard neither paused nor dead-lettered anything. Status is {agent.Status}, and the "
            + "exhausted pool was " + (agent.Failure is null ? "not reported" : agent.Failure.Detail));
    }

    private async Task<IReadOnlyList<ParkedIdentity>> parkedAsync(ISubscriptionAgent agent)
        => await ((IEventDatabase)observer.Database).FetchParkedIdentitiesAsync(
            new ParkedIdentityQuery { ProjectionName = agent.Name.Name }, Ct);

    private async Task<long> deadLetterCountAsync()
    {
        var counts = await observer.Database.FetchDeadLetterCountsAsync(CancellationToken.None);
        return counts.Sum(x => x.Count);
    }

    private DocumentStore? _observer;

    /// <summary>
    ///     The same schema through an ordinary pool. Watching what the daemon does through the pool that
    ///     has been run dry would time out too, and fail the test on its own polling.
    /// </summary>
    private DocumentStore observer
    {
        get
        {
            if (_observer is not null) return _observer;

            _observer = DocumentStore.For(opts =>
            {
                opts.ConnectionString = ConnectionSource.ConnectionString;
                opts.DatabaseSchemaName = theStore.Options.DatabaseSchemaName;
                opts.AutoCreateSchemaObjects = AutoCreate.None;
                opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            });
            Disposables.Add(_observer);

            return _observer;
        }
    }
}

public record PoolPressureEvent(int Number);

public class PoolPressureView
{
    public Guid Id { get; set; }
    public int Applied { get; set; }
}

public class PoolTimeoutTarget
{
    public Guid Id { get; set; }
}

/// <summary>
///     The aggregation shape the issue describes: <c>session.LoadAsync</c> inside <c>EvolveAsync</c>.
/// </summary>
public partial class PoolStarvedAggregation(IPoolPressure exhaustion) : SingleStreamProjection<PoolPressureView, Guid>
{
    public override async ValueTask<PoolPressureView?> EvolveAsync(PoolPressureView? snapshot, Guid id,
        IQuerySession session, IEvent e, CancellationToken cancellation)
    {
        await exhaustion.LoadOnceThePoolIsDryAsync(session, id, cancellation);

        snapshot ??= new PoolPressureView { Id = id };
        snapshot.Applied++;
        return snapshot;
    }
}

public partial class PoolStarvedEventProjection : EventProjection
{
    private readonly IPoolPressure _exhaustion;

    public PoolStarvedEventProjection(IPoolPressure exhaustion)
    {
        _exhaustion = exhaustion;
    }

    public async Task Project(IEvent<PoolPressureEvent> e, IDocumentSession ops, CancellationToken cancellation)
    {
        await _exhaustion.LoadOnceThePoolIsDryAsync(ops, e.StreamId, cancellation);

        var view = await ops.LoadAsync<PoolPressureView>(e.StreamId, cancellation)
                   ?? new PoolPressureView { Id = e.StreamId };
        view.Applied = e.Data.Number;
        ops.Store(view);
    }
}

/// <summary>
///     Runs a pool dry and lets a projection's own load find out.
/// </summary>
public interface IPoolPressure
{
    Task LoadOnceThePoolIsDryAsync(IQuerySession session, Guid id, CancellationToken token);
}

/// <summary>
///     Counts the retries of the pipeline it listens to.
/// </summary>
public sealed class RetryCounter : TelemetryListener
{
    private int _count;

    public int Count => Volatile.Read(ref _count);

    public override void Write<TResult, TArgs>(in TelemetryEventArguments<TResult, TArgs> args)
    {
        if (args.Event.EventName == "OnRetry") Interlocked.Increment(ref _count);
    }
}

/// <summary>
///     Takes every connection the pool has on the first load and holds them until <see cref="ReleaseAsync" />, so every
///     load in between fails: a load spike that outlasts the daemon's retries.
/// </summary>
public sealed class SustainedPoolExhaustion(string connectionString, int maxPoolSize) : IPoolPressure, IAsyncDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly ConcurrentQueue<Exception> _witnessed = new();
    private List<SqlConnection>? _held;
    private bool _released;

    public Exception? Witnessed => _witnessed.TryPeek(out var first) ? first : null;

    public async Task LoadOnceThePoolIsDryAsync(IQuerySession session, Guid id, CancellationToken token)
    {
        await _gate.WaitAsync(token);
        try
        {
            if (_released) return;
            _held ??= await PoolExhaustion.DrainAsync(connectionString, maxPoolSize, token);
        }
        finally
        {
            _gate.Release();
        }

        try
        {
            await session.LoadAsync<PoolPressureView>(id, token);
        }
        catch (Exception e)
        {
            _witnessed.Enqueue(e);
            throw;
        }
    }

    public async Task ReleaseAsync()
    {
        await _gate.WaitAsync();
        try
        {
            _released = true;
            foreach (var connection in _held ?? [])
            {
                await connection.DisposeAsync();
            }

            _held = null;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync() => await ReleaseAsync();
}

/// <summary>
///     Takes every connection the pool has, then lets a projection's own load find out. The first
///     <paramref name="parties" /> calls pay for it, all failing against the same dry pool; after that
///     the pressure has gone, as it does once a spike passes.
/// </summary>
public sealed class PoolExhaustion(string connectionString, int maxPoolSize, int parties = 1) : IPoolPressure
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(30);

    private readonly TaskCompletionSource _dry = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _everyoneFailed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ConcurrentQueue<Exception> _witnessed = new();
    private int _arrivals;

    public Exception[] AllWitnessed => _witnessed.ToArray();

    public Exception? Witnessed => _witnessed.TryPeek(out var first) ? first : null;

    public async Task LoadOnceThePoolIsDryAsync(IQuerySession session, Guid id, CancellationToken token)
    {
        var arrival = Interlocked.Increment(ref _arrivals);
        if (arrival > parties) return;

        var held = arrival == 1 ? await DrainAsync(connectionString, maxPoolSize, token) : [];
        if (arrival == 1) _dry.TrySetResult();
        try
        {
            await _dry.Task.WaitAsync(Patience, token);

            try
            {
                await session.LoadAsync<PoolPressureView>(id, token);
            }
            catch (Exception e)
            {
                _witnessed.Enqueue(e);
                if (_witnessed.Count == parties) _everyoneFailed.TrySetResult();

                // The pool stays dry until the last of them has failed too
                await _everyoneFailed.Task.WaitAsync(Patience, token);
                throw;
            }
        }
        finally
        {
            foreach (var connection in held)
            {
                await connection.DisposeAsync();
            }
        }
    }

    internal static async Task<List<SqlConnection>> DrainAsync(string connectionString, int maxPoolSize,
        CancellationToken token)
    {
        var held = new List<SqlConnection>();
        var started = TimeProvider.System.GetTimestamp();

        // Every connection, not "until one is refused": the daemon's polling borrows them briefly, so a
        // refusal only means they were all out at that instant, and one handed back between here and
        // the load would let the load succeed.
        while (held.Count < maxPoolSize)
        {
            if (TimeProvider.System.GetElapsedTime(started) > Patience)
            {
                throw new TimeoutException("Something is holding a connection for good, so the pool cannot be drained");
            }

            var connection = new SqlConnection(connectionString);
            try
            {
                await connection.OpenAsync(token);
                held.Add(connection);
            }
            catch (InvalidOperationException)
            {
                await connection.DisposeAsync();
            }
        }

        return held;
    }
}

internal static class PoolTimeouts
{
    public const string EnglishMessage =
        "Timeout expired.  The timeout period elapsed prior to obtaining a connection from the pool.  "
        + "This may have occurred because all pooled connections were in use and max pool size was reached.";

    /// <summary>
    ///     A pool nobody else shares. Pools are keyed by the exact connection string, so the application
    ///     name alone gives each test its own, and draining it cannot starve anything else in the run.
    /// </summary>
    public static string LimitedPool(int maxPoolSize) =>
        new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            MaxPoolSize = maxPoolSize,

            // How long a caller waits for the pool. Long enough that a real login cannot trip it on a
            // loaded machine and be mistaken for the failure under test.
            ConnectTimeout = 2,
            ApplicationName = $"polecat-845-{Guid.NewGuid():N}"
        }.ConnectionString;

    /// <summary>
    ///     Exhaust a <c>Max Pool Size=1</c> pool and hand back exactly what SqlClient raises.
    /// </summary>
    /// <remarks>
    ///     #889. SqlClient's pruning timer, every 30s once the process is four minutes old, retires any
    ///     pool with a count of zero, and a new pool counts nothing until its first login has finished.
    ///     A tick during the holder's login leaves it on a retired pool and the second open quietly
    ///     builds another, which says nothing about exhausted pools, so that setup is thrown away.
    ///     Failing to throw on every attempt still fails: the timer cannot land on all of them.
    /// </remarks>
    public static async Task<InvalidOperationException> CaptureAsync(CancellationToken token,
        bool synchronousOpen = false)
    {
        const int setupAttempts = 3;

        for (var attempt = 1; ; attempt++)
        {
            var connectionString = LimitedPool(maxPoolSize: 1);

            await using var holder = new SqlConnection(connectionString);
            await holder.OpenAsync(token);

            await using var second = new SqlConnection(connectionString);

            var opened = false;

            try
            {
                return await Should.ThrowAsync<InvalidOperationException>(async () =>
                {
                    if (synchronousOpen)
                    {
                        second.Open();
                    }
                    else
                    {
                        await second.OpenAsync(token);
                    }

                    opened = true;
                });
            }
            catch (ShouldAssertException) when (opened && attempt < setupAttempts)
            {
                // Round again on a new pool
            }
        }
    }
}
