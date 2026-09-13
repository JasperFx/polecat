using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Subscriptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Subscriptions;

/// <summary>
///     A subscription registered in a real host reaches events through
///     <c>AddProjectionCoordinator</c> — jasperfx#827 / JasperFx 2.69.1.
/// </summary>
/// <remarks>
///     <para>
///         <b>This could not work before 2.69.1, on any store.</b>
///         <c>JasperFxSubscriptionBase</c>'s two <c>BuildExecution</c> overloads disagreed about what
///         they handed <c>SubscriptionExecution</c> as its <c>storage</c> argument: the <c>ILogger</c>
///         one passed the store, and the explicit-interface <c>ILoggerFactory</c> one passed the
///         <em>database</em>. <c>SubscriptionExecution</c> casts that argument to
///         <c>ISubscriptionRunner&lt;T&gt;</c>, which an <c>IEventDatabase</c> never implements, so
///         it threw <c>ArgumentOutOfRangeException</c> — and
///         <c>JasperFxAsyncDaemon.buildAgentForShard</c> calls the second overload.
///     </para>
///     <para>
///         <b>Why the existing subscription tests could not see it.</b> Every one of them —
///         <c>subscription_tests</c> here, and <c>SubscriptionCompliance</c> — drives the daemon
///         through <c>WaitForProjectionAsync</c> or a fixture-built daemon, and those take the
///         <em>working</em> overload. The gap is specific to the daemon a host builds for itself, so
///         closing it needs a test that registers Polecat the documented way and starts the host.
///         That is the whole point of this file: it is the shape of a real application, not of a
///         test harness.
///     </para>
///     <para>
///         It surfaced as a logged agent-start failure rather than a red test, while running
///         <c>ProjectionStatusCompliance</c> (#591) — whose coordinator host is the only one in the
///         suite set that registers a subscription <em>and</em> a coordinator.
///     </para>
/// </remarks>
public class subscription_under_coordinator_tests
{
    [Fact]
    public async Task a_subscription_receives_events_through_the_projection_coordinator()
    {
        var token = TestContext.Current.CancellationToken;

        CoordinatorRecordingSubscription.Reset();

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddLogging();

        // The documented registration, coordinator and all -- AddProjectionCoordinator rather than
        // AddAsyncDaemon, because the coordinator is the path that was broken.
        builder.Services.AddPolecat(opts =>
            {
                opts.ConnectionString = ConnectionSource.ConnectionString;
                opts.DatabaseSchemaName = "subscription_coordinator";
                opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
                opts.AutoCreateSchemaObjects = JasperFx.AutoCreate.All;
                opts.Projections.Subscribe<CoordinatorRecordingSubscription>();
            })
            .AddProjectionCoordinator(DaemonMode.Solo);

        using var host = builder.Build();

        var store = host.Services.GetRequiredService<IDocumentStore>();
        await store.Advanced.ResetAllData(token);

        await host.StartAsync(token);

        try
        {
            var streamId = Guid.NewGuid();
            await using (var session = store.LightweightSession())
            {
                session.Events.StartStream(streamId,
                    new QuestStarted("Coordinated Quest"),
                    new MembersJoined(1, "Bree", ["Strider"]));
                await session.SaveChangesAsync(token);
            }

            // The coordinator's own daemon, not one this test built -- asking the fixture for a
            // daemon here would take the overload that always worked and prove nothing.
            var coordinator = host.Services.GetRequiredService<IProjectionCoordinator>();
            await coordinator.DaemonForMainDatabase().WaitForNonStaleData(TimeSpan.FromSeconds(60));

            CoordinatorRecordingSubscription.ProcessedEvents.Count.ShouldBeGreaterThanOrEqualTo(2,
                "The subscription's agent never delivered a range. Before jasperfx#828 its agent "
                + "could not start at all under a coordinator, and the failure was logged rather "
                + "than thrown, so a test that only asserted 'no exception' would pass over it.");
        }
        finally
        {
            // StopAsync before disposal: IHost.Dispose does not stop a started host, and an
            // abandoned coordinator goes on holding shard locks against a shared SQL Server.
            await host.StopAsync(token);
        }
    }
}

/// <summary>
///     Deliberately its own type rather than a reuse of <c>RecordingSubscription</c>: that one is
///     driven by <c>subscription_tests</c> in the "integration" collection, and two classes sharing
///     one static recorder across collections race.
/// </summary>
public class CoordinatorRecordingSubscription : SubscriptionBase
{
    private static readonly List<object> _events = new();
    private static readonly object _lock = new();

    public static IReadOnlyList<object> ProcessedEvents
    {
        get
        {
            lock (_lock) return _events.ToList();
        }
    }

    public static void Reset()
    {
        lock (_lock) _events.Clear();
    }

    public override Task<IChangeListener> ProcessEventsAsync(
        EventRange page,
        ISubscriptionController controller,
        IDocumentOperations operations,
        CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            foreach (var @event in page.Events)
            {
                _events.Add(@event.Data);
            }
        }

        return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
    }
}
