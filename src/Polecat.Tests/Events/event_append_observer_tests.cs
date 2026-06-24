using JasperFx;
using JasperFx.Events;
using Microsoft.Extensions.DependencyInjection;
using Polecat.Internal;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

// Marker for the ancillary-store coverage.
public interface IObserverAncillaryStore : IDocumentStore;

/// <summary>
///     Covers polecat#213 — runtime event-append observation. The built-in
///     <see cref="EventAppendObservationListener" /> forwards the events appended in each unit of work
///     to <see cref="IEventStoreInstrumentation.AppendObserver" />, and is auto-registered on every
///     store (primary + ancillary) when <c>JasperFxOptions.EnableAdvancedTracking</c> is on.
/// </summary>
public class event_append_observer_tests
{
    [Fact]
    public void advanced_tracking_registers_the_append_observation_listener_idempotently()
    {
        var options = new StoreOptions();
        options.Listeners.OfType<EventAppendObservationListener>().ShouldBeEmpty();

        options.ReadJasperFxOptions(new JasperFxOptions { EnableAdvancedTracking = true });
        options.Listeners.OfType<EventAppendObservationListener>().Count().ShouldBe(1);

        // A second read (both registration paths call it) must not double-register.
        options.ReadJasperFxOptions(new JasperFxOptions { EnableAdvancedTracking = true });
        options.Listeners.OfType<EventAppendObservationListener>().Count().ShouldBe(1);
    }

    [Fact]
    public void no_listener_registered_when_advanced_tracking_off()
    {
        var options = new StoreOptions();
        options.ReadJasperFxOptions(new JasperFxOptions { EnableAdvancedTracking = false });
        options.Listeners.OfType<EventAppendObservationListener>().ShouldBeEmpty();
    }

    [Fact]
    public async Task observer_is_invoked_on_the_main_store_when_advanced_tracking_enabled()
    {
        var observed = new List<IReadOnlyList<IEvent>>();
        await using var provider = BuildProvider(advancedTracking: true, services =>
            services.AddPolecat(opts =>
            {
                ConfigureStore(opts, "append_observer_main");
                opts.Events.AppendObserver = events => observed.Add(events);
            }));

        var store = await MigrateAsync(provider.GetRequiredService<IDocumentStore>());
        var streamId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new QuestStarted("Observed"));
            await session.SaveChangesAsync();
        }

        observed.Count.ShouldBe(1);
        observed[0].Count.ShouldBe(1);
        observed[0][0].EventType.ShouldBe(typeof(QuestStarted));
        observed[0][0].StreamId.ShouldBe(streamId);

        // A second commit notifies again with only that commit's events.
        await using (var session = store.LightweightSession())
        {
            session.Events.Append(streamId, new MonsterSlain("Dragon", 10));
            await session.SaveChangesAsync();
        }

        observed.Count.ShouldBe(2);
        observed[1][0].EventType.ShouldBe(typeof(MonsterSlain));
    }

    [Fact]
    public async Task observer_is_invoked_on_an_ancillary_store()
    {
        var observed = new List<IReadOnlyList<IEvent>>();
        await using var provider = BuildProvider(advancedTracking: true, services =>
        {
            // AddPolecatStore<T> requires a primary store registration.
            services.AddPolecat(opts => ConfigureStore(opts, "append_observer_primary"));
            services.AddPolecatStore<IObserverAncillaryStore>(opts =>
            {
                ConfigureStore(opts, "append_observer_ancillary");
                opts.Events.AppendObserver = events => observed.Add(events);
            });
        });

        var store = await MigrateAsync(provider.GetRequiredService<IObserverAncillaryStore>());
        var streamId = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId, new QuestStarted("Ancillary"));
        await session.SaveChangesAsync();

        observed.Count.ShouldBe(1);
        observed[0][0].EventType.ShouldBe(typeof(QuestStarted));
        observed[0][0].StreamId.ShouldBe(streamId);
    }

    [Fact]
    public async Task observer_is_not_invoked_without_advanced_tracking()
    {
        var calls = 0;
        await using var provider = BuildProvider(advancedTracking: false, services =>
            services.AddPolecat(opts =>
            {
                ConfigureStore(opts, "append_observer_off");
                opts.Events.AppendObserver = _ => calls++;
            }));

        var store = await MigrateAsync(provider.GetRequiredService<IDocumentStore>());
        await using var session = store.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new QuestStarted("Unobserved"));
        await session.SaveChangesAsync();

        calls.ShouldBe(0);
    }

    [Fact]
    public async Task observer_fault_does_not_fail_the_commit()
    {
        await using var provider = BuildProvider(advancedTracking: true, services =>
            services.AddPolecat(opts =>
            {
                ConfigureStore(opts, "append_observer_fault");
                opts.Events.AppendObserver = _ => throw new InvalidOperationException("boom");
            }));

        var store = await MigrateAsync(provider.GetRequiredService<IDocumentStore>());
        var streamId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new QuestStarted("Resilient"));
            await Should.NotThrowAsync(async () => await session.SaveChangesAsync());
        }

        await using var query = store.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
    }

    private static ServiceProvider BuildProvider(bool advancedTracking, Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        if (advancedTracking)
        {
            services.AddJasperFx(o => o.EnableAdvancedTracking = true);
        }
        else
        {
            services.AddJasperFx();
        }

        configure(services);
        return services.BuildServiceProvider();
    }

    private static async Task<TStore> MigrateAsync<TStore>(TStore store) where TStore : class, IDocumentStore
    {
        await ((DocumentStore)(IDocumentStore)store).Database.ApplyAllConfiguredChangesToDatabaseAsync();
        return store;
    }

    private static void ConfigureStore(StoreOptions opts, string schema)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
    }
}
