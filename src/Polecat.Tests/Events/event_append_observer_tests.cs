using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     Covers polecat#213 — Polecat's implementation of
///     <see cref="IEventStoreInstrumentation.AppendObserver" /> (jasperfx 2.15.0): the store emits a
///     runtime append observation, carrying the events committed in each SaveChanges, for external
///     lifecycle observation (CritterWatch#500).
/// </summary>
[Collection("integration")]
public class event_append_observer_tests : IntegrationContext
{
    public event_append_observer_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task observer_is_invoked_with_appended_events_after_each_commit()
    {
        var observed = new List<IReadOnlyList<IEvent>>();
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "append_observer";
            opts.Events.AppendObserver = events => observed.Add(events);
        });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Observed"));
        await theSession.SaveChangesAsync();

        // First commit notified once, carrying that commit's event with self-describing metadata.
        observed.Count.ShouldBe(1);
        observed[0].Count.ShouldBe(1);
        observed[0][0].EventType.ShouldBe(typeof(QuestStarted));
        observed[0][0].StreamId.ShouldBe(streamId);
        observed[0][0].Version.ShouldBe(1);
        observed[0][0].Timestamp.ShouldNotBe(default);

        // A second commit notifies again with only that commit's events.
        theSession.Events.Append(streamId, new MonsterSlain("Dragon", 10));
        await theSession.SaveChangesAsync();

        observed.Count.ShouldBe(2);
        observed[1].Count.ShouldBe(1);
        observed[1][0].EventType.ShouldBe(typeof(MonsterSlain));
        observed[1][0].StreamId.ShouldBe(streamId);
        observed[1][0].Version.ShouldBe(2);
    }

    [Fact]
    public async Task observer_is_not_invoked_when_no_events_are_appended()
    {
        var calls = 0;
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "append_observer";
            opts.Events.AppendObserver = _ => calls++;
        });

        // A document-only unit of work still commits, but appends no events.
        theSession.Store(new ObserverDoc { Id = Guid.NewGuid(), Name = "no events" });
        await theSession.SaveChangesAsync();

        calls.ShouldBe(0);
    }

    [Fact]
    public async Task observer_fault_does_not_fail_the_commit()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "append_observer";
            opts.Events.AppendObserver = _ => throw new InvalidOperationException("boom");
        });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Resilient"));

        // The events are already committed when the observer runs, so its fault is swallowed
        // (logged) rather than surfaced — SaveChanges succeeds and the events are durable.
        await Should.NotThrowAsync(async () => await theSession.SaveChangesAsync());

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
    }
}

public class ObserverDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
