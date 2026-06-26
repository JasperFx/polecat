using System.Diagnostics;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
/// #239: a session auto-seeds CorrelationId from Activity.Current.RootId and CausationId from
/// Activity.Current.ParentId on open, so distributed-tracing context flows onto events with zero
/// app code (mirrors Marten). An explicit caller value still wins.
/// </summary>
public class activity_correlation_tests : OneOffConfigurationsContext
{
    private async Task ConfigureAndApply(Action<StoreOptions> configure)
    {
        ConfigureStore(configure);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    private static (Activity parent, Activity child) StartActivityScope()
    {
        Activity.DefaultIdFormat = ActivityIdFormat.W3C;
        var parent = new Activity("parent").Start();
        var child = new Activity("child").Start(); // child of parent → has a non-null ParentId
        return (parent, child);
    }

    [Fact]
    public async Task session_seeds_correlation_and_causation_from_activity()
    {
        await ConfigureAndApply(_ => { });

        var (parent, child) = StartActivityScope();
        try
        {
            await using var session = theStore.LightweightSession();
            session.CorrelationId.ShouldBe(child.RootId);
            session.CausationId.ShouldBe(child.ParentId);
        }
        finally
        {
            child.Stop();
            parent.Stop();
        }
    }

    [Fact]
    public async Task events_appended_in_activity_scope_carry_root_and_parent_ids()
    {
        await ConfigureAndApply(opts =>
        {
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableCausationId = true;
        });

        var (parent, child) = StartActivityScope();
        var streamId = Guid.NewGuid();
        try
        {
            await using var session = theStore.LightweightSession();
            session.Events.StartStream(streamId, new QuestStarted("Traced Quest"));
            await session.SaveChangesAsync();
        }
        finally
        {
            child.Stop();
            parent.Stop();
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(1);
        events[0].CorrelationId.ShouldBe(child.RootId);
        events[0].CausationId.ShouldBe(child.ParentId);
    }

    [Fact]
    public async Task explicit_caller_value_wins_over_activity()
    {
        await ConfigureAndApply(_ => { });

        var (parent, child) = StartActivityScope();
        try
        {
            await using var session = theStore.LightweightSession();
            session.CorrelationId = "explicit-corr";
            session.CorrelationId.ShouldBe("explicit-corr");
            session.CorrelationId.ShouldNotBe(child.RootId);
        }
        finally
        {
            child.Stop();
            parent.Stop();
        }
    }

    [Fact]
    public async Task no_activity_leaves_correlation_null()
    {
        await ConfigureAndApply(_ => { });

        // Ensure no ambient activity for this test.
        var saved = Activity.Current;
        Activity.Current = null;
        try
        {
            await using var session = theStore.LightweightSession();
            session.CorrelationId.ShouldBeNull();
            session.CausationId.ShouldBeNull();
        }
        finally
        {
            Activity.Current = saved;
        }
    }
}
