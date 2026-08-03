using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Events.TestSupport;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public partial class ScenarioQuestParty
{
    public Guid Id { get; set; }
    public List<string> Members { get; set; } = [];
    public string Name { get; set; } = string.Empty;

    public void Apply(QuestStarted e) => Name = e.Name;
    public void Apply(MembersJoined e) => Members.AddRange(e.Members);
    public void Apply(MembersDeparted e) => Members.RemoveAll(m => e.Members.Contains(m));
}

/// <summary>
///     String-keyed twin of <see cref="ScenarioQuestParty" />, so the scenario's object-id load
///     dispatch gets exercised with something other than a Guid.
/// </summary>
public partial class ScenarioStringQuestParty
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;

    public void Apply(QuestStarted e) => Name = e.Name;
}

[Collection("integration")]
public class projection_scenario_tests : IntegrationContext
{
    public projection_scenario_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task scenario_with_inline_projection_document_should_exist()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_inline";
            opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Inline);
        });

        var questId = Guid.NewGuid();

        await theStore.Advanced.EventProjectionScenario(scenario =>
        {
            scenario.Append(questId, new QuestStarted("The Ring Quest"));
            scenario.DocumentShouldExist<ScenarioQuestParty>(questId, doc =>
            {
                doc.Name.ShouldBe("The Ring Quest");
            });
        }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task scenario_with_multi_step_events()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_multi";
            opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Inline);
        });

        var questId = Guid.NewGuid();

        await theStore.Advanced.EventProjectionScenario(scenario =>
        {
            scenario.Append(questId, new QuestStarted("Fellowship"));

            scenario.DocumentShouldExist<ScenarioQuestParty>(questId, doc =>
            {
                doc.Name.ShouldBe("Fellowship");
                doc.Members.Count.ShouldBe(0);
            });

            scenario.Append(questId,
                new MembersJoined(1, "Shire", ["Frodo", "Sam", "Gandalf"]));

            scenario.DocumentShouldExist<ScenarioQuestParty>(questId, doc =>
            {
                doc.Members.Count.ShouldBe(3);
                doc.Members.ShouldContain("Frodo");
            });

            scenario.Append(questId,
                new MembersDeparted(2, "Moria", ["Gandalf"]));

            scenario.DocumentShouldExist<ScenarioQuestParty>(questId, doc =>
            {
                doc.Members.Count.ShouldBe(2);
                doc.Members.ShouldNotContain("Gandalf");
            });
        }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task scenario_document_should_not_exist()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_notexist";
            opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Inline);
        });

        var missingId = Guid.NewGuid();

        await theStore.Advanced.EventProjectionScenario(scenario =>
        {
            scenario.DocumentShouldNotExist<ScenarioQuestParty>(missingId);
        }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task scenario_with_append_events_lambda()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_lambda";
            opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Inline);
        });

        var questId = Guid.NewGuid();

        await theStore.Advanced.EventProjectionScenario(scenario =>
        {
            scenario.AppendEvents("Start quest and add members", events =>
            {
                events.StartStream(questId, new QuestStarted("Lambda Quest"));
                events.Append(questId, new MembersJoined(1, "Bag End", ["Bilbo"]));
            });

            scenario.DocumentShouldExist<ScenarioQuestParty>(questId, doc =>
            {
                doc.Name.ShouldBe("Lambda Quest");
                doc.Members.ShouldContain("Bilbo");
            });
        }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task scenario_failure_throws_projection_scenario_exception()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_fail";
            opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Inline);
        });

        var missingId = Guid.NewGuid();

        await Should.ThrowAsync<JasperFx.Events.TestSupport.ProjectionScenarioException>(async () =>
        {
            await theStore.Advanced.EventProjectionScenario(scenario =>
            {
                // Assert a document exists when it doesn't
                scenario.DocumentShouldExist<ScenarioQuestParty>(missingId);
            });
        });
    }

    // The tests below cover what #404 bought: the harness is now
    // JasperFx.Events.TestSupport.ProjectionScenario<,> and Polecat supplies only the seam. They
    // exercise the seam through surface Polecat's own copy never had, rather than re-testing
    // scripting behavior that jasperfx#616 already unit-tests against a fake store.

    [Fact]
    public async Task append_accepts_an_enumerable_of_events()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_enumerable";
            opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Inline);
        });

        var questId = Guid.NewGuid();

        // The headline gap in the original #404: a caller holding a List<object> had to spread it
        // at every call site, because Polecat's copy was params-only.
        var events = new List<object>
        {
            new QuestStarted("Enumerable Quest"),
            new MembersJoined(1, "Bree", ["Barliman"])
        };

        await theStore.Advanced.EventProjectionScenario(scenario =>
        {
            scenario.StartStream(questId, events);

            scenario.DocumentShouldExist<ScenarioQuestParty>(questId, doc =>
            {
                doc.Name.ShouldBe("Enumerable Quest");
                doc.Members.ShouldContain("Barliman");
            });
        }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task string_stream_key_flows_through_the_object_id_load_dispatch()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_stringkey";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add<SingleStreamProjection<ScenarioStringQuestParty, string>>(
                ProjectionLifecycle.Inline);
        });

        var key = "quest-" + Guid.NewGuid();

        // DocumentShouldExist takes object now rather than one overload per identity type, so the
        // seam's LoadDocumentAsync has to dispatch on the runtime type. That is the part only a
        // real store can prove.
        await theStore.Advanced.EventProjectionScenario(scenario =>
        {
            scenario.StartStream(key, new QuestStarted("Keyed Quest"));
            scenario.DocumentShouldExist<ScenarioStringQuestParty>(key, doc => doc.Name.ShouldBe("Keyed Quest"));
        }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task a_trailing_append_with_no_assertion_after_it_is_still_committed()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_trailing";
            opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Inline);
        });

        var questId = Guid.NewGuid();

        // An arrange-only scenario used to be a silent no-op that passed: appends only flushed when
        // the next step was an assertion, and the trailing one was disposed uncommitted (marten#5126).
        await theStore.Advanced.EventProjectionScenario(scenario =>
        {
            scenario.Append(questId, new QuestStarted("Trailing Quest"));
        }, TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var party = await query.LoadAsync<ScenarioQuestParty>(questId, TestContext.Current.CancellationToken);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Trailing Quest");
    }

    [Fact]
    public async Task a_scenario_cannot_be_executed_twice()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_once";
            opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Inline);
        });

        var scenario = new ProjectionScenario(theStore);
        scenario.Append(Guid.NewGuid(), new QuestStarted("Once"));

        await scenario.ExecuteAsync(TestContext.Current.CancellationToken);

        // The steps were consumed by the first run, so a second run would be a silent no-op. It
        // should be a loud failure instead.
        await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            await scenario.ExecuteAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task delete_existing_data_can_be_turned_off()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_keepdata";
            opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Inline);
        });

        var first = Guid.NewGuid();
        var second = Guid.NewGuid();

        await theStore.Advanced.EventProjectionScenario(scenario =>
        {
            scenario.Append(first, new QuestStarted("Survivor"));
        }, TestContext.Current.CancellationToken);

        // DeleteExistingData replaced the double-negative DoNotDeleteExistingData, and defaults to
        // true -- so without this the first quest would be wiped by the second run.
        await theStore.Advanced.EventProjectionScenario(scenario =>
        {
            scenario.DeleteExistingData = false;
            scenario.Append(second, new QuestStarted("Newcomer"));

            scenario.DocumentShouldExist<ScenarioQuestParty>(first, doc => doc.Name.ShouldBe("Survivor"));
            scenario.DocumentShouldExist<ScenarioQuestParty>(second, doc => doc.Name.ShouldBe("Newcomer"));
        }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task scenario_stands_up_a_daemon_for_an_async_projection()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "scenario_async";
            opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Async);
        });

        var questId = Guid.NewGuid();

        // Exercises the BuildDaemonAsync seam and the non-stale wait: with an async lifecycle the
        // assertion can only pass if the scenario actually ran a daemon and waited for it.
        await theStore.Advanced.EventProjectionScenario(scenario =>
        {
            scenario.Append(questId,
                new QuestStarted("Async Quest"),
                new MembersJoined(1, "Rivendell", ["Elrond"]));

            scenario.DocumentShouldExist<ScenarioQuestParty>(questId, doc =>
            {
                doc.Name.ShouldBe("Async Quest");
                doc.Members.ShouldContain("Elrond");
            });
        }, TestContext.Current.CancellationToken);
    }
}
