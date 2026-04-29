using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public class ScenarioQuestParty
{
    public Guid Id { get; set; }
    public List<string> Members { get; set; } = [];
    public string Name { get; set; } = string.Empty;

    public void Apply(QuestStarted e) => Name = e.Name;
    public void Apply(MembersJoined e) => Members.AddRange(e.Members);
    public void Apply(MembersDeparted e) => Members.RemoveAll(m => e.Members.Contains(m));
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
        });
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
        });
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
        });
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
        });
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

        await Should.ThrowAsync<Polecat.Events.TestSupport.ProjectionScenarioException>(async () =>
        {
            await theStore.Advanced.EventProjectionScenario(scenario =>
            {
                // Assert a document exists when it doesn't
                scenario.DocumentShouldExist<ScenarioQuestParty>(missingId);
            });
        });
    }
}
