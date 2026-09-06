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

/* The eleven facts this file carried were retired when Polecat enrolled
 * ProjectionScenarioCompliance (#556). The shared suite carries all eleven and nine more -- the
 * step observer and PlannedSteps (jasperfx#688), the general assertion hook, the typed inner
 * exception pair, skipped-step reporting, and the arrange-only and teardown-isolation cases.
 *
 * The scenario harness itself is shared code (JasperFx.Events.TestSupport.ProjectionScenario), so
 * what these tests were really exercising is Polecat's seam under it -- and the suite drives that
 * seam through Advanced.EventProjectionScenario, Polecat's own documented entry point, rather than
 * constructing the harness directly. That is a strictly better test of the same thing: a fixture
 * that reached past the entry point would pass while the entry point was broken.
 *
 * ScenarioQuestParty and ScenarioStringQuestParty stay: Projections/projection_sg_dispatch_audit_tests.cs
 * enumerates ScenarioQuestParty as a source-generator dispatch case.
 */
