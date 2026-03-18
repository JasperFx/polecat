namespace Polecat.Events.TestSupport;

internal class ScenarioAction : ScenarioStep
{
    private readonly Action<IEventOperations> _action;

    public ScenarioAction(Action<IEventOperations> action)
    {
        _action = action;
    }

    public override async Task Execute(ProjectionScenario scenario, CancellationToken ct = default)
    {
        _action(scenario.Session.Events);

        if (scenario.NextStep is ScenarioAssertion)
        {
            await scenario.Session.SaveChangesAsync(ct);
            await scenario.WaitForNonStaleData();
        }
    }
}
