namespace Polecat.Events.TestSupport;

/// <summary>
///     Thrown when a ProjectionScenario fails. Contains step descriptions and all collected exceptions.
/// </summary>
public class ProjectionScenarioException : AggregateException
{
    public ProjectionScenarioException(List<string> descriptions, List<Exception> exceptions)
        : base($"Event Projection Scenario Failure{Environment.NewLine}{string.Join(Environment.NewLine, descriptions)}",
            exceptions)
    {
    }
}
