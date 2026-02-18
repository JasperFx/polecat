namespace Polecat.Exceptions;

/// <summary>
///     Thrown when an optimistic concurrency check fails while updating
///     projection progression. This indicates the progression was updated
///     by another process between the read and write.
/// </summary>
public class ProgressionProgressOutOfOrderException : Exception
{
    public ProgressionProgressOutOfOrderException(string projectionName, long expectedFloor, long ceiling)
        : base(
            $"Progression for '{projectionName}' is out of order. Expected floor {expectedFloor} but it had already moved. Attempted ceiling: {ceiling}.")
    {
        ProjectionName = projectionName;
        ExpectedFloor = expectedFloor;
        AttemptedCeiling = ceiling;
    }

    public string ProjectionName { get; }
    public long ExpectedFloor { get; }
    public long AttemptedCeiling { get; }
}
