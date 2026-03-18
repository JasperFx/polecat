namespace Polecat.Exceptions;

/// <summary>
///     Thrown when attempting to append to a stream that does not exist,
///     specifically in optimistic or exclusive append scenarios.
/// </summary>
public class NonExistentStreamException : Exception
{
    public NonExistentStreamException(object id)
        : base($"Attempt to append to a nonexistent event stream '{id}'")
    {
        Id = id;
    }

    public object Id { get; }
}
