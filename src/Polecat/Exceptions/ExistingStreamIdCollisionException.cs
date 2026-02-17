namespace Polecat.Exceptions;

/// <summary>
///     Thrown when attempting to start a stream with an id that already exists.
/// </summary>
public class ExistingStreamIdCollisionException : Exception
{
    public ExistingStreamIdCollisionException(object id)
        : base($"Stream with id '{id}' already exists.")
    {
        Id = id;
    }

    public object Id { get; }
}
