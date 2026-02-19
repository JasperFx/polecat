namespace Polecat.Exceptions;

/// <summary>
///     Thrown when an operation is attempted on a stream that is in an invalid state,
///     such as appending to an archived stream.
/// </summary>
public class InvalidStreamException : Exception
{
    public InvalidStreamException(object streamId, string reason)
        : base($"Stream '{streamId}' is invalid: {reason}")
    {
        StreamId = streamId;
    }

    public object StreamId { get; }
}
