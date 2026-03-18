namespace Polecat.Exceptions;

/// <summary>
///     Thrown when a stream cannot be locked for exclusive access, typically due to
///     another transaction holding a lock on the stream row.
/// </summary>
public class StreamLockedException : Exception
{
    public StreamLockedException(object streamId, Exception? innerException)
        : base($"Stream '{streamId}' may be locked for updates", innerException)
    {
        StreamId = streamId;
    }

    public object StreamId { get; }
}
