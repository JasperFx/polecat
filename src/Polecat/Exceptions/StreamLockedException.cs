namespace Polecat.Exceptions;

/// <summary>
///     Thrown when a stream cannot be locked for exclusive access, typically due to
///     another transaction holding a lock on the stream row.
/// </summary>
/// <remarks>
///     jasperfx#756 lifted this into <see cref="JasperFx.Events.StreamLockedException" /> with
///     Polecat's own message and shape (the <c>StreamId</c> property plus the nullable inner
///     exception) as the canonical wording. Polecat keeps the name in its own namespace so existing
///     <c>catch</c> sites go on compiling, but derives from the shared type, which is what a
///     store-agnostic caller catches — and what a Wolverine
///     <c>OnException&lt;JasperFx.Events.StreamLockedException&gt;().RetryWithCooldown(...)</c>
///     policy matches on. Subclassing rather than type-forwarding is what the shared type's own
///     remarks prescribe, and is what <see cref="ExistingStreamIdCollisionException" /> already does.
/// </remarks>
public class StreamLockedException : JasperFx.Events.StreamLockedException
{
    public StreamLockedException(object streamId, Exception? innerException)
        : base(streamId, innerException)
    {
    }
}
