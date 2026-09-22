namespace Polecat.Exceptions;

/// <summary>
///     Thrown when attempting to append to a stream that does not exist,
///     specifically in optimistic or exclusive append scenarios.
/// </summary>
/// <remarks>
///     jasperfx#751 lifted this into <see cref="JasperFx.Events.NonExistentStreamException" /> with
///     Polecat's own message and its <c>Id</c> property as the canonical shape. Polecat keeps the
///     name in its own namespace so existing <c>catch</c> sites go on compiling, but derives from
///     the shared type so store-agnostic code can catch one exception across stores. See
///     <see cref="StreamLockedException" /> and <see cref="ExistingStreamIdCollisionException" />.
/// </remarks>
public class NonExistentStreamException : JasperFx.Events.NonExistentStreamException
{
    public NonExistentStreamException(object id) : base(id)
    {
    }
}
