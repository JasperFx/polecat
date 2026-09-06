namespace Polecat.Exceptions;

/// <summary>
///     Thrown when attempting to start a stream with an id that already exists.
/// </summary>
/// <remarks>
///     jasperfx#751 lifted this into <see cref="JasperFx.Events.ExistingStreamIdCollisionException" />
///     with Polecat's own message as the canonical wording. Polecat keeps the name in its own
///     namespace so existing <c>catch</c> sites go on compiling, but derives from the shared type,
///     which is what a store-agnostic caller — and
///     <c>ProjectionSideEffectCompliance.a_projection_that_starts_a_stream_that_already_exists_fails</c>
///     — catches. Subclassing rather than type-forwarding is what the shared type's own remarks
///     prescribe.
/// </remarks>
public class ExistingStreamIdCollisionException : JasperFx.Events.ExistingStreamIdCollisionException
{
    public ExistingStreamIdCollisionException(object id) : base(id)
    {
    }
}
