namespace Polecat.Internal.Operations;

/// <summary>
///     An operation that acts on a stream that's already there, found by its id or key. #919: a key the
///     column can't hold would act on whichever stream it collapses onto, so a save drops the operation.
/// </summary>
internal interface IStreamKeyedOperation
{
    object StreamId { get; }
}
