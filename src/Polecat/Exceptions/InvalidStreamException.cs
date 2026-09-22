namespace Polecat.Exceptions;

/// <summary>
///     Thrown when an operation is attempted on a stream that is in an invalid state.
/// </summary>
/// <remarks>
///     #654: appending to an archived stream was this type's ONLY use, and that refusal is now
///     <see cref="ArchivedStreamException" /> — named for the condition, and carrying a message that
///     names <c>UnArchiveStream</c> as the way back. Nothing in Polecat throws this any more.
///     <para>
///         Kept rather than deleted so a <c>catch (InvalidStreamException)</c> in application code
///         still COMPILES and the obsoletion says what to catch instead; a removal would have turned
///         a behaviour change into a build break, with the same fix either way.
///     </para>
/// </remarks>
[Obsolete("Appending to an archived stream now throws Polecat.Exceptions.ArchivedStreamException, " +
          "which derives from JasperFx.Events.ArchivedStreamException. Nothing throws " +
          "InvalidStreamException any more; catch ArchivedStreamException instead.")]
public class InvalidStreamException : Exception
{
    public InvalidStreamException(object streamId, string reason)
        : base($"Stream '{streamId}' is invalid: {reason}")
    {
        StreamId = streamId;
    }

    public object StreamId { get; }
}
