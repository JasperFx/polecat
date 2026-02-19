namespace Polecat.Exceptions;

public class HiloSequenceAdvanceToNextHiAttemptsExceededException : Exception
{
    private const string DefaultMessage =
        "Advance to next hilo sequence retry limit exceeded. Unable to secure next hi sequence.";

    public HiloSequenceAdvanceToNextHiAttemptsExceededException() : base(DefaultMessage)
    {
    }
}
