namespace Polecat;

/// <summary>
///     Allows registering transaction participants that will be called
///     before the session's transaction commits.
/// </summary>
public interface ITransactionParticipantRegistrar
{
    void AddTransactionParticipant(ITransactionParticipant participant);
}
