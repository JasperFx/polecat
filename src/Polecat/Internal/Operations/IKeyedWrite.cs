namespace Polecat.Internal.Operations;

/// <summary>
///     #935: what a write names its row by, so a refusal of the row can say which one it was.
/// </summary>
internal interface IKeyedWrite
{
    string? TenantId { get; }

    object? Key { get; }
}
