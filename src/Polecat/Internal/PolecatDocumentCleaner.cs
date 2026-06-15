namespace Polecat.Internal;

/// <summary>
///     Thin adapter exposing the schema-scoped clean operations on
///     <see cref="AdvancedOperations" /> through the <see cref="IDocumentCleaner" />
///     contract. Mirrors how Marten surfaces <c>IDocumentCleaner</c> off the store.
/// </summary>
internal sealed class PolecatDocumentCleaner : IDocumentCleaner
{
    private readonly AdvancedOperations _advanced;

    internal PolecatDocumentCleaner(AdvancedOperations advanced)
    {
        _advanced = advanced;
    }

    public Task DeleteAllDocumentsAsync(CancellationToken ct = default)
        => _advanced.CleanAllDocumentsAsync(ct);

    public Task DeleteAllEventDataAsync(CancellationToken ct = default)
        => _advanced.CleanAllEventDataAsync(ct);

    public Task CompletelyRemoveAllAsync(CancellationToken ct = default)
        => _advanced.CompletelyRemoveAllAsync(ct);
}
