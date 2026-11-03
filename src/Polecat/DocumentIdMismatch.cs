namespace Polecat;

/// <summary>
///     A stored document whose JSON id doesn't match the <c>id</c> column of its row. See
///     <see cref="AdvancedOperations.FindDocumentsWithMismatchedIdsAsync{T}(CancellationToken)" />.
/// </summary>
/// <param name="DocumentType">The document type, whose table the row is in.</param>
/// <param name="Id">The value of the <c>id</c> column, as the document's id type. This one is right.</param>
/// <param name="StoredId">
///     What the body holds in place of it, as text: an object or an array as JSON, and null for a JSON null.
/// </param>
/// <param name="TenantId">The row's tenant under conjoined tenancy, otherwise null.</param>
/// <param name="Database">The database the row is in, which is what tells tenants apart under database-per-tenant.</param>
/// <param name="IsCorrupted">
///     True when the server can't read the stored id at all, and reports "Encountered corrupted JSON type".
///     <paramref name="StoredId" /> is null, and an attempt to load the document fails.
/// </param>
public sealed record DocumentIdMismatch(
    Type DocumentType,
    object Id,
    string? StoredId,
    string? TenantId,
    string Database,
    bool IsCorrupted = false);
