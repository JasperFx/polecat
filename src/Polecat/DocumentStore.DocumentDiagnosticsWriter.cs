using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Reflection;
using JasperFx.Documents;

namespace Polecat;

/// <summary>
///     jasperfx#870 — the write half of the diagnostics surface: save one document from JSON, delete
///     one, both with an optional expected-version guard.
/// </summary>
/// <remarks>
///     <para>
///         <b>A separate class from the reader on purpose</b>, mirroring the interface split:
///         <see cref="IDocumentStoreDiagnostics" /> stays read-only, so a host can register the reader
///         for a console that only browses and leave the writer out entirely.
///     </para>
///     <para>
///         <b>Everything goes through an ordinary session.</b> The reader talks to the tables directly
///         because it must report exactly what is stored; a WRITE that bypassed the session would
///         bypass the store's own versioning, soft-delete style, tenancy routing, metadata stamping and
///         index maintenance — and would then disagree with every document the application itself
///         wrote. The session is the only thing that knows all of that, so the writer's job is to
///         translate a request into one, not to reimplement it.
///     </para>
/// </remarks>
public sealed class PolecatDocumentDiagnosticsWriter : IDocumentStoreDiagnosticsWriter
{
    private readonly DocumentStore _store;

    public PolecatDocumentDiagnosticsWriter(DocumentStore store)
    {
        _store = store;
    }

    /// <inheritdoc />
    public Uri Subject => ((IDocumentStoreDiagnostics)_store).Subject;

    /// <inheritdoc />
    [RequiresUnreferencedCode("Deserializes a document from JSON to a runtime Type through ISerializer.FromJson.")]
    [RequiresDynamicCode("Deserializes a document from JSON to a runtime Type through ISerializer.FromJson.")]
    public async Task<DocumentWriteResult> SaveDocumentJsonAsync(DocumentWriteRequest request,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var documentType = ResolveWritableType(request.DocumentTypeName);

        var current = await CurrentAsync(request.DocumentTypeName, request.Id, request.TenantId, token)
            .ConfigureAwait(false);

        if (IsStale(request.ExpectedVersion, current))
        {
            return new DocumentWriteResult(DocumentWriteStatus.ConcurrencyConflict, current);
        }

        object document;
        try
        {
            document = _store.Options.Serializer.FromJson(documentType, request.Json);
        }
        catch (Exception e)
        {
            throw new ArgumentException(
                $"The JSON could not be read as '{documentType.FullNameInCode()}': {e.Message}",
                nameof(request), e);
        }

        AssertIdAgrees(documentType, document, request.Id);

        await using (var session = OpenSession(request.TenantId))
        {
            session.StoreObjects([document]);
            await session.SaveChangesAsync(token).ConfigureAwait(false);
        }

        var saved = await CurrentAsync(request.DocumentTypeName, request.Id, request.TenantId, token)
            .ConfigureAwait(false);

        return new DocumentWriteResult(DocumentWriteStatus.Saved, saved);
    }

    /// <inheritdoc />
    public async Task<DocumentWriteResult> DeleteDocumentAsync(DocumentDeleteRequest request,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var documentType = ResolveWritableType(request.DocumentTypeName);

        var current = await CurrentAsync(request.DocumentTypeName, request.Id, request.TenantId, token)
            .ConfigureAwait(false);

        // A soft-deleted row is not a live one, so deleting it again is NotFound rather than a second
        // delete — the same distinction QueryDocumentsAsync draws by excluding it.
        if (current == null || current.IsDeleted)
        {
            return new DocumentWriteResult(DocumentWriteStatus.NotFound);
        }

        if (IsStale(request.ExpectedVersion, current))
        {
            return new DocumentWriteResult(DocumentWriteStatus.ConcurrencyConflict, current);
        }

        await using (var session = OpenSession(request.TenantId))
        {
            DeleteById(session, documentType, request.Id);
            await session.SaveChangesAsync(token).ConfigureAwait(false);
        }

        return new DocumentWriteResult(DocumentWriteStatus.Deleted);
    }

    private IDocumentSession OpenSession(string? tenantId)
    {
        var tenant = DocumentQueryOptions.NormalizeTenantId(tenantId);
        return tenant == null ? _store.LightweightSession() : _store.LightweightSession(tenant);
    }

    private Task<StoredDocument?> CurrentAsync(string typeName, string id, string? tenantId,
        CancellationToken token)
        => ((IDocumentStoreDiagnostics)_store).LoadDocumentAsync(typeName, id, tenantId, token);

    /// <summary>
    ///     A null expected version writes unconditionally. Otherwise the caller is asserting which
    ///     document it read, so a row that has since changed OR disappeared is a conflict — a vanished
    ///     document is not "no conflict", it is the strongest possible one.
    /// </summary>
    private static bool IsStale(string? expectedVersion, StoredDocument? current)
    {
        if (expectedVersion == null) return false;
        if (current == null) return true;
        return !string.Equals(current.Version, expectedVersion, StringComparison.OrdinalIgnoreCase);
    }

    private Type ResolveWritableType(string documentTypeName)
    {
        var type = _store.ResolveDiagnosticsWriteType(documentTypeName);
        if (type == null)
        {
            throw new ArgumentException(
                $"'{documentTypeName}' is not a document type mapped by this store.",
                nameof(documentTypeName));
        }

        return type;
    }

    /// <summary>
    ///     The id in the JSON has to agree with the request's. A mismatch is refused rather than
    ///     resolved: writing the JSON's id would save a different document than the caller named, and
    ///     writing the request's would silently rewrite the body's identity.
    /// </summary>
    [RequiresUnreferencedCode("Reads the document's Id member reflectively to compare it with the request.")]
    private static void AssertIdAgrees(Type documentType, object document, string requestedId)
    {
        var idMember = documentType.GetProperty("Id") ?? (System.Reflection.MemberInfo?)documentType.GetField("Id");
        if (idMember == null) return;

        var value = idMember switch
        {
            System.Reflection.PropertyInfo p => p.GetValue(document),
            System.Reflection.FieldInfo f => f.GetValue(document),
            _ => null
        };

        var actual = value?.ToString();
        if (actual == null) return;

        if (!string.Equals(actual, requestedId, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                $"The id in the JSON ('{actual}') does not match the requested id ('{requestedId}').",
                nameof(requestedId));
        }
    }

    /// <summary>
    ///     Delete by id through the session, so a soft-deleted type soft-deletes. The id is converted
    ///     to the session's own overload set rather than to SQL.
    /// </summary>
    [RequiresUnreferencedCode("Closes IDocumentOperations.Delete<T> over a runtime document type.")]
    [RequiresDynamicCode("Closes IDocumentOperations.Delete<T> over a runtime document type.")]
    private void DeleteById(IDocumentSession session, Type documentType, string id)
    {
        var mappingIdType = _store.Options.Providers.GetProvider(documentType).Mapping.InnerIdType;
        var type = Nullable.GetUnderlyingType(mappingIdType) ?? mappingIdType;

        object typedId = type switch
        {
            _ when type == typeof(Guid) => Guid.Parse(id),
            _ when type == typeof(int) => int.Parse(id),
            _ when type == typeof(long) => long.Parse(id),
            _ => id
        };

        var method = typeof(IDocumentOperations)
            .GetMethods()
            .First(m => m.Name == nameof(IDocumentOperations.Delete)
                        && m.GetParameters().Length == 1
                        && m.GetParameters()[0].ParameterType == typedId.GetType());

        method.MakeGenericMethod(documentType).Invoke(session, [typedId]);
    }
}
