using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Reflection;
using JasperFx.Documents;
using Polecat.Storage;

namespace Polecat;

/// <summary>
///     jasperfx#870 — the write half of the diagnostics surface: save one document from JSON, delete
///     one, restore one soft-deleted one (jasperfx#1059), each with an optional expected-version guard,
///     and preview what a save would store without writing it (jasperfx#1071).
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

        var document = ReadDocument(documentType, request);

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
    /// <remarks>
    ///     jasperfx#1071. The save's own <see cref="ReadDocument" /> step, so the refusals are the
    ///     save's, followed by the same serializer call the document storage makes when it binds the
    ///     upsert's JSON parameter. No session is opened and nothing is read or written, so neither the
    ///     version guard nor the document's existence comes into it.
    /// </remarks>
    [RequiresUnreferencedCode("Deserializes a document from JSON to a runtime Type through ISerializer.FromJson.")]
    [RequiresDynamicCode("Deserializes a document from JSON to a runtime Type through ISerializer.FromJson.")]
    public Task<string> PreviewDocumentJsonAsync(DocumentWriteRequest request, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        token.ThrowIfCancellationRequested();

        var documentType = ResolveWritableType(request.DocumentTypeName);
        var document = ReadDocument(documentType, request);

        return Task.FromResult(_store.Options.Serializer.ToJson(document));
    }

    /// <summary>
    ///     The deserialize step a save and a preview share: the store's own serializer reads the JSON as
    ///     the resolved type, and the id inside it has to agree with the request's. Shared so a preview
    ///     can never accept what a save would refuse, or the other way round.
    /// </summary>
    [RequiresUnreferencedCode("Deserializes a document from JSON to a runtime Type through ISerializer.FromJson.")]
    [RequiresDynamicCode("Deserializes a document from JSON to a runtime Type through ISerializer.FromJson.")]
    private object ReadDocument(Type documentType, DocumentWriteRequest request)
    {
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

        return document;
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

    /// <inheritdoc />
    /// <remarks>
    ///     #813 (jasperfx#1059). Queues the same <c>UndoDeleteWhereOperation</c> that
    ///     <see cref="IDocumentOperations.UndoDeleteWhere{T}" /> does, through an ordinary session, with
    ///     an id predicate and the version moved on so the restored row reads as a new write.
    /// </remarks>
    [RequiresUnreferencedCode("Closes the session's undelete over a runtime document type.")]
    [RequiresDynamicCode("Closes the session's undelete over a runtime document type.")]
    public async Task<DocumentWriteResult> UndeleteDocumentAsync(DocumentUndeleteRequest request,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var documentType = ResolveWritableType(request.DocumentTypeName);
        var mapping = _store.Options.Providers.GetProvider(documentType).Mapping;

        // A hard-deleting type has nothing to restore. Answered rather than thrown, so a console can
        // offer the action per type without guarding the call.
        if (mapping.DeleteStyle != DeleteStyle.SoftDelete)
        {
            return new DocumentWriteResult(DocumentWriteStatus.NotSupported);
        }

        var current = await CurrentAsync(request.DocumentTypeName, request.Id, request.TenantId, token)
            .ConfigureAwait(false);

        // A live row is NotFound too: an undelete that did nothing must not look like one that did.
        if (current == null || !current.IsDeleted)
        {
            return new DocumentWriteResult(DocumentWriteStatus.NotFound);
        }

        if (IsStale(request.ExpectedVersion, current))
        {
            return new DocumentWriteResult(DocumentWriteStatus.ConcurrencyConflict, current);
        }

        var (typedId, idType) = TypedId(mapping.InnerIdType, request.Id);

        await using (var session = OpenSession(request.TenantId))
        {
            typeof(Internal.DocumentSessionBase)
                .GetMethod(nameof(Internal.DocumentSessionBase.UndeleteByIdForDiagnostics),
                    System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
                .MakeGenericMethod(documentType)
                .Invoke(session, [typedId, idType]);
            await session.SaveChangesAsync(token).ConfigureAwait(false);
        }

        var restored = await CurrentAsync(request.DocumentTypeName, request.Id, request.TenantId, token)
            .ConfigureAwait(false);

        return new DocumentWriteResult(DocumentWriteStatus.Undeleted, restored);
    }

    private static (object Id, SqlDbType Type) TypedId(Type innerIdType, string id)
    {
        var type = Nullable.GetUnderlyingType(innerIdType) ?? innerIdType;

        if (type == typeof(Guid)) return (Guid.Parse(id), SqlDbType.UniqueIdentifier);
        if (type == typeof(int)) return (int.Parse(id), SqlDbType.Int);
        if (type == typeof(long)) return (long.Parse(id), SqlDbType.BigInt);

        // VarChar rather than NVarChar, so the comparison does not CONVERT_IMPLICIT the column (#363).
        return (id, SqlDbType.VarChar);
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
