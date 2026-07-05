using System.Diagnostics.CodeAnalysis;
using JasperFx;
using Polecat.Internal.Operations;
using Polecat.Metadata;
using Polecat.Schema.Identity.Sequences;
using Polecat.Serialization;
using Polecat.Storage;
using Weasel.Core.Sequences;

namespace Polecat.Internal;

/// <summary>
///     Per-document-type factory for storage operations. Caches the DocumentMapping
///     and generates SQL operations for a specific type.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: invokes ISerializer.ToJson, which is annotated RUC because the default STJ-reflection serializer requires unreferenced code. AOT consumers supply a source-generator-backed ISerializer impl per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.ToJson is annotated RDC for the same reason as IL2026 above. AOT consumers supply a source-generator-backed ISerializer impl.")]
internal class DocumentProvider
{
    public DocumentProvider(DocumentMapping mapping)
    {
        Mapping = mapping;
    }

    public DocumentMapping Mapping { get; }
    internal ISequence? Sequence { get; set; }

    // #273: the store's sequence source, used by the shared Weasel.Core.Identity generation strategy
    // (via DocumentMapping.AssignIdIfMissing). Set by the registry; present whenever a SequenceFactory
    // is configured (always, in practice).
    internal ISequenceSource? SequenceSource { get; set; }

    public string QualifiedTableName => Mapping.QualifiedTableName;

    public string SelectSql
    {
        get
        {
            var baseCols = "id, data, version, last_modified, created_at, dotnet_type, tenant_id";
            if (Mapping.UseOptimisticConcurrency)
            {
                baseCols += ", guid_version";
            }

            if (Mapping.IsHierarchy())
            {
                baseCols += ", doc_type";
            }

            return $"SELECT {baseCols} FROM {Mapping.QualifiedTableName}";
        }
    }

    /// <summary>
    ///     Column index of doc_type in SelectSql, or -1 if not a hierarchy.
    /// </summary>
    public int DocTypeColumnIndex
    {
        get
        {
            if (!Mapping.IsHierarchy()) return -1;
            // Base columns: id[0], data[1], version[2], last_modified[3], created_at[4], dotnet_type[5], tenant_id[6]
            // Optional: guid_version[7], doc_type[8] OR doc_type[7]
            return Mapping.UseOptimisticConcurrency ? 8 : 7;
        }
    }

    public string LoadSql
    {
        get
        {
            var softDeleteFilter = Mapping.DeleteStyle == DeleteStyle.SoftDelete
                ? " AND is_deleted = 0"
                : "";
            return $"{SelectSql} WHERE id = @id AND tenant_id = @tenant_id{softDeleteFilter};";
        }
    }

    public UpsertOperation BuildUpsert(object document, ISerializer serializer, string tenantId,
        DocumentMetadataValues metadata = default)
    {
        AssignIdIfNeeded(document);
        var id = Mapping.GetId(document);
        var json = serializer.ToJson(document);

        long expectedRevision = 0;
        Guid? expectedGuidVersion = null;

        if (Mapping.UseNumericRevisions && document is ILongVersioned longVersioned)
        {
            expectedRevision = longVersioned.Version;
        }
        else if (Mapping.UseNumericRevisions && document is IRevisioned revisioned)
        {
            expectedRevision = revisioned.Version;
        }
        else if (Mapping.UseOptimisticConcurrency && document is IVersioned versioned)
        {
            expectedGuidVersion = versioned.Version;
        }

        return new UpsertOperation(document, id, json, Mapping, tenantId, expectedRevision, expectedGuidVersion, metadata);
    }

    public InsertOperation BuildInsert(object document, ISerializer serializer, string tenantId,
        DocumentMetadataValues metadata = default)
    {
        AssignIdIfNeeded(document);
        var id = Mapping.GetId(document);
        var json = serializer.ToJson(document);
        return new InsertOperation(document, id, json, Mapping, tenantId, metadata);
    }

    public UpdateOperation BuildUpdate(object document, ISerializer serializer, string tenantId,
        DocumentMetadataValues metadata = default)
    {
        var id = Mapping.GetId(document);
        var json = serializer.ToJson(document);

        long expectedRevision = 0;
        Guid? expectedGuidVersion = null;

        if (Mapping.UseNumericRevisions && document is ILongVersioned longVersioned)
        {
            expectedRevision = longVersioned.Version;
        }
        else if (Mapping.UseNumericRevisions && document is IRevisioned revisioned)
        {
            expectedRevision = revisioned.Version;
        }
        else if (Mapping.UseOptimisticConcurrency && document is IVersioned versioned)
        {
            expectedGuidVersion = versioned.Version;
        }

        return new UpdateOperation(document, id, json, Mapping, tenantId, expectedRevision, expectedGuidVersion, metadata);
    }

    public IStorageOperation BuildDeleteById(object id, string tenantId)
    {
        if (Mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            return new SoftDeleteByIdOperation(id, Mapping, tenantId);
        }

        return new DeleteByIdOperation(id, Mapping, tenantId);
    }

    public IStorageOperation BuildDeleteByDocument(object document, string tenantId)
    {
        var id = Mapping.GetId(document);
        return BuildDeleteById(id, tenantId);
    }

    public DeleteByIdOperation BuildHardDeleteById(object id, string tenantId)
    {
        return new DeleteByIdOperation(id, Mapping, tenantId);
    }

    public DeleteByIdOperation BuildHardDeleteByDocument(object document, string tenantId)
    {
        var id = Mapping.GetId(document);
        return new DeleteByIdOperation(id, Mapping, tenantId);
    }

    private void AssignIdIfNeeded(object document)
    {
        // #273: id generation now runs through the shared Weasel.Core.Identity strategy resolved on the
        // mapping (Guid → UUIDv7, int/long → Hi-Lo, strong-typed wrappers handled, string left external),
        // replacing Polecat's bespoke per-type branching so single-doc and bulk-insert stay consistent.
        if (SequenceSource is not null)
        {
            Mapping.AssignIdIfMissing(document, SequenceSource);
        }
    }
}
