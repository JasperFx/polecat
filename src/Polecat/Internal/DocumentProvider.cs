using System.Diagnostics.CodeAnalysis;
using JasperFx;
using Polecat.Internal.Operations;
using Polecat.Metadata;
using Polecat.Schema.Identity.Sequences;
using Polecat.Serialization;
using Polecat.Storage;

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

    public UpsertOperation BuildUpsert(object document, ISerializer serializer, string tenantId)
    {
        AssignIdIfNeeded(document);
        var id = Mapping.GetId(document);
        var json = serializer.ToJson(document);

        int expectedRevision = 0;
        Guid? expectedGuidVersion = null;

        if (Mapping.UseNumericRevisions && document is IRevisioned revisioned)
        {
            expectedRevision = revisioned.Version;
        }
        else if (Mapping.UseOptimisticConcurrency && document is IVersioned versioned)
        {
            expectedGuidVersion = versioned.Version;
        }

        return new UpsertOperation(document, id, json, Mapping, tenantId, expectedRevision, expectedGuidVersion);
    }

    public InsertOperation BuildInsert(object document, ISerializer serializer, string tenantId)
    {
        AssignIdIfNeeded(document);
        var id = Mapping.GetId(document);
        var json = serializer.ToJson(document);
        return new InsertOperation(document, id, json, Mapping, tenantId);
    }

    public UpdateOperation BuildUpdate(object document, ISerializer serializer, string tenantId)
    {
        var id = Mapping.GetId(document);
        var json = serializer.ToJson(document);

        int expectedRevision = 0;
        Guid? expectedGuidVersion = null;

        if (Mapping.UseNumericRevisions && document is IRevisioned revisioned)
        {
            expectedRevision = revisioned.Version;
        }
        else if (Mapping.UseOptimisticConcurrency && document is IVersioned versioned)
        {
            expectedGuidVersion = versioned.Version;
        }

        return new UpdateOperation(document, id, json, Mapping, tenantId, expectedRevision, expectedGuidVersion);
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
        // Auto-assign Guid for strongly typed Guid wrappers when default
        if (Mapping.IsStrongTypedId && Mapping.InnerIdType == typeof(Guid))
        {
            var currentId = Mapping.GetId(document);
            if ((Guid)currentId == Guid.Empty)
            {
                Mapping.SetId(document, Guid.NewGuid());
            }

            return;
        }

        if (Sequence == null || !Mapping.IsNumericId) return;

        var numericId = Mapping.GetId(document);
        if (Mapping.InnerIdType == typeof(int))
        {
            if ((int)numericId <= 0)
            {
                Mapping.SetId(document, Sequence.NextInt());
            }
        }
        else if (Mapping.InnerIdType == typeof(long))
        {
            if ((long)numericId <= 0)
            {
                Mapping.SetId(document, Sequence.NextLong());
            }
        }
    }
}
