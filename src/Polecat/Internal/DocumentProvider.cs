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
            var baseCols = "id, data, version, last_modified, dotnet_type, tenant_id";
            if (Mapping.UseOptimisticConcurrency)
            {
                baseCols += ", guid_version";
            }

            return $"SELECT {baseCols} FROM {Mapping.QualifiedTableName}";
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
        if (Sequence == null || !Mapping.IsNumericId) return;

        var currentId = Mapping.GetId(document);
        if (Mapping.IdType == typeof(int))
        {
            if ((int)currentId <= 0)
            {
                Mapping.SetId(document, Sequence.NextInt());
            }
        }
        else if (Mapping.IdType == typeof(long))
        {
            if ((long)currentId <= 0)
            {
                Mapping.SetId(document, Sequence.NextLong());
            }
        }
    }
}
