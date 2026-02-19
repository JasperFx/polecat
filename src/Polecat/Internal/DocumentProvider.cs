using Polecat.Internal.Operations;
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

    public string SelectSql =>
        $"SELECT id, data, version, last_modified, dotnet_type, tenant_id FROM {Mapping.QualifiedTableName}";

    public string LoadSql => $"{SelectSql} WHERE id = @id AND tenant_id = @tenant_id;";

    public UpsertOperation BuildUpsert(object document, ISerializer serializer, string tenantId)
    {
        AssignIdIfNeeded(document);
        var id = Mapping.GetId(document);
        var json = serializer.ToJson(document);
        return new UpsertOperation(document, id, json, Mapping, tenantId);
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
        return new UpdateOperation(document, id, json, Mapping, tenantId);
    }

    public DeleteByIdOperation BuildDeleteById(object id, string tenantId)
    {
        return new DeleteByIdOperation(id, Mapping, tenantId);
    }

    public DeleteByIdOperation BuildDeleteByDocument(object document, string tenantId)
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
