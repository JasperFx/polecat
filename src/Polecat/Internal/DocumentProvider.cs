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

    // #273 E2e: the per-document write/delete operation factories are retired — every write
    // and delete now flows through the closed-shape storage layer (Storage/ClosedShape).
    // What remains here is the read-side SQL the batching items still compose (SelectSql /
    // LoadSql / DocTypeColumnIndex) and the sequence plumbing.
}
