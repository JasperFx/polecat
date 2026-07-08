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

    // #234: tenant_id is present only on conjoined tables. When absent, every trailing column
    // ordinal shifts down by one, so the read-side ordinals below are computed off tenancy.
    private bool IsConjoined => Mapping.TenancyStyle == TenancyStyle.Conjoined;

    public string SelectSql
    {
        get
        {
            var baseCols = "id, data, version, last_modified, created_at, dotnet_type";
            if (IsConjoined)
            {
                baseCols += ", tenant_id";
            }

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
    ///     Column index of guid_version in SelectSql (valid only under optimistic concurrency).
    ///     Base columns id[0]..dotnet_type[5], then tenant_id[6] only when conjoined.
    /// </summary>
    public int GuidVersionColumnIndex => 6 + (IsConjoined ? 1 : 0);

    /// <summary>
    ///     Column index of doc_type in SelectSql, or -1 if not a hierarchy.
    /// </summary>
    public int DocTypeColumnIndex
    {
        get
        {
            if (!Mapping.IsHierarchy()) return -1;
            return 6 + (IsConjoined ? 1 : 0) + (Mapping.UseOptimisticConcurrency ? 1 : 0);
        }
    }

    public string LoadSql
    {
        get
        {
            var softDeleteFilter = Mapping.DeleteStyle == DeleteStyle.SoftDelete
                ? " AND is_deleted = 0"
                : "";
            var tenantFilter = IsConjoined ? " AND tenant_id = @tenant_id" : "";
            return $"{SelectSql} WHERE id = @id{tenantFilter}{softDeleteFilter};";
        }
    }

    // #273 E2e: the per-document write/delete operation factories are retired — every write
    // and delete now flows through the closed-shape storage layer (Storage/ClosedShape).
    // What remains here is the read-side SQL the batching items still compose (SelectSql /
    // LoadSql / DocTypeColumnIndex) and the sequence plumbing.
}
