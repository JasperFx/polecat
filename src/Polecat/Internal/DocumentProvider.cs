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

    // #273: the per-document write/delete operation factories (E2e) and the read-side SQL the
    // batching items used to compose (doc-side convergence) are both retired — every write,
    // delete, and load now flows through the closed-shape storage layer (Storage/ClosedShape).
    // What remains here is the table-mapping handle and the sequence plumbing.
}
