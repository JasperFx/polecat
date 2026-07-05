using Weasel.Core.Identity;
using Weasel.Core.Sequences;

namespace Polecat.Internal;

/// <summary>
///     #273: non-generic adapter over Weasel.Core's generic <see cref="IIdentification{TDoc,TId}" />
///     strategy, so Polecat's object-based <see cref="Polecat.Storage.DocumentMapping" /> can route its
///     id-generation path through the shared identity runtime. Resolved once per document type.
/// </summary>
internal interface IIdentityAssigner
{
    void AssignIfMissing(object document, ISequenceSource sequences);
}

internal sealed class IdentityAssigner<TDoc, TId> : IIdentityAssigner
    where TDoc : notnull
    where TId : notnull
{
    private readonly IIdentification<TDoc, TId> _identification;

    public IdentityAssigner(IIdentification<TDoc, TId> identification) => _identification = identification;

    public void AssignIfMissing(object document, ISequenceSource sequences)
        => _identification.AssignIfMissing((TDoc)document, sequences);
}
