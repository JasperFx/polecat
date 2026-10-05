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

/// <summary>
///     #733: the same adapter over Weasel's NON-GENERIC identity facade, for Native AOT.
/// </summary>
/// <remarks>
///     <para>
///         <see cref="IdentityAssigner{TDoc,TId}" /> is closed over <c>(documentType, idType)</c>, and
///         <c>idType</c> is routinely <see cref="System.Guid" /> or <c>int</c>.
///         <c>MakeGenericType</c> shares one canonical body across all-reference-type arguments but
///         cannot close an instantiation with a VALUE-type argument, so that construction was the
///         last wall a natively-published Polecat hit — after the id accessors and the strong-typed
///         id converters:
///     </para>
///     <code>
///     NotSupportedException: 'Polecat.Internal.IdentityAssigner`2[DeadLetterEvent,System.Guid]'
///     is missing native code or metadata.
///     </code>
///     <para>
///         ⚠️ <b>Reflection was tried and is a dead end</b>, which is why this needed an upstream
///         seam rather than a local workaround: holding the strategy as <see cref="object" /> and
///         invoking <c>AssignIfMissing</c> through <c>MethodInfo</c> fails because the trimmer removed
///         the member metadata — <c>GetMethods()</c> comes back empty. Weasel 9.41.0's
///         <see cref="IIdentification" /> facade (weasel#689) is a statically-referenced interface,
///         which is the one shape that survives both problems.
///     </para>
///     <para>
///         <b>Why the generic adapter stays for the JIT.</b> The facade's
///         <c>AssignIfMissing</c> returns <see cref="object" />, so it boxes the assigned id on every
///         call; the generic adapter discards a <c>TId</c> and allocates nothing. Weasel's own
///         contract advertises "no allocations when the document already has an id", and id
///         assignment is on every insert — so the boxing is taken only where the alternative is not
///         running at all.
///     </para>
/// </remarks>
internal sealed class FacadeIdentityAssigner : IIdentityAssigner
{
    private readonly IIdentification _identification;

    public FacadeIdentityAssigner(IIdentification identification) => _identification = identification;

    public void AssignIfMissing(object document, ISequenceSource sequences)
        => _identification.AssignIfMissing(document, sequences);
}
