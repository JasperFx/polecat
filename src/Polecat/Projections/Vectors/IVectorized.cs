namespace Polecat.Projections.Vectors;

/// <summary>
///     The shape a document must have for <see cref="VectorProjection{TDoc,TId}" /> to maintain it.
/// </summary>
/// <remarks>
///     <para>
///         <see cref="ContentHash" /> is the member that makes a rebuild affordable rather than
///         expensive, so it is part of the contract rather than an implementation detail. An embedding
///         call is a metered network round trip; without a stored hash, replaying a stream would
///         re-embed every document whose content had not changed at all.
///     </para>
///     <para>
///         <see cref="Embedding" /> is an ordinary <c>float[]</c> on an ordinary document, which is what
///         makes the result searchable with nothing else added — declare
///         <c>Schema.For&lt;TDoc&gt;().VectorIndex(x =&gt; x.Embedding, dimensions)</c> and
///         <c>VectorSearchAsync</c> reads it like any other document. Conjoined tenancy, soft delete and
///         the identity map all apply without this projection knowing about them.
///     </para>
/// </remarks>
public interface IVectorized<TId>
{
    TId Id { get; set; }

    /// <summary>The text the embedding was produced from.</summary>
    string? Content { get; set; }

    /// <summary>SHA-256 of <see cref="Content" />; an unchanged hash costs no provider call.</summary>
    string? ContentHash { get; set; }

    float[]? Embedding { get; set; }
}
