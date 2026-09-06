using JasperFx;
using JasperFx.Core.Reflection;
using Polecat.Storage;

namespace Polecat.Linq.Parsing;

/// <summary>
///     Whether the table a query runs against has a deletion state for the operators on
///     <see cref="SoftDeletes.SoftDeletedExtensions" /> to talk about, and what to tell the caller
///     when it does not.
/// </summary>
/// <remarks>
///     <para>
///         gh-558. Every <see cref="LinqQueryParser" /> is handed one of these, with no default: a
///         query provider cannot construct a parser without answering the question, and the parser
///         refuses a soft-delete operator whose target answered "no". Previously the four operators
///         were only ever <em>read</em> — by the six places in
///         <see cref="PolecatLinqQueryProvider" /> that append <c>is_deleted = 0/1</c> behind a
///         <c>DeleteStyle == SoftDelete</c> guard — so against a hard-delete type the guard was
///         false, no predicate was appended, and <c>IsDeleted()</c> quietly returned every row.
///     </para>
///     <para>
///         Marten asserts the same thing from each of its four soft-delete parsers, and Fisher from
///         a single filter pass. Polecat asserts from the parser, which is the one place all query
///         shapes and all three providers share.
///     </para>
/// </remarks>
internal readonly record struct SoftDeleteTarget
{
    private SoftDeleteTarget(bool isSoftDeleted, string description, string remedy)
    {
        IsSoftDeleted = isSoftDeleted;
        Description = description;
        Remedy = remedy;
    }

    /// <summary>Whether the target table carries the <c>is_deleted</c> / <c>deleted_at</c> columns.</summary>
    public bool IsSoftDeleted { get; }

    /// <summary>How to name the target in a refusal message.</summary>
    public string Description { get; }

    /// <summary>What the caller can do about it.</summary>
    public string Remedy { get; }

    /// <summary>
    ///     A document query, which supports the soft-delete operators exactly when the type opted in.
    /// </summary>
    public static SoftDeleteTarget For(DocumentMapping mapping)
    {
        var name = mapping.DocumentType.FullNameInCode();
        return new SoftDeleteTarget(
            mapping.DeleteStyle == DeleteStyle.SoftDelete,
            $"Document type '{name}'",
            $"Mark it with [SoftDeleted], implement ISoftDeleted, or call "
            + $"StoreOptions.Schema.For<{mapping.DocumentType.NameInCode()}>().SoftDeleted() if it should be.");
    }

    /// <summary>
    ///     A query over one of the event-store tables. Neither the events table nor the stream-state
    ///     table has a deletion state, and neither can be configured to have one.
    /// </summary>
    public static SoftDeleteTarget NeverSoftDeleted(string description) =>
        new(false, description,
            "Events are never soft deleted. Use Query<T>() against a soft-deleted document type instead.");
}
