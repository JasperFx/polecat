using JasperFx.Events;
using JasperFx.Events.Documents;

namespace Polecat.Services;

/// <summary>
///     A snapshot of the document and event changes committed in a single unit of work — either a
///     user <see cref="IDocumentSession.SaveChangesAsync" /> or an async daemon projection batch.
///     Handed to <see cref="IChangeListener" /> and <see cref="IDocumentSessionListener" /> so
///     post-commit side effects (cache invalidation, messaging, etc.) can see exactly what changed.
///     Mirrors Marten's <c>Marten.Services.IChangeSet</c>.
/// </summary>
public interface IChangeSet : IDocumentChangeSet
{
    /// <summary>
    ///     Documents that were updated (Update or Upsert operations) in this unit of work.
    /// </summary>
    IEnumerable<object> Updated { get; }

    /// <summary>
    ///     Documents that were inserted in this unit of work.
    /// </summary>
    IEnumerable<object> Inserted { get; }

    /// <summary>
    ///     Documents that were deleted in this unit of work.
    /// </summary>
    IEnumerable<IDeletion> Deleted { get; }

    /// <summary>
    ///     #485 / jasperfx#679: the shared contract's view of this change set, so a consumer that
    ///     registered an <see cref="IDocumentCommitListener" /> can read what a commit wrote without
    ///     naming a Polecat type.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Explicit, and an adapter rather than a rename, for the reason
    ///         <see cref="IQuerySession" />'s <c>Events</c> member spells out: C# interface
    ///         implementation is not covariant in a member's type, so the three properties above do
    ///         not satisfy the contract's. <c>IEnumerable&lt;object&gt; Inserted</c> is not
    ///         <c>IReadOnlyList&lt;object&gt; Inserted</c>, and <c>IEnumerable&lt;IDeletion&gt;
    ///         Deleted</c> is not <c>IReadOnlyList&lt;IDocumentDeletion&gt; Deleted</c> even though
    ///         <see cref="IDeletion" /> and <see cref="IDocumentDeletion" /> declare the same pair.
    ///     </para>
    ///     <para>
    ///         ⚠️ It differs from the jasperfx#669 <c>Events</c> case in exactly one way, and it is
    ///         the way that matters: <see cref="IDocumentChangeSet" /> carries NO default
    ///         implementations, so omitting these three is CS0535 at build time rather than a member
    ///         silently bound to a throwing default. The silent failure for #485 lives in the
    ///         <em>wiring</em> instead — see <c>DocumentSessionBase.SaveChangesAsync</c>.
    ///     </para>
    ///     <para>
    ///         Declared here as default interface members rather than on the implementing classes so
    ///         that adding a base interface to a PUBLIC interface is not a breaking change for an
    ///         outside implementor. Polecat's own two implementations
    ///         (<c>Polecat.Services.ChangeSet</c> and <c>Polecat.Internal.WorkTracker</c>) override
    ///         them with memoized fields, because these defaults re-materialise the lazy LINQ chains
    ///         on every access and <c>SaveChangesAsync</c> is a hot path.
    ///     </para>
    ///     <para>
    ///         The contract requires SNAPSHOTS — <see cref="IReadOnlyList{T}" /> and not
    ///         <see cref="IEnumerable{T}" /> — because a listener may retain the change set past the
    ///         commit boundary. <c>.ToList()</c> here is what makes that true of a live change set
    ///         such as an <see cref="IWorkTracker" />, which is otherwise a view over a unit of work
    ///         that is <c>Reset()</c> immediately after the listener loop runs.
    ///     </para>
    /// </remarks>
    IReadOnlyList<object> IDocumentChangeSet.Inserted => Inserted as IReadOnlyList<object> ?? Inserted.ToList();

    /// <inheritdoc cref="IDocumentChangeSet.Inserted" />
    IReadOnlyList<object> IDocumentChangeSet.Updated => Updated as IReadOnlyList<object> ?? Updated.ToList();

    /// <inheritdoc cref="IDocumentChangeSet.Inserted" />
    IReadOnlyList<IDocumentDeletion> IDocumentChangeSet.Deleted =>
        Deleted as IReadOnlyList<IDocumentDeletion> ?? Deleted.Cast<IDocumentDeletion>().ToList();

    /// <summary>
    ///     All events appended across every stream in this unit of work.
    /// </summary>
    IEnumerable<IEvent> GetEvents();

    /// <summary>
    ///     All stream actions (starts/appends) in this unit of work.
    /// </summary>
    IEnumerable<StreamAction> GetStreams();

    /// <summary>
    ///     Produce an immutable copy of this change set. Callers that retain the change set beyond the
    ///     commit boundary must clone it, because the live unit of work is reset after each commit.
    /// </summary>
    IChangeSet Clone();
}

/// <summary>
///     Describes a single document deletion within an <see cref="IChangeSet" />.
/// </summary>
/// <remarks>
///     #485 / jasperfx#679: derives from the shared <see cref="IDocumentDeletion" />, which declares
///     the identical pair. The inheritance is what lets a Polecat <see cref="IDeletion" /> instance
///     be handed straight to an <see cref="IDocumentCommitListener" /> — but note it does NOT make
///     <c>IEnumerable&lt;IDeletion&gt;</c> satisfy the contract's
///     <c>IReadOnlyList&lt;IDocumentDeletion&gt;</c>; see the adapter above.
/// </remarks>
public interface IDeletion : IDocumentDeletion
{
    /// <summary>
    ///     The .NET type of the deleted document.
    /// </summary>
    Type DocumentType { get; }

    /// <summary>
    ///     The identity of the deleted document, when the deletion targeted a single document by id.
    ///     Null for predicate-based (delete-where) operations.
    /// </summary>
    object? Id { get; }
}
