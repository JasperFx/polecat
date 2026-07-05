using JasperFx.Events;

namespace Polecat.Services;

/// <summary>
///     A snapshot of the document and event changes committed in a single unit of work — either a
///     user <see cref="IDocumentSession.SaveChangesAsync" /> or an async daemon projection batch.
///     Handed to <see cref="IChangeListener" /> and <see cref="IDocumentSessionListener" /> so
///     post-commit side effects (cache invalidation, messaging, etc.) can see exactly what changed.
///     Mirrors Marten's <c>Marten.Services.IChangeSet</c>.
/// </summary>
public interface IChangeSet
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
public interface IDeletion
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
