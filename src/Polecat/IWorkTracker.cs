using JasperFx.Events;
using Polecat.Internal;
using Polecat.Services;

namespace Polecat;

/// <summary>
///     Public read-only view of pending operations in a document session. Also exposes the pending
///     work as an <see cref="IChangeSet" /> (Inserted/Updated/Deleted/events), mirroring Marten's
///     <c>ISessionWorkTracker : IUnitOfWork, IChangeSet</c>.
/// </summary>
public interface IWorkTracker : IChangeSet
{
    /// <summary>
    ///     All pending storage operations.
    /// </summary>
    IReadOnlyList<IStorageOperation> Operations { get; }

    /// <summary>
    ///     All pending stream actions (event appends/starts).
    /// </summary>
    IReadOnlyList<StreamAction> Streams { get; }

    /// <summary>
    ///     Whether there are any pending operations or stream actions.
    /// </summary>
    bool HasOutstandingWork();
}
