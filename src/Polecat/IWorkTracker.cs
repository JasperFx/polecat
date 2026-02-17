using JasperFx.Events;
using Polecat.Internal;

namespace Polecat;

/// <summary>
///     Public read-only view of pending operations in a document session.
/// </summary>
public interface IWorkTracker
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
