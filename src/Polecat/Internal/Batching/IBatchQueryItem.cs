using System.Data.Common;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

/// <summary>
///     A single item in a batched query. Writes its SQL into a shared ICommandBuilder
///     and reads its result set when Execute() processes the reader.
/// </summary>
internal interface IBatchQueryItem
{
    void WriteSql(ICommandBuilder builder);
    Task ReadResultSetAsync(DbDataReader reader, CancellationToken token);

    /// <summary>
    ///     #849: <c>Execute</c> failed, so this item's task has to finish some other way. A no-op on an
    ///     item that already read its result.
    /// </summary>
    void Fail(Exception failure);
}

internal static class BatchQueryItemExtensions
{
    /// <summary>
    ///     Faults the task, or cancels it when that is what happened to the batch.
    /// </summary>
    /// <remarks>
    ///     Reading <see cref="Task.Exception" /> marks the fault as observed. <c>Execute</c> has already
    ///     reported this failure, and a caller who catches it has no reason to touch the items, so
    ///     without this each one would come back through
    ///     <see cref="TaskScheduler.UnobservedTaskException" /> at finalization, which a host setting
    ///     <c>ThrowUnobservedTaskExceptions</c> treats as fatal.
    /// </remarks>
    public static void FailWith<T>(this TaskCompletionSource<T> completion, Exception failure)
    {
        if (failure is OperationCanceledException cancelled)
        {
            completion.TrySetCanceled(cancelled.CancellationToken);
            return;
        }

        if (completion.TrySetException(failure))
        {
            _ = completion.Task.Exception;
        }
    }
}
