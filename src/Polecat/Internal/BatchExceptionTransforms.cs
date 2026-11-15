using JasperFx.Core.Exceptions;
using Microsoft.Data.SqlClient;
using Polecat.Internal.Operations;
using Weasel.Storage;

namespace Polecat.Internal;

/// <summary>
///     #781: gives the operation whose batch command actually failed the chance to name a SQL
///     error. Asking every queued operation in turn let the FIRST one that recognised the error
///     number claim it — so a save that started a new stream and an existing one reported the
///     collision against the new stream, because every insert-mode append maps 2627.
/// </summary>
/// <remarks>
///     <para>
///         #849: the application's <see cref="StoreOptions.ExceptionTransforms" /> come after the
///         operations, which know the most, and before anything the caller does with the error next.
///     </para>
///     <para>
///         #807: a session's flush gets all of this from Weasel's <see cref="OperationBatchExecutor" />
///         (which lifted #781) and <see cref="PolecatOperationBatchListener" />. The async daemon's
///         projection batch is the one caller left, because it only takes the executor's walk; see the
///         note in <c>PolecatProjectionBatch</c>. The per-operation half is Weasel's
///         <see cref="OperationBatchExecutor.TryTransform(Weasel.Storage.IStorageOperation, Exception, out Exception)" />,
///         so an adapter is looked through the same way on both paths.
///     </para>
/// </remarks>
internal static class BatchExceptionTransforms
{
    public static bool TryTransform(SqlException ex, SqlBatch batch, IReadOnlyList<Weasel.Storage.IStorageOperation> operations,
        ExceptionTransforms application, out Exception? transformed)
    {
        if (TryOperations(ex, batch, operations, out var mapped))
        {
            transformed = mapped;
            return true;
        }

        return PolecatOperationBatchListener.TryApplication(ex, application, out transformed);
    }

    private static bool TryOperations(SqlException ex, SqlBatch batch, IReadOnlyList<Weasel.Storage.IStorageOperation> operations,
        out Exception transformed)
    {
        // Each operation is configured into its own batch command (StartNewCommand between them),
        // so the failing command's index is the failing operation's index. If that ever stops being
        // one-to-one, or the driver does not say which command failed, fall back to asking them all.
        if (ex.BatchCommand is { } failed && batch.BatchCommands.Count == operations.Count)
        {
            for (var i = 0; i < batch.BatchCommands.Count; i++)
            {
                if (ReferenceEquals(batch.BatchCommands[i], failed))
                {
                    return OperationBatchExecutor.TryTransform(operations[i], ex, out transformed);
                }
            }
        }

        return OperationBatchExecutor.TryTransform(operations, ex, out transformed);
    }
}
