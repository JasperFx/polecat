using JasperFx.Core.Exceptions;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal;

/// <summary>
///     #781: gives the operation whose batch command actually failed the chance to name a SQL
///     error. Asking every queued operation in turn let the FIRST one that recognised the error
///     number claim it — so a save that started a new stream and an existing one reported the
///     collision against the new stream, because every insert-mode append maps 2627.
/// </summary>
/// <remarks>
///     #849: the application's <see cref="StoreOptions.ExceptionTransforms" /> come after the
///     operations, which know the most, and before anything the caller does with the error next.
/// </remarks>
internal static class BatchExceptionTransforms
{
    public static bool TryTransform<T>(SqlException ex, SqlBatch batch, IReadOnlyList<T> operations,
        ExceptionTransforms application, out Exception? transformed)
    {
        if (TryOperations(ex, batch, operations, out transformed))
        {
            return true;
        }

        foreach (var transform in application)
        {
            if (transform.TryTransform(ex, out transformed) && transformed != null)
            {
                return true;
            }
        }

        transformed = null;
        return false;
    }

    private static bool TryOperations<T>(SqlException ex, SqlBatch batch, IReadOnlyList<T> operations,
        out Exception? transformed)
    {
        // Each operation is configured into its own batch command (StartNewCommand between them),
        // so the failing command's index is the failing operation's index. If that ever stops being
        // one-to-one, or the driver does not say which command failed, fall back to asking them all.
        if (ex.BatchCommand is { } failed && batch.BatchCommands.Count == operations.Count)
        {
            for (var i = 0; i < batch.BatchCommands.Count; i++)
            {
                if (!ReferenceEquals(batch.BatchCommands[i], failed)) continue;

                if (operations[i] is IExceptionTransform owner
                    && owner.TryTransform(ex, out transformed) && transformed != null)
                {
                    return true;
                }

                transformed = null;
                return false;
            }
        }

        foreach (var op in operations)
        {
            if (op is IExceptionTransform transform
                && transform.TryTransform(ex, out transformed) && transformed != null)
            {
                return true;
            }
        }

        transformed = null;
        return false;
    }
}
