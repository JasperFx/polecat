using JasperFx.Core.Exceptions;
using Microsoft.Data.SqlClient;
using Weasel.Storage;

namespace Polecat.Internal.Operations;

/// <summary>
///     What Polecat adds to Weasel's <see cref="OperationBatchExecutor" /> when a session flushes a batch
///     (#807, weasel#810): the application's <see cref="StoreOptions.ExceptionTransforms" />, and the
///     Polecat-specific wording of the surplus-result-set refusal.
/// </summary>
/// <remarks>
///     <para>
///         The executor offers a failure to the operations first — the one whose batch command failed,
///         when SQL Server names it (#781), looking through <see cref="IStorageOperationWrapper" />
///         adapters — and only then to this listener. That is the order the session flush has always
///         used: #849 put the application's transforms after the operations, which know the most, and
///         before Polecat's own mapping of 2627 to <c>DocumentAlreadyExistsException</c>, which the flush
///         still does with whatever comes out.
///     </para>
///     <para>
///         Application transforms are only ever offered a <see cref="SqlException" />, as before. The
///         executor would offer them anything thrown while the batch ran.
///     </para>
/// </remarks>
internal sealed class PolecatOperationBatchListener : IOperationBatchListener
{
    private readonly ExceptionTransforms _application;

    public PolecatOperationBatchListener(ExceptionTransforms application)
    {
        _application = application;
    }

    public bool TryTransform(OperationBatchCommand command, Exception exception, out Exception? transformed)
    {
        // #831: Weasel's message is about the NoDataReturnedCall marker, which an application never sees.
        // The way an application gets here is a QueueSqlCommand statement with a SELECT or OUTPUT clause,
        // so say that.
        if (exception is SurplusResultSetsException surplus)
        {
            transformed = new InvalidOperationException(
                $"This batch returned {surplus.SurplusResultSets} more result set(s) than its operations declare, "
                + "so a concurrency check may have read another statement's rows. A statement queued with "
                + "QueueSqlCommand must not return a result set: remove its SELECT or OUTPUT clause, or run it "
                + "outside the session.", surplus);
            return true;
        }

        if (exception is SqlException sql && TryApplication(sql, _application, out transformed))
        {
            return true;
        }

        if (exception is SqlException { Number: Exceptions.KeyTooLongException.ErrorNumber } tooLong)
        {
            transformed = KeyTooLong(tooLong, command.Operations, command.Batch);
            return true;
        }

        transformed = null;
        return false;
    }

    /// <summary>
    ///     #935: Msg 1946, a row whose key passed the 900 or 1,700 bytes an index holds, which only
    ///     <see cref="AdvancedOptions.UseUnicodeKeys" /> makes possible. Named by the write it came from, when
    ///     SQL Server says which batch command failed and that command is one operation's.
    /// </summary>
    internal static Exceptions.KeyTooLongException KeyTooLong(SqlException exception,
        IReadOnlyList<Weasel.Storage.IStorageOperation> operations, System.Data.Common.DbBatch? batch)
    {
        Weasel.Storage.IStorageOperation? failed = operations.Count == 1 ? operations[0] : null;
        if (failed == null && exception.BatchCommand is { } command && batch != null
            && batch.BatchCommands.Count == operations.Count)
        {
            for (var i = 0; i < batch.BatchCommands.Count; i++)
            {
                if (ReferenceEquals(batch.BatchCommands[i], command)) failed = operations[i];
            }
        }

        // An adapter carries the key itself, and otherwise the operation it wraps may
        for (var depth = 0; failed != null && depth < 8; depth++)
        {
            if (failed is IKeyedWrite keyed)
            {
                return new Exceptions.KeyTooLongException(keyed.TenantId, keyed.Key, exception);
            }

            failed = (failed as IStorageOperationWrapper)?.Inner;
        }

        return new Exceptions.KeyTooLongException(null, null, exception);
    }

    internal static bool TryApplication(SqlException exception, ExceptionTransforms application,
        out Exception? transformed)
    {
        foreach (var transform in application)
        {
            if (transform.TryTransform(exception, out transformed) && transformed != null)
            {
                return true;
            }
        }

        transformed = null;
        return false;
    }
}
