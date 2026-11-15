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

        transformed = null;
        return false;
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
