using Microsoft.Data.SqlClient;

namespace Polecat.Internal;

/// <summary>
///     Classifies a <see cref="SqlException" /> as "the storage this read needs is not there",
///     which is how every diagnostic read of the event-store tables decides to answer "no results"
///     instead of throwing. polecat#677, porting marten#5512.
/// </summary>
/// <remarks>
///     <para>
///         Two error numbers, and both are reachable. <b>208</b> is a missing table, which is what
///         a store on a fresh database under <c>AutoCreate.None</c> raises: being in the migration
///         set does not help if nothing ever applies it. <b>207</b> is a missing COLUMN, which is
///         what a <c>pc_event_progression</c> created before <c>EnableExtendedProgressionTracking</c>
///         raises the moment a progression read selects <c>heartbeat</c>, <c>agent_status</c> and
///         friends. The second is the one that shipped a production failure in Marten
///         (marten#5509), and a guard that handles only 208 looks complete and misses it.
///     </para>
///     <para>
///         The decision is made by asking the DATABASE, not by inspecting configuration. Drift is
///         invisible to configuration: a store can declare extended progression tracking, have every
///         table in its migration set, and still be pointed at a schema where neither is true. That
///         is the explicit lesson of marten#5512, whose configuration-based predecessor was both too
///         narrow (it reported nothing for a diagnostics-shaped store pointed at a fully provisioned
///         event store) and unstable (it flipped mid-process as event types registered).
///     </para>
/// </remarks>
internal static class MissingStorageDetection
{
    /// <summary>SQL Server error 208: "Invalid object name '%s'."</summary>
    public const int InvalidObjectName = 208;

    /// <summary>SQL Server error 207: "Invalid column name '%s'."</summary>
    public const int InvalidColumnName = 207;

    /// <summary>
    ///     True when <paramref name="exception" /> reports a missing table.
    /// </summary>
    public static bool IsUndefinedTable(Exception exception) =>
        Matches(exception, static number => number == InvalidObjectName);

    /// <summary>
    ///     True when <paramref name="exception" /> reports a missing table or a missing column —
    ///     the two ways the storage behind a diagnostic read turns out not to be there.
    /// </summary>
    public static bool IsMissingStorage(Exception exception) =>
        Matches(exception,
            static number => number is InvalidObjectName or InvalidColumnName);

    /// <summary>
    ///     Walks the exception graph rather than testing the caught exception alone. Polecat has no
    ///     <c>MartenCommandException</c> analogue, so the <see cref="SqlException" /> is usually the
    ///     outer exception — but the batched-command path raises
    ///     <see cref="AggregateException" /> (see <c>Internal/DocumentSessionBase</c>), and Polly can
    ///     surface an inner exception of its own, so neither shape can be assumed.
    /// </summary>
    private static bool Matches(Exception? exception, Func<int, bool> predicate)
    {
        while (exception != null)
        {
            if (exception is SqlException sql)
            {
                // A batch can report several errors; any one of them being the missing-storage error
                // is enough, because the read cannot succeed either way.
                foreach (SqlError error in sql.Errors)
                {
                    if (predicate(error.Number)) return true;
                }

                if (predicate(sql.Number)) return true;
            }

            if (exception is AggregateException aggregate)
            {
                foreach (var inner in aggregate.Flatten().InnerExceptions)
                {
                    if (Matches(inner, predicate)) return true;
                }

                return false;
            }

            exception = exception.InnerException;
        }

        return false;
    }
}
