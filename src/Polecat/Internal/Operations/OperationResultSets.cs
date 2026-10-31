using System.Data.Common;
using Weasel.Storage;

namespace Polecat.Internal.Operations;

/// <summary>
///     The one walk over a batch's result sets (#831). Every flush path — a session's document batch,
///     its event batch, the async daemon's projection batch and bulk insert — goes through
///     <see cref="ApplyAsync" /> so they cannot drift apart again.
/// </summary>
/// <remarks>
///     <para>
///         <b>A batch does not have one result set per operation.</b> <c>SqlBatch</c> surfaces only
///         real result sets, so a command that is pure DML contributes none and the reader moves
///         straight past it, while every document write contributes exactly one through its
///         <c>OUTPUT</c> (or the full-text variant's trailing <c>SELECT</c>) — including the write a
///         guard refused, whose set is simply empty. The loops used to call <c>NextResultAsync</c>
///         once per operation regardless. With <c>[DML, VersionedUpsert]</c> the reader starts on the
///         upsert's set, the DML's turn stepped past it, and the upsert then read nothing, which is its
///         conflict signal. A real conflict could vanish the same way: the stale write's
///         <c>ReadAsync</c> landed on the next operation's row and reported success.
///     </para>
///     <para>
///         So alignment comes from what each operation declares: <see cref="NoDataReturnedCall" />
///         means no result set and no turn in the walk, and anything else owns exactly one.
///         <c>operation_result_set_census_tests</c> holds that declaration against what SQL Server
///         actually returns for every operation shape, and sweeps the assembly so a new operation
///         cannot arrive undeclared.
///     </para>
///     <para>
///         A <see cref="NoDataReturnedCall" /> operation's <c>PostprocessAsync</c> is not called: the
///         reader is on some other operation's result set at that moment, so there is nothing there
///         that belongs to it. The same rule as Marten's <c>OperationPage</c>.
///     </para>
/// </remarks>
internal static class OperationResultSets
{
    /// <summary>
    ///     Does this operation's command contribute a result set? Looks through the adapters, which
    ///     wrap both kinds and so cannot carry the marker themselves.
    /// </summary>
    internal static bool ReturnsResultSet(Weasel.Storage.IStorageOperation operation)
        => (operation is IStorageOperationAdapter adapter ? adapter.Inner : operation) is not NoDataReturnedCall;

    /// <summary>
    ///     Hand each result-returning operation its own result set, in batch order, then drain the
    ///     reader. <paramref name="operations" /> must be in the order their commands were configured.
    /// </summary>
    internal static async Task ApplyAsync(DbDataReader reader,
        IReadOnlyList<Weasel.Storage.IStorageOperation> operations, IList<Exception> exceptions,
        CancellationToken token)
    {
        // The reader opens on the first result set in the batch, wherever in the batch that is.
        var onOwnResultSet = true;
        var anOperationOwnsAResultSet = false;

        for (var i = 0; i < operations.Count; i++)
        {
            var operation = operations[i];
            if (!ReturnsResultSet(operation)) continue;

            if (!onOwnResultSet)
            {
                await reader.NextResultAsync(token).ConfigureAwait(false);
            }

            await operation.PostprocessAsync(reader, exceptions, token).ConfigureAwait(false);
            onOwnResultSet = false;
            anOperationOwnsAResultSet = true;
        }

        // #273 E2b: drain the reader past every remaining result set. SQL Server surfaces a
        // per-command batch error (unique index 2601, FK 547, ...) only when the reader ADVANCES over
        // that command. The walk above stops ON the last result set any operation owns, so without
        // this a failed command at or behind that point would sit unconsumed, be swallowed by
        // DisposeAsync, and the transaction would commit around it.
        var surplusResultSets = 0;
        while (await reader.NextResultAsync(token).ConfigureAwait(false))
        {
            surplusResultSets++;
        }

        // A set nobody declared came from a statement queued with QueueSqlCommand, which is what
        // pushes every later write onto the wrong set. A stale write then reads somebody else's row
        // and reports success, so this has to be loud, and it throws inside the transaction so the
        // batch rolls back. Only after the drain, because a SQL error in the batch is the better
        // answer. A batch where no operation reads a result set has nothing to misread, so it passes.
        if (anOperationOwnsAResultSet && surplusResultSets > 0)
        {
            throw new InvalidOperationException(
                $"This batch returned {surplusResultSets} more result set(s) than its operations declare, so a "
                + "concurrency check may have read another statement's rows. A statement queued with "
                + "QueueSqlCommand must not return a result set: remove its SELECT or OUTPUT clause, or run it "
                + "outside the session.");
        }
    }
}

/// <summary>
///     An operation that wraps another. Whether a result set comes back is a fact about the wrapped
///     operation's SQL, so <see cref="OperationResultSets.ReturnsResultSet" /> has to ask that one.
/// </summary>
internal interface IStorageOperationAdapter
{
    Weasel.Storage.IStorageOperation Inner { get; }
}
