namespace Polecat.Internal.Operations;

/// <summary>
///     Flush-time dispatch over the shared operation currency (#273 E2e). Bespoke Polecat
///     operations — including the closed-shape adapter, which forwards to its captured
///     (possibly tenant-scoped) session — configure through their one-arg entry point; raw
///     shared operations need the executing session handed in. The async daemon's batch
///     flush aggregates operations from multiple tenant sessions and has no single session
///     in scope, so it passes null and relies on every queued operation carrying its own
///     session context.
/// </summary>
internal static class StorageOperationExecution
{
    internal static void Configure(Weasel.Storage.IStorageOperation op, ICommandBuilder builder,
        Weasel.Storage.IStorageSession? session)
    {
        // polecat#517: materialize the batch command BEFORE the operation appends anything.
        //
        // Weasel's BatchBuilder only creates the underlying SqlBatchCommand as a side effect of
        // appending a PARAMETER — AppendWithParameters writes its text straight into the shared
        // StringBuilder and creates nothing when the statement has no placeholders. An operation
        // that appends no parameters therefore leaves the builder with no current command, and the
        // caller's next StartNewCommand() hits `if (_current != null)`, skips saving the text, and
        // then _builder.Clear()s it away. The statement is silently discarded: no exception, no log,
        // and the rest of the batch commits.
        //
        // QueueSqlCommand with a parameterless statement is the reported case — it worked alone
        // (Compile() creates a command when the batch is empty) and vanished the moment any other
        // operation was queued behind it. Append(string) does `_current ??= appendCommand()`, so an
        // empty append is enough to pin the command down. Harmless for every other operation: the
        // caller has already called StartNewCommand() for i > 0, which leaves _current non-null.
        builder.Append(string.Empty);

        if (op is IStorageOperation bespoke)
        {
            bespoke.ConfigureCommand(builder);
            return;
        }

        op.ConfigureCommand(builder, session ?? throw new InvalidOperationException(
            $"Operation {op.GetType().FullName} requires an executing session, but this execution " +
            "path (the async daemon batch) has none — queue it through a session-bound adapter."));
    }
}
