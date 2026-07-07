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
