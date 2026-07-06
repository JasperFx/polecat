using System.Data.Common;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal;

// #273: converged onto the dialect-neutral Weasel.Storage.IStorageOperation (which itself
// extends Weasel.Core.IStorageOperation), so Polecat operations satisfy the shared closed-shape
// runtime's operation contract. Polecat operations capture all their state at construction and
// only need the SqlServer command builder, so the shared two-arg ConfigureCommand is bridged
// with a default interface method — no change to the ~30 concrete operations.
public interface IStorageOperation : Weasel.Storage.IStorageOperation
{
    object? DocumentId => null;
    void ConfigureCommand(ICommandBuilder builder);

    /// <summary>
    ///     Shared-runtime entry point. Polecat's execution pipeline always hands operations a
    ///     Weasel.SqlServer builder (CommandBuilder / BatchBuilder), so the dialect-neutral
    ///     builder is downcast; the session goes unused because Polecat operations are fully
    ///     bound at construction.
    /// </summary>
    void Weasel.Storage.IStorageOperation.ConfigureCommand(
        Weasel.Core.ICommandBuilder builder, Weasel.Storage.IStorageSession session)
    {
        if (builder is not ICommandBuilder sqlServerBuilder)
        {
            throw new ArgumentException(
                $"Polecat operations require a Weasel.SqlServer command builder; got {builder.GetType().FullName}.",
                nameof(builder));
        }

        ConfigureCommand(sqlServerBuilder);
    }
}
