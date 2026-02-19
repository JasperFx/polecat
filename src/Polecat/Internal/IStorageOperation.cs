using Microsoft.Data.SqlClient;

namespace Polecat.Internal;

/// <summary>
///     The role of a storage operation in the unit of work.
/// </summary>
public enum OperationRole
{
    Upsert,
    Insert,
    Update,
    Delete,
    Patch
}

/// <summary>
///     Represents a single storage operation that can be executed against the database.
/// </summary>
public interface IStorageOperation
{
    /// <summary>
    ///     The document type this operation acts on.
    /// </summary>
    Type DocumentType { get; }

    /// <summary>
    ///     The role of this operation.
    /// </summary>
    OperationRole Role { get; }

    /// <summary>
    ///     The document ID this operation targets, if applicable.
    ///     Null for bulk/where operations.
    /// </summary>
    object? DocumentId => null;

    /// <summary>
    ///     Configure the SqlCommand for execution.
    /// </summary>
    void ConfigureCommand(SqlCommand command);

    /// <summary>
    ///     Process results after execution (e.g., capture OUTPUT version).
    /// </summary>
    Task PostprocessAsync(SqlCommand command, CancellationToken token);
}
