namespace Polecat;

/// <summary>
///     Controls the behavior of bulk insert operations.
/// </summary>
public enum BulkInsertMode
{
    /// <summary>
    ///     Fast INSERT — throws on duplicate primary key.
    /// </summary>
    InsertsOnly,

    /// <summary>
    ///     MERGE — skip existing rows (do not update).
    /// </summary>
    IgnoreDuplicates,

    /// <summary>
    ///     MERGE — update existing rows with new data.
    /// </summary>
    OverwriteExisting
}
