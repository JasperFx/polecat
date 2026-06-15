namespace Polecat;

/// <summary>
///     Per-store clean/reset surface, scoped to the owning store's configured schema.
///     Exposed via <see cref="AdvancedOperations.Clean" /> and parallels Marten's
///     <c>Marten.Schema.IDocumentCleaner</c>. Every operation targets only the tables in
///     this store's <see cref="StoreOptions.DatabaseSchemaName" /> — an ancillary store
///     (e.g. <c>AddPolecatStore&lt;T&gt;</c> with its own schema) never touches the host
///     application's data.
/// </summary>
public interface IDocumentCleaner
{
    /// <summary>
    ///     Delete all rows from every document table (<c>pc_doc_*</c>) and any
    ///     <c>FlatTableProjection</c> table in this store's schema. Schema objects are kept.
    /// </summary>
    Task DeleteAllDocumentsAsync(CancellationToken ct = default);

    /// <summary>
    ///     Delete all event and stream data (<c>pc_events</c>, <c>pc_streams</c>,
    ///     <c>pc_event_progression</c>, natural-key and flat-table data) in this store's
    ///     schema. Schema objects are kept.
    /// </summary>
    Task DeleteAllEventDataAsync(CancellationToken ct = default);

    /// <summary>
    ///     Drop all Polecat schema objects (the <c>pc_*</c> tables plus any
    ///     <c>FlatTableProjection</c> tables) in this store's schema. Unlike the delete
    ///     operations, this removes the tables themselves.
    /// </summary>
    Task CompletelyRemoveAllAsync(CancellationToken ct = default);
}
