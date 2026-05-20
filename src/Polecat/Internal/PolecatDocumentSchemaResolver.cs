using JasperFx.Events;

namespace Polecat.Internal;

/// <summary>
///     Polecat's SQL Server implementation of the lifted
///     <see cref="IDocumentSchemaResolver"/> (jasperfx#333) — the single
///     "where does this document live" surface for schema inspection,
///     diagnostics, and projection-coordinator activity tags.
/// </summary>
/// <remarks>
///     Polecat stores documents in <c>pc_doc_{typename}</c> and the event-store
///     tables in <c>pc_events</c> / <c>pc_streams</c> / <c>pc_event_progression</c>,
///     all under the single <see cref="StoreOptions.DatabaseSchemaName"/> (events
///     share the document schema). A qualified name is bracket-quoted
///     <c>[schema].[table]</c>; a bare name is the unbracketed table identifier,
///     matching <c>DocumentMapping.TableName</c>.
/// </remarks>
internal sealed class PolecatDocumentSchemaResolver : IDocumentSchemaResolver
{
    private readonly StoreOptions _options;

    public PolecatDocumentSchemaResolver(StoreOptions options)
    {
        _options = options;
    }

    public string DatabaseSchemaName => _options.DatabaseSchemaName;

    // Events share the document schema in Polecat (the pc_ prefix distinguishes them).
    public string EventsSchemaName => _options.DatabaseSchemaName;

    public string For<TDocument>(bool qualified = true) => For(typeof(TDocument), qualified);

    public string For(Type documentType, bool qualified = true)
        => Format($"pc_doc_{documentType.Name.ToLowerInvariant()}", qualified);

    public string ForEvents(bool qualified = true) => Format("pc_events", qualified);

    public string ForStreams(bool qualified = true) => Format("pc_streams", qualified);

    public string ForEventProgression(bool qualified = true) => Format("pc_event_progression", qualified);

    private string Format(string table, bool qualified)
        => qualified ? $"[{_options.DatabaseSchemaName}].[{table}]" : table;
}
