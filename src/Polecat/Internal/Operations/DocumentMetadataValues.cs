using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

/// <summary>
///     #241: a snapshot of the session-level metadata values to persist into the opt-in document
///     metadata columns (correlation_id / causation_id / last_modified_by / headers) at the moment a
///     document operation is queued. Headers are pre-serialized to JSON by the session (which owns
///     the serializer).
/// </summary>
internal readonly struct DocumentMetadataValues
{
    public string? CorrelationId { get; init; }
    public string? CausationId { get; init; }
    public string? LastModifiedBy { get; init; }
    public string? HeadersJson { get; init; }

    /// <summary>
    ///     Add a parameter for every enabled opt-in metadata column on the mapping, drawing the value
    ///     from this snapshot. Parameter names match the column names (correlation_id, etc.).
    /// </summary>
    public void AddParameters(ICommandBuilder builder, DocumentMapping mapping)
    {
        foreach (var column in mapping.EnabledMetadataColumns)
        {
            object? value = column.Name switch
            {
                "correlation_id" => CorrelationId,
                "causation_id" => CausationId,
                "last_modified_by" => LastModifiedBy,
                "headers" => HeadersJson,
                _ => null
            };

            builder.AddParameters(new Dictionary<string, object?> { [column.Name] = value ?? (object)DBNull.Value });
        }
    }
}
