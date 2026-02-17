namespace Polecat.Events;

/// <summary>
///     Metadata about an event stream.
/// </summary>
public class StreamState
{
    public StreamState()
    {
    }

    public StreamState(Guid id, long version, Type? aggregateType,
        DateTimeOffset lastTimestamp, DateTimeOffset created)
    {
        Id = id;
        Version = version;
        AggregateType = aggregateType;
        LastTimestamp = lastTimestamp;
        Created = created;
    }

    public StreamState(string key, long version, Type? aggregateType,
        DateTimeOffset lastTimestamp, DateTimeOffset created)
    {
        Key = key;
        Version = version;
        AggregateType = aggregateType;
        LastTimestamp = lastTimestamp;
        Created = created;
    }

    public Guid Id { get; set; }
    public string? Key { get; set; }
    public long Version { get; set; }
    public Type? AggregateType { get; set; }
    public DateTimeOffset LastTimestamp { get; set; }
    public DateTimeOffset Created { get; set; }
    public bool IsArchived { get; set; }
}
