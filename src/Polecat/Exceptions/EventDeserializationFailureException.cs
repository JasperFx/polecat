using JasperFx.Events.Daemon;

namespace Polecat.Exceptions;

/// <summary>
///     Thrown when Polecat cannot deserialize a persisted event body out of <c>pc_events</c>.
/// </summary>
/// <remarks>
///     #368 / jasperfx#565: this exception declares its own <see cref="ShardFailureCategory" /> through
///     <see cref="IEventFailureContext" />, which is how a paused shard reports <em>why</em> it is down.
///     The daemon deliberately has no fallback — it never sniffs a store's exception type names — so
///     without this a corrupted event body classified as <see cref="ShardFailureCategory.Other" /> with
///     no event details at all.
/// </remarks>
public class EventDeserializationFailureException : Exception, IEventFailureContext
{
    public EventDeserializationFailureException(long sequence, string? eventTypeName, Exception innerException)
        : base($"Event deserialization error on sequence = {sequence} for event type {eventTypeName}",
            innerException)
    {
        Sequence = sequence;
        EventTypeName = eventTypeName;
    }

    /// <summary>
    ///     Store-wide <c>seq_id</c> of the event whose body could not be read.
    /// </summary>
    public long Sequence { get; }

    /// <summary>
    ///     The event store's type alias for the failing event (the <c>type</c> column, e.g.
    ///     <c>trip_started</c>), when the row supplied one. Retained as data rather than being buried in
    ///     the message string so the daemon can report it on <see cref="ShardFailure" />.
    /// </summary>
    public string? EventTypeName { get; }

    /// <summary>
    ///     A body Polecat could not deserialize is a serializer or data problem — governed by
    ///     <c>SkipSerializationErrors</c> — which is a different operator action from an unregistered
    ///     event type. See <see cref="UnknownEventTypeException" />.
    /// </summary>
    public ShardFailureCategory Category => ShardFailureCategory.EventSerialization;

    // Everything below is raised while reading a pc_events row, BEFORE there is an IEvent to inspect, so
    // nothing but the sequence and the stored type alias is knowable here. IEventFailureContext makes
    // every one of these nullable for exactly this case.
    Guid? IEventFailureContext.EventId => null;
    Guid? IEventFailureContext.StreamId => null;
    string? IEventFailureContext.StreamKey => null;
    string? IEventFailureContext.TenantId => null;
    long? IEventFailureContext.Version => null;
}
