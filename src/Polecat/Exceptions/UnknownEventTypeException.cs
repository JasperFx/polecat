using JasperFx.Events.Daemon;

namespace Polecat.Exceptions;

/// <summary>
///     Thrown when an event's persisted <c>dotnet_type</c> resolves to no known .NET type in this
///     deployment.
/// </summary>
/// <remarks>
///     #368 / jasperfx#565: kept deliberately distinct from
///     <see cref="ShardFailureCategory.EventSerialization" />. An alias that resolves to nothing is
///     normally a missing registration or a rollback past the event type's introduction — a deployment
///     fix, not a data fix — so an operator responds to it differently.
/// </remarks>
public class UnknownEventTypeException : Exception, IEventFailureContext
{
    /// <summary>
    ///     The sequence reported when the throw site had no <c>pc_events</c> row in hand.
    ///     <see cref="IEventFailureContext.Sequence" /> is non-nullable by contract, so a sentinel is
    ///     unavoidable.
    /// </summary>
    public const long UnknownSequence = -1;

    public UnknownEventTypeException(string? eventTypeName)
        : this(eventTypeName, UnknownSequence)
    {
    }

    public UnknownEventTypeException(string? eventTypeName, long sequence)
        : base(
            $"Unknown event type name alias '{eventTypeName}'. You may need to register this event type through StoreOptions.Events.AddEventType(type)")
    {
        EventTypeName = eventTypeName;
        Sequence = sequence;
    }

    /// <summary>
    ///     Store-wide <c>seq_id</c> of the offending row, or <see cref="UnknownSequence" /> when the throw
    ///     site had no row.
    /// </summary>
    public long Sequence { get; }

    /// <summary>
    ///     The unresolvable type name from the row.
    /// </summary>
    public string? EventTypeName { get; }

    public ShardFailureCategory Category => ShardFailureCategory.UnknownEventType;

    // The type never resolved, so no event was ever materialized to read these from.
    Guid? IEventFailureContext.EventId => null;
    Guid? IEventFailureContext.StreamId => null;
    string? IEventFailureContext.StreamKey => null;
    string? IEventFailureContext.TenantId => null;
    long? IEventFailureContext.Version => null;
}
