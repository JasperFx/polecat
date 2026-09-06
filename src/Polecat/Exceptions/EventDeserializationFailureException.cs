namespace Polecat.Exceptions;

/// <summary>
///     Thrown when Polecat cannot deserialize a persisted event body out of <c>pc_events</c>.
/// </summary>
/// <remarks>
///     <para>
///         #368 / jasperfx#565: this exception declares its own
///         <see cref="JasperFx.Events.Daemon.ShardFailureCategory" /> through
///         <see cref="JasperFx.Events.Daemon.IEventFailureContext" />, which is how a paused shard
///         reports <em>why</em> it is down. The daemon deliberately has no fallback — it never sniffs
///         a store's exception type names — so without this a corrupted event body classified as
///         <see cref="JasperFx.Events.Daemon.ShardFailureCategory.Other" /> with no event details at
///         all.
///     </para>
///     <para>
///         jasperfx#751 lifted the whole shape into
///         <see cref="JasperFx.Events.EventDeserializationFailureException" />, which also carries the
///         <c>ToDeadLetterEvent</c> helper. Polecat keeps the name in its own namespace so existing
///         <c>catch</c> sites go on compiling, and derives from the shared type so a store-agnostic
///         caller catches it too.
///     </para>
/// </remarks>
public class EventDeserializationFailureException : JasperFx.Events.EventDeserializationFailureException
{
    public EventDeserializationFailureException(long sequence, string? eventTypeName, Exception innerException)
        : base(sequence, eventTypeName, innerException)
    {
    }
}
