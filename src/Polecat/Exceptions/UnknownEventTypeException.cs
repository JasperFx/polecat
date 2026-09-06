namespace Polecat.Exceptions;

/// <summary>
///     Thrown when an event's persisted <c>dotnet_type</c> resolves to no known .NET type in this
///     deployment.
/// </summary>
/// <remarks>
///     <para>
///         #368 / jasperfx#565: kept deliberately distinct from
///         <see cref="JasperFx.Events.Daemon.ShardFailureCategory.EventSerialization" />. An alias
///         that resolves to nothing is normally a missing registration or a rollback past the event
///         type's introduction — a deployment fix, not a data fix — so an operator responds to it
///         differently.
///     </para>
///     <para>
///         jasperfx#751 lifted the whole shape, <see cref="JasperFx.Events.Daemon.IEventFailureContext" />
///         implementation included, into <see cref="JasperFx.Events.UnknownEventTypeException" />.
///         Polecat keeps the name in its own namespace so existing <c>catch</c> sites go on
///         compiling, and derives from the shared type so a store-agnostic caller catches it too.
///     </para>
/// </remarks>
public class UnknownEventTypeException : JasperFx.Events.UnknownEventTypeException
{
    public UnknownEventTypeException(string? eventTypeName) : base(eventTypeName)
    {
    }

    public UnknownEventTypeException(string? eventTypeName, long sequence) : base(eventTypeName, sequence)
    {
    }
}
