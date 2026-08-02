namespace Polecat.Events;

/// <summary>
///     Pluggable binary serializer for event data — Polecat's counterpart of Marten's
///     <c>IEventBinarySerializer</c> (<see href="https://github.com/JasperFx/marten/issues/4515" />,
///     shipped in Marten 9.20.2), at parity so a store-agnostic consumer can wire either flavor.
///     Tracked as <see href="https://github.com/JasperFx/polecat/issues/388">polecat#388</see>.
/// </summary>
/// <remarks>
///     <para>
///         Binary serialization is enabled <strong>per event type</strong>, not store-wide. A store
///         can have JSON events and binary events mixed in the same <c>pc_events</c> table; a row's
///         format is determined by whether its <c>bdata</c> column is <c>NULL</c> (JSON) or not
///         (binary). That is what makes the feature safe to switch on for an existing store with no
///         migration of existing event data — and just as safe to switch back off.
///     </para>
///     <para>
///         Opt in by marking an event type with <see cref="BinaryEventAttribute" /> (resolved against
///         the store-wide <c>opts.Events.DefaultBinarySerializer</c>) or by registering it explicitly
///         with <c>opts.Events.UseBinarySerializer&lt;TEvent&gt;(serializer)</c>. An explicit per-type
///         registration wins over the attribute.
///     </para>
///     <para>
///         Implementations must be thread-safe: one instance serves every session in the store.
///     </para>
/// </remarks>
public interface IEventBinarySerializer
{
    /// <summary>
    ///     Serialize an event data instance to bytes.
    /// </summary>
    /// <param name="type">The runtime CLR type of the event data.</param>
    /// <param name="data">The event data to serialize.</param>
    byte[] Serialize(Type type, object data);

    /// <summary>
    ///     Deserialize bytes back into an event data instance.
    /// </summary>
    /// <param name="type">The target CLR type to deserialize into.</param>
    /// <param name="data">The bytes previously produced by <see cref="Serialize" />.</param>
    object Deserialize(Type type, byte[] data);
}
