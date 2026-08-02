namespace Polecat.Events;

/// <summary>
///     Marks an event type as binary-serialized: its <c>pc_events.data</c> column holds the
///     <c>'{}'</c> placeholder and the real payload lives in <c>bdata</c>, written and read by an
///     <see cref="IEventBinarySerializer" />. Mirrors Marten's <c>[BinaryEvent]</c>
///     (<see href="https://github.com/JasperFx/marten/issues/4515" />); tracked as
///     <see href="https://github.com/JasperFx/polecat/issues/388">polecat#388</see>.
/// </summary>
/// <remarks>
///     <para>
///         The serializer for an attribute-marked type is the store-wide
///         <c>opts.Events.DefaultBinarySerializer</c>. An explicit
///         <c>opts.Events.UseBinarySerializer&lt;TEvent&gt;(serializer)</c> registration takes
///         precedence. If a type is attribute-marked but neither is configured, the store throws when
///         it first resolves that event type rather than silently writing JSON — a silent fallback
///         would produce a store whose write amplification quietly does not match its configuration.
///     </para>
///     <para>
///         JSON and binary events coexist per event type in the same table, so applying this to a
///         single event type is a safe in-place change: existing JSON rows keep <c>bdata = NULL</c>
///         and keep reading through the JSON path.
///     </para>
/// </remarks>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class BinaryEventAttribute : Attribute
{
}
