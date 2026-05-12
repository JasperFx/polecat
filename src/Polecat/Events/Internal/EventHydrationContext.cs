using JasperFx.Events;
using Polecat.Serialization;

namespace Polecat.Events.Internal;

/// <summary>
///     Per-call context for hydrating <c>pc_events</c> rows into either
///     a fully-resolved <see cref="IEvent"/> (via <c>FetchStreamAsync</c>)
///     or a raw <see cref="JasperFx.Descriptors.EventRecord"/> (via the
///     <see cref="IEventStore"/> explorer's <c>ReadStreamAsync</c>).
///     Packages the dependencies the two read paths share so the shared
///     <see cref="PcEventsRowReader"/> doesn't grow a long parameter list.
/// </summary>
/// <remarks>
///     <para>
///     <see cref="StreamId"/> is per-call (each <c>FetchStreamAsync</c>
///     invocation supplies its own); the rest are session- or store-
///     scoped. <see cref="StreamId"/> is unused by the
///     <see cref="JasperFx.Descriptors.EventRecord"/> path, which reads
///     each row's <c>stream_id</c> column directly.
///     </para>
///     <para>
///     <see cref="DefaultTenantId"/> is the fallback when the row's
///     <c>tenant_id</c> column is NULL — only reachable from the explorer
///     path; the <see cref="IEvent"/> path's <c>WHERE</c> clause filters
///     by tenant and guarantees the column is non-null.
///     </para>
///     <para>
///     <c>EventStoreOptions</c> and <c>StreamIdentity</c> are deliberately
///     NOT exposed here. Both are stable across a batch, so per-row reads
///     would re-evaluate them needlessly; instead the caller hoists those
///     decisions: <c>EventStoreOptions</c> flows through
///     <see cref="MetadataSlots"/> (computed once), and
///     <c>StreamIdentity</c> is dispatched once via
///     <see cref="PcEventsRowReader.ReadEventAsGuid"/> vs
///     <see cref="PcEventsRowReader.ReadEventAsString"/>.
///     </para>
/// </remarks>
internal sealed class EventHydrationContext
{
    public EventHydrationContext(
        EventGraph eventGraph,
        ISerializer serializer,
        object streamId,
        string defaultTenantId)
    {
        EventGraph = eventGraph;
        Serializer = serializer;
        StreamId = streamId;
        DefaultTenantId = defaultTenantId;
    }

    public EventGraph EventGraph { get; }
    public ISerializer Serializer { get; }

    /// <summary>
    ///     The stream id the caller is reading (<see cref="System.Guid"/> or
    ///     <see cref="string"/> depending on the active
    ///     <see cref="JasperFx.Events.StreamIdentity"/>). Used by the
    ///     <see cref="IEvent"/> read methods to set
    ///     <see cref="IEvent.StreamId"/> / <see cref="IEvent.StreamKey"/>;
    ///     unused by <see cref="PcEventsRowReader.ReadEventRecord"/>.
    /// </summary>
    public object StreamId { get; }

    /// <summary>
    ///     Tenant id substituted when a row's <c>tenant_id</c> column is
    ///     NULL. Only reachable from the explorer path; the IEvent path's
    ///     WHERE clause filters by tenant.
    /// </summary>
    public string DefaultTenantId { get; }
}
