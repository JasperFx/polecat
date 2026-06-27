using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Internal;

namespace Polecat.Events.Projections;

/// <summary>
///     Inline projection that maintains natural key → stream id mappings
///     in the pc_natural_key_{type} table. Automatically upserts mappings
///     when events carrying natural key values are appended, and marks
///     mappings as archived when an Archived event is detected.
/// </summary>
internal class NaturalKeyProjection : IInlineProjection<IDocumentSession>
{
    private readonly NaturalKeyDefinition _definition;
    private readonly EventGraph _events;
    private readonly string _qualifiedTableName;
    private readonly bool _isGuidStream;
    private readonly bool _isConjoined;

    public NaturalKeyProjection(NaturalKeyDefinition definition, EventGraph events)
    {
        _definition = definition;
        _events = events;
        _qualifiedTableName = $"[{events.DatabaseSchemaName}].[pc_natural_key_{definition.AggregateType.Name.ToLowerInvariant()}]";
        _isGuidStream = events.StreamIdentity == StreamIdentity.AsGuid;
        _isConjoined = events.TenancyStyle == TenancyStyle.Conjoined;
    }

    public Task ApplyAsync(IDocumentSession operations, IEnumerable<StreamAction> streams,
        CancellationToken cancellation)
    {
        if (operations is not DocumentSessionBase sessionBase) return Task.CompletedTask;

        foreach (var stream in streams)
        {
            var streamId = _isGuidStream ? (object)stream.Id : stream.Key!;
            var tenantId = stream.TenantId;

            foreach (var e in stream.Events)
            {
                QueueOperationForEvent(sessionBase, streamId, tenantId, e);
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    ///     #259: rebuild-time counterpart to <see cref="ApplyAsync" />. The async-daemon rebuild path
    ///     replays already-persisted events without appending streams, so <see cref="ApplyAsync" />'s
    ///     StreamAction-driven dispatch never fires and the <c>pc_natural_key_X</c> table stays empty
    ///     after teardown. This entry-point feeds raw <see cref="IEvent" />s through the same operation
    ///     builder, pulling stream id/key + tenant id off the event itself (events read from pc_events
    ///     always carry these). Called from <c>StartProjectionBatchAsync</c> per rebuild page with
    ///     <paramref name="operations" /> routed through <c>PolecatProjectionBatch.SessionForTenant</c>,
    ///     so the operations land on a session whose work-tracker flushes inside the batch transaction
    ///     alongside the rebuilt snapshots. Archived events are replayed too, so a stream archived
    ///     before the rebuild keeps its natural-key row marked <c>is_archived = 1</c> afterward.
    /// </summary>
    internal void QueueUpsertsForEvents(IDocumentSession operations, IEnumerable<IEvent> events)
    {
        if (operations is not DocumentSessionBase sessionBase) return;

        foreach (var e in events)
        {
            var streamId = _isGuidStream ? (object)e.StreamId : e.StreamKey!;
            var tenantId = e.TenantId ?? StorageConstants.DefaultTenantId;
            QueueOperationForEvent(sessionBase, streamId, tenantId, e);
        }
    }

    /// <summary>
    ///     Shared operation builder used by both the inline append path (<see cref="ApplyAsync" />) and
    ///     the rebuild path (<see cref="QueueUpsertsForEvents" />): an Archived event marks the
    ///     natural-key row archived; an event carrying a mapped natural key upserts the mapping.
    /// </summary>
    private void QueueOperationForEvent(DocumentSessionBase sessionBase, object streamId, string tenantId, IEvent e)
    {
        // Check for Archived event to mark natural key as archived
        if (e.EventType == typeof(Archived))
        {
            sessionBase.WorkTracker.Add(
                new NaturalKeyArchiveOperation(_qualifiedTableName, streamId, _isGuidStream,
                    _isConjoined, tenantId));
            return;
        }

        // Check if this event type has a natural key mapping
        var mapping = _definition.EventMappings
            .FirstOrDefault(m => m.EventType.IsAssignableFrom(e.Data.GetType()));

        if (mapping == null) return;

        var naturalKeyValue = mapping.Extractor(e.Data);
        if (naturalKeyValue == null) return;

        // Unwrap strong-typed id to primitive value
        var unwrapped = _definition.Unwrap(naturalKeyValue);
        if (unwrapped == null) return;

        sessionBase.WorkTracker.Add(
            new NaturalKeyUpsertOperation(_qualifiedTableName, unwrapped, streamId, _isGuidStream,
                _isConjoined, tenantId));
    }
}
