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
                // Check for Archived event to mark natural key as archived
                if (e.EventType == typeof(Archived))
                {
                    sessionBase.WorkTracker.Add(
                        new NaturalKeyArchiveOperation(_qualifiedTableName, streamId, _isGuidStream,
                            _isConjoined, tenantId));
                    continue;
                }

                // Check if this event type has a natural key mapping
                var mapping = _definition.EventMappings
                    .FirstOrDefault(m => m.EventType.IsAssignableFrom(e.Data.GetType()));

                if (mapping == null) continue;

                var naturalKeyValue = mapping.Extractor(e.Data);
                if (naturalKeyValue == null) continue;

                // Unwrap strong-typed id to primitive value
                var unwrapped = _definition.Unwrap(naturalKeyValue);
                if (unwrapped == null) continue;

                sessionBase.WorkTracker.Add(
                    new NaturalKeyUpsertOperation(_qualifiedTableName, unwrapped, streamId, _isGuidStream,
                        _isConjoined, tenantId));
            }
        }

        return Task.CompletedTask;
    }
}
