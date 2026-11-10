using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Events;

namespace Polecat;

/// <summary>
///     Stateless projection replay (CritterWatch #87 / #210). Walks a fixed
///     in-memory event list through a registered projection, returning the
///     per-event before/after timeline. Nothing is persisted.
/// </summary>
public partial class DocumentStore
{
    // jasperfx#1049 (polecat#806): the three replay members forward to the shared
    // ProjectionStepThrough, which folds through the projection's own dispatch and records each step as
    // a detached copy read back from the JSON the store would persist. Polecat's own fold recorded live
    // references, so an aggregate that mutated itself in place showed its final state on every step.
    private ProjectionStepThrough<IQuerySession>? _stepThrough;

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "ToDomainEvent and the store serializer use STJ over runtime types. Projection step-through is a diagnostic IEventStore surface (CritterWatch / projection replay), and the interface contract does not declare the requirement.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "ToDomainEvent and the store serializer use STJ runtime code generation. Diagnostic IEventStore surface, as above.")]
    private ProjectionStepThrough<IQuerySession> StepThrough()
    {
        return _stepThrough ??= Options.Projections.StepThrough(ToDomainEvent,
            new ProjectionStateSerializer(
                state => JsonSerializer.Deserialize<JsonElement>(Options.Serializer.ToJson(state)),
                (json, type) => Options.Serializer.FromJson(type, json.GetRawText())));
    }

    async Task<ProjectionTimeline<TState>> IEventStore.RunProjectionAsync<TState>(
        string projectionName, object identity, IReadOnlyList<EventRecord> events,
        TState? startingState, CancellationToken ct) where TState : default
    {
        var stepThrough = StepThrough();
        await using var session = QuerySession();
        return await stepThrough.RunProjectionAsync(projectionName, identity, events, startingState, session, ct)
            .ConfigureAwait(false);
    }

    async Task<ProjectionTimelineRaw> IEventStore.RunProjectionByNameAsync(
        string projectionName, object identity, IReadOnlyList<EventRecord> events,
        JsonElement? startingState, CancellationToken ct)
    {
        var stepThrough = StepThrough();
        await using var session = QuerySession();
        return await stepThrough.RunProjectionByNameAsync(projectionName, identity, events, startingState, session, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    ///     #346: MultiStreamProjection stepthrough parity. Replays a fixed event list through a
    ///     registered aggregation projection's <em>real</em> execution path — slice → group →
    ///     <c>EnrichEventsAsync</c> → evolve — fanning the events out across every aggregate identity
    ///     the projection touches and returning one timeline per identity. Stateless; nothing is persisted.
    /// </summary>
    async Task<MultiAggregateProjectionResult> IEventStore.RunMultiStreamProjectionAsync(
        string projectionName, IReadOnlyList<EventRecord> events, CancellationToken ct)
    {
        var stepThrough = StepThrough();
        await using var session = QuerySession();
        return await stepThrough.RunMultiStreamProjectionAsync(projectionName, events, session, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    ///     Converts a wire-level <see cref="EventRecord"/> back into a Polecat
    ///     domain event the aggregator can apply. Returns <c>null</c> when the
    ///     event type isn't registered with the store.
    /// </summary>
    [RequiresUnreferencedCode("Walks loaded assemblies to resolve eventTypeName → CLR Type and routes through ISerializer.FromJson which is annotated [RequiresUnreferencedCode] for STJ usage.")]
    [RequiresDynamicCode("ISerializer.FromJson uses STJ which requires runtime code generation for non-source-generated types.")]
    private IEvent? ToDomainEvent(EventRecord record)
    {
        // #561 / jasperfx#752: upcast first, like every other read path. This one is already keyed on
        // the stored event type name rather than the dotnet_type hint, so there is no authority
        // question to resolve here -- but a replay that skipped upcasting would show the timeline in
        // terms of an event schema the rest of the store has stopped producing.
        object? raw;
        Type clrType;

        if (Events.TryFindUpcast(record.EventTypeName, bdata: null, out var transformation))
        {
            clrType = transformation.EventType;
            raw = Events.Upcast(transformation, record.Data.GetRawText(), Options.Serializer);
        }
        else
        {
            var resolved = ResolveEventClrType(record.EventTypeName);
            if (resolved is null) return null;

            clrType = resolved;
            raw = Options.Serializer.FromJson(clrType, record.Data.GetRawText());
        }

        if (raw is null) return null;

        var mapping = Events.EventMappingFor(clrType);
        var wrapped = mapping.Wrap(raw);

        wrapped.Id = record.EventId;
        wrapped.Sequence = record.Sequence;
        wrapped.Version = record.StreamVersion;
        wrapped.Timestamp = record.Timestamp;
        wrapped.TenantId = record.TenantId ?? JasperFx.StorageConstants.DefaultTenantId;
        wrapped.EventTypeName = record.EventTypeName;
        wrapped.DotNetTypeName = clrType.AssemblyQualifiedName ?? clrType.FullName!;

        if (Events.StreamIdentity == StreamIdentity.AsGuid && Guid.TryParse(record.StreamId, out var sg))
        {
            wrapped.StreamId = sg;
        }
        else
        {
            wrapped.StreamKey = record.StreamId;
        }

        return wrapped;
    }

    [RequiresUnreferencedCode("Walks AppDomain.GetAssemblies() + Assembly.GetTypes() to resolve eventTypeName → CLR Type. The trimmer cannot reason about which event types survive; AOT-publishing apps should register event types explicitly via EventGraph.AddEventType or use the source-generated event registry.")]
    private Type? ResolveEventClrType(string eventTypeName)
    {
        // 1. Most common: an event-type alias registered on the EventGraph
        //    (e.g. "quest_started"). PolecatEventType derives the alias from
        //    the CLR type name, so a lookup against the registry is cheap.
        foreach (var registered in Options.EventGraph.AllKnownEventTypes())
        {
            if (string.Equals(registered.EventTypeName, eventTypeName, StringComparison.Ordinal))
            {
                return registered.EventType;
            }
        }

        // 2. Try assembly-qualified name on the off chance the caller is
        //    sending raw .NET type identifiers
        var direct = Type.GetType(eventTypeName);
        if (direct != null) return direct;

        // 3. Walk loaded assemblies for a FullName / Name match — covers events
        //    that were never appended on this store (and so missed registry
        //    population) but are present in the project's compiled assemblies.
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            // FullName match
            var match = assembly.GetType(eventTypeName, throwOnError: false);
            if (match != null)
            {
                Options.EventGraph.AddEventType(match);
                return match;
            }
        }

        // 4. snake_case alias → PascalCase type-name search
        var pascal = SnakeToPascal(eventTypeName);
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            Type[] types;
            try { types = assembly.GetTypes(); }
            catch (ReflectionTypeLoadException ex) { types = ex.Types.Where(t => t != null).ToArray()!; }

            foreach (var t in types)
            {
                if (t == null) continue;
                if (string.Equals(t.Name, pascal, StringComparison.Ordinal))
                {
                    Options.EventGraph.AddEventType(t);
                    return t;
                }
            }
        }

        return null;
    }

    private static string SnakeToPascal(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        var sb = new System.Text.StringBuilder(name.Length);
        var capitalizeNext = true;
        foreach (var c in name)
        {
            if (c == '_') { capitalizeNext = true; continue; }
            sb.Append(capitalizeNext ? char.ToUpperInvariant(c) : c);
            capitalizeNext = false;
        }
        return sb.ToString();
    }
}
