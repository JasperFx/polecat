using System.Diagnostics;
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
    [UnconditionalSuppressMessage("Trimming", "IL2091:DynamicallyAccessedMembers",
        Justification = "Forwards to RunProjectionForReferenceTypeAsync<TState> via MakeGenericMethod. Projection step-through is a dev-time / diagnostic IEventStore surface (CritterWatch / projection replay); AOT-publishing apps either avoid the surface entirely or supply a source-generated dispatcher.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "MakeGenericMethod is required to satisfy the aggregator's `class, new()` constraint without changing the public IEventStore.RunProjectionAsync<TState> contract.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075:DynamicallyAccessedMembers",
        Justification = "Reads Task<T>.Result via reflection; Task<T> is a framework type whose Result property is intrinsically preserved.")]
    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Calls into RunProjectionForReferenceTypeAsync<TState> which is annotated [RequiresUnreferencedCode]. The explicit interface impl can't propagate the requirement because the interface contract doesn't declare it yet (tracked in jasperfx#262 for the Events-side IEventStore annotation).")]
    async Task<ProjectionTimeline<TState>> IEventStore.RunProjectionAsync<TState>(
        string projectionName, object identity, IReadOnlyList<EventRecord> events,
        TState? startingState, CancellationToken ct) where TState : default
    {
        ArgumentException.ThrowIfNullOrEmpty(projectionName);
        ArgumentNullException.ThrowIfNull(events);

        if (!Options.Projections.TryFindProjection(projectionName, out _))
        {
            throw new ArgumentException(
                $"Unknown projection '{projectionName}'. Register the projection on StoreOptions.Projections before replaying.",
                nameof(projectionName));
        }

        // The interface leaves TState unconstrained, but the aggregator graph
        // requires a reference-type aggregate. Surface a clear NotSupportedException
        // for value-typed states until multi-aggregate / DCB projections land.
        if (typeof(TState).IsValueType)
        {
            throw new NotSupportedException(
                $"RunProjectionAsync requires a reference-typed aggregate state on Polecat. " +
                $"'{typeof(TState).FullName}' is a value type — multi-aggregate / value-state projections are not yet supported.");
        }

        // Forward to the strong-typed implementation through reflection so we
        // can satisfy the aggregator's class constraint without changing the
        // public contract.
        var generic = typeof(DocumentStore)
            .GetMethod(nameof(RunProjectionForReferenceTypeAsync), BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(typeof(TState));

        var resultTask = (Task)generic.Invoke(this, new object?[] { events, startingState, ct })!;
        await resultTask.ConfigureAwait(false);
        return (ProjectionTimeline<TState>)resultTask.GetType().GetProperty("Result")!.GetValue(resultTask)!;
    }

    [RequiresUnreferencedCode("Routes projection events through the aggregator graph + ToDomainEvent reflective deserialization. TState's public members must survive trimming for the aggregator to access them.")]
    [RequiresDynamicCode("ToDomainEvent routes through ISerializer.FromJson which uses STJ runtime code generation.")]
    private async Task<ProjectionTimeline<TState>> RunProjectionForReferenceTypeAsync<TState>(
        IReadOnlyList<EventRecord> events, TState? startingState, CancellationToken ct)
        where TState : class, new()
    {
        var aggregator = Options.Projections.AggregatorFor<TState>();

        await using var session = QuerySession();

        var current = startingState;
        var steps = new List<ProjectionStepResult<TState>>(events.Count);

        foreach (var record in events)
        {
            var domainEvent = ToDomainEvent(record);
            var before = current;
            var sw = Stopwatch.StartNew();
            TState? after = current;
            Exception? error = null;
            try
            {
                if (domainEvent != null)
                {
                    var built = await aggregator.BuildAsync(
                        new List<IEvent> { domainEvent }, session, current, ct);
                    after = built ?? current;
                }
            }
            catch (Exception ex)
            {
                error = ex;
                after = current;
            }
            sw.Stop();

            steps.Add(new ProjectionStepResult<TState>(record, before!, after!, sw.Elapsed, error!));
            current = after;
        }

        return new ProjectionTimeline<TState>(steps, current!);
    }

    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "MakeGenericMethod over the projection's published state type; required to dispatch into the strong-typed RunProjectionAsync<TState>. Diagnostic IEventStore surface — see RunProjectionAsync above for the rationale.")]
    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "STJ JsonSerializer.Deserialize / SerializeToElement over the projection's published state type. AOT-publishing diagnostic-only consumers should supply an STJ source-generator context.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075:DynamicallyAccessedMembers",
        Justification = "Reads Task<T> + ProjectionTimeline<T> properties via reflection. The closed generic types' Result / Steps / FinalState properties are intrinsically preserved by the framework / by this class' own usage.")]
    async Task<ProjectionTimelineRaw> IEventStore.RunProjectionByNameAsync(
        string projectionName, object identity, IReadOnlyList<EventRecord> events,
        JsonElement? startingState, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(projectionName);
        ArgumentNullException.ThrowIfNull(events);

        if (!Options.Projections.TryFindProjection(projectionName, out var source))
        {
            throw new ArgumentException(
                $"Unknown projection '{projectionName}'. Register the projection on StoreOptions.Projections before replaying.",
                nameof(projectionName));
        }

        var stateType = source.PublishedTypes().FirstOrDefault()
            ?? throw new NotSupportedException(
                $"Projection '{projectionName}' does not publish a strong-typed state — RunProjectionByNameAsync only supports single-aggregate projections on Polecat.");

        var generic = typeof(IEventStore).GetMethod(nameof(IEventStore.RunProjectionAsync))!
            .MakeGenericMethod(stateType);

        var startingTyped = startingState.HasValue
            ? JsonSerializer.Deserialize(startingState.Value.GetRawText(), stateType)
            : null;

        var task = (Task)generic.Invoke(this, new[] { projectionName, identity, events, startingTyped, (object)ct })!;
        await task.ConfigureAwait(false);

        var typedTimeline = task.GetType().GetProperty("Result")!.GetValue(task)!;
        var typedSteps = (System.Collections.IEnumerable)typedTimeline.GetType().GetProperty("Steps")!.GetValue(typedTimeline)!;
        var typedFinal = typedTimeline.GetType().GetProperty("FinalState")!.GetValue(typedTimeline);

        var rawSteps = new List<ProjectionStepResultRaw>(events.Count);
        foreach (var step in typedSteps)
        {
            var stepType = step.GetType();
            var record = (EventRecord)stepType.GetProperty("Event")!.GetValue(step)!;
            var beforeVal = stepType.GetProperty("Before")!.GetValue(step);
            var afterVal = stepType.GetProperty("After")!.GetValue(step);
            var elapsed = (TimeSpan)stepType.GetProperty("Elapsed")!.GetValue(step)!;
            var error = (Exception?)stepType.GetProperty("Error")!.GetValue(step);

            rawSteps.Add(new ProjectionStepResultRaw(
                record,
                beforeVal is null ? null : JsonSerializer.SerializeToElement(beforeVal, stateType),
                afterVal is null ? null : JsonSerializer.SerializeToElement(afterVal, stateType),
                elapsed,
                error?.Message!));
        }

        var finalJson = typedFinal is null
            ? (JsonElement?)null
            : JsonSerializer.SerializeToElement(typedFinal, stateType);

        return new ProjectionTimelineRaw(rawSteps, finalJson);
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
        var clrType = ResolveEventClrType(record.EventTypeName);
        if (clrType is null) return null;

        var raw = Options.Serializer.FromJson(clrType, record.Data.GetRawText());
        if (raw is null) return null;

        var mapping = Events.EventMappingFor(clrType);
        var wrapped = mapping.Wrap(raw);

        wrapped.Id = record.EventId;
        wrapped.Sequence = record.Sequence;
        wrapped.Version = record.StreamVersion;
        wrapped.Timestamp = record.Timestamp;
        wrapped.TenantId = record.TenantId ?? Tenancy.DefaultTenantId;
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
