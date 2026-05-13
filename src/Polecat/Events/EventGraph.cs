using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Polecat.Events.Schema;
using Polecat.Projections;
using Polecat.Serialization;

namespace Polecat.Events;

/// <summary>
///     Central configuration and registry for the Polecat event store.
///     Analogous to Marten's EventGraph. Manages event type registration
///     and wrapping of raw event data into IEvent instances.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: extends JasperFx.Events.EventRegistry (annotated RUC) for type-aliased event construction; also wires aggregator sources via reflection. Event types are preserved by registration on the caller side per the AOT publishing guide.")]
[UnconditionalSuppressMessage("Trimming", "IL2057:UnrecognizedTypeName",
    Justification = "Class-level: AggregateTypeFor uses Type.GetType(string) as a fallback to resolve aggregate types persisted by .NET type name in event metadata. The aggregate types are preserved by projection registration on the caller side; AOT consumers should register all aggregate types ahead of time per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: aggregator-source factories and event-type registration use Type.MakeGenericType — runtime code generation. AOT consumers register concrete event types ahead of time.")]
public class EventGraph : EventRegistry, IAggregationSourceFactory<IQuerySession>
{
    private readonly StoreOptions _options;
    private readonly ConcurrentDictionary<Type, PolecatEventType> _eventTypes = new();
    private readonly ConcurrentDictionary<string, Type> _aggregateTypes = new();
    private readonly List<ITagTypeRegistration> _tagTypes = new();
    private readonly List<IMasker> _maskers = new();

    internal EventGraph(StoreOptions options)
    {
        _options = options;
        AppendMode = EventAppendMode.Quick;
    }

    /// <summary>
    ///     Controls whether streams are identified by Guid or string.
    /// </summary>
    public override StreamIdentity StreamIdentity
    {
        get => _options.Events.StreamIdentity;
        set => _options.Events.StreamIdentity = value;
    }

    /// <summary>
    ///     Controls the tenancy style for event store tables.
    /// </summary>
    public TenancyStyle TenancyStyle
    {
        get => _options.Events.TenancyStyle;
        set => _options.Events.TenancyStyle = value;
    }

    /// <summary>
    ///     The database schema name for event store tables.
    ///     Falls back to StoreOptions.DatabaseSchemaName if not overridden.
    /// </summary>
    public string DatabaseSchemaName =>
        _options.Events.DatabaseSchemaName ?? _options.DatabaseSchemaName;

    internal ISerializer Serializer => _options.Serializer;

    internal EventStoreOptions EventOptions => _options.Events;

    internal string JsonColumnType => _options.JsonColumnType;

    /// <summary>
    ///     Whether extended progression tracking columns are enabled.
    /// </summary>
    public bool EnableExtendedProgressionTracking => _options.Events.EnableExtendedProgressionTracking;

    /// <summary>
    ///     Opt into a performance optimization that directs Polecat to use a session-level
    ///     identity map for aggregates fetched via FetchForWriting() or FetchLatest().
    ///     Subsequent calls to FetchLatest() within the same session will return the cached
    ///     instance instead of re-querying the database.
    ///     Note: only appropriate if using immutable aggregations or when you do not mutate
    ///     the aggregate yourself outside of Polecat internals.
    /// </summary>
    public bool UseIdentityMapForAggregates { get; set; }

    /// <summary>
    ///     Enable SQL Server table partitioning on the pc_events table by the
    ///     is_archived column. This separates archived events into a different
    ///     partition for improved query performance when aggressively archiving
    ///     event streams.
    /// </summary>
    public bool UseArchivedStreamPartitioning { get; set; }

    internal string StreamsTableName => $"[{DatabaseSchemaName}].[pc_streams]";
    internal string EventsTableName => $"[{DatabaseSchemaName}].[pc_events]";
    internal string ProgressionTableName => $"[{DatabaseSchemaName}].[pc_event_progression]";

    public override EventAppendMode AppendMode
    {
        get => base.AppendMode;
        set => base.AppendMode = value;
    }

    /// <summary>
    ///     Wrap raw event data into an IEvent instance with type metadata.
    /// </summary>
    public override IEvent BuildEvent(object eventData)
    {
        ArgumentNullException.ThrowIfNull(eventData);

        if (eventData is IEvent e)
        {
            var mapping = EventMappingFor(e.EventType);
            e.EventTypeName = mapping.EventTypeName;
            e.DotNetTypeName = mapping.DotNetTypeName;
            return e;
        }

        var eventType = EventMappingFor(eventData.GetType());
        return eventType.Wrap(eventData);
    }

    /// <summary>
    ///     Get or create event type metadata for the given .NET type.
    /// </summary>
    public override PolecatEventType EventMappingFor(Type eventType)
    {
        return _eventTypes.GetOrAdd(eventType, static type => new PolecatEventType(type));
    }

    public override void AddEventType(Type eventType)
    {
        EventMappingFor(eventType);
    }

    /// <summary>
    ///     Build an on-the-fly aggregator source for live aggregation of the given type.
    ///     Creates a SingleStreamProjection for convention-based aggregate types.
    /// </summary>
    IAggregatorSource<IQuerySession>? IAggregationSourceFactory<IQuerySession>.Build<TDoc>()
    {
        // Use the appropriate identity type based on stream identity configuration
        var idType = StreamIdentity == StreamIdentity.AsGuid ? typeof(Guid) : typeof(string);
        var projectionType = typeof(SingleStreamProjection<,>).MakeGenericType(typeof(TDoc), idType);
#pragma warning disable CS8714 // notnull constraint mismatch
        var projection = (ProjectionBase)Activator.CreateInstance(projectionType)!;
#pragma warning restore CS8714
        projection.Lifecycle = ProjectionLifecycle.Live;
        projection.AssembleAndAssertValidity();
        foreach (var et in projection.IncludedEventTypes) AddEventType(et);
        return projection as IAggregatorSource<IQuerySession>;
    }

    public override Type AggregateTypeFor(string aggregateTypeName)
    {
        if (_aggregateTypes.TryGetValue(aggregateTypeName, out var type)) return type;
        throw new ArgumentOutOfRangeException(nameof(aggregateTypeName),
            $"Unknown aggregate type name '{aggregateTypeName}'.");
    }

    public override string AggregateAliasFor(Type aggregateType)
    {
        _aggregateTypes.TryAdd(aggregateType.Name, aggregateType);
        return aggregateType.Name;
    }

    /// <summary>
    ///     Try to resolve a .NET type from the dotnet_type name stored in the database.
    /// </summary>
    internal Type? ResolveEventType(string? dotNetTypeName)
    {
        if (string.IsNullOrEmpty(dotNetTypeName)) return null;
        return Type.GetType(dotNetTypeName);
    }

    internal StreamsTable BuildStreamsTable()
    {
        return new StreamsTable(this);
    }

    internal EventsTable BuildEventsTable()
    {
        return new EventsTable(this);
    }

    internal EventProgressionTable BuildEventProgressionTable()
    {
        return new EventProgressionTable(this);
    }

    public ITagTypeRegistration RegisterTagType<TTag>() where TTag : notnull
    {
        var existing = _tagTypes.FirstOrDefault(t => t.TagType == typeof(TTag));
        if (existing != null) return existing;
        var registration = TagTypeRegistration.Create<TTag>();
        _tagTypes.Add(registration);
        return registration;
    }

    public ITagTypeRegistration RegisterTagType<TTag>(string tableSuffix) where TTag : notnull
    {
        var existing = _tagTypes.FirstOrDefault(t => t.TagType == typeof(TTag));
        if (existing != null) return existing;
        var registration = TagTypeRegistration.Create<TTag>(tableSuffix);
        _tagTypes.Add(registration);
        return registration;
    }

    public IReadOnlyList<ITagTypeRegistration> TagTypes => _tagTypes;

    /// <summary>
    ///     All currently registered event types.
    /// </summary>
    public IReadOnlyList<PolecatEventType> AllKnownEventTypes() => _eventTypes.Values.ToList();

    public ITagTypeRegistration? FindTagType(Type tagType)
    {
        return _tagTypes.FirstOrDefault(t => t.TagType == tagType);
    }

    internal EventTagTable BuildEventTagTable(ITagTypeRegistration registration)
    {
        return new EventTagTable(this, registration);
    }

    /// <summary>
    ///     Convert a PascalCase type name to a snake_case event type alias.
    ///     e.g. QuestStarted → quest_started
    /// </summary>
    internal static string ToEventTypeName(string typeName)
    {
        var result = new StringBuilder();
        for (var i = 0; i < typeName.Length; i++)
        {
            var c = typeName[i];
            if (char.IsUpper(c))
            {
                if (i > 0) result.Append('_');
                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }

        return result.ToString();
    }

    /// <summary>
    ///     Register a policy for how to remove or mask protected information
    ///     for an event type T or series of event types that can be cast to T.
    /// </summary>
    public void AddMaskingRuleForProtectedInformation<T>(Action<T> action) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(action);
        _maskers.Add(new ActionMasker<T>(action));
    }

    /// <summary>
    ///     Register a policy for how to remove or mask protected information
    ///     for an event type T, replacing the event data with a new instance.
    /// </summary>
    public void AddMaskingRuleForProtectedInformation<T>(Func<T, T> func) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(func);
        _maskers.Add(new FuncMasker<T>(func));
    }

    internal bool TryMask(IEvent e)
    {
        var matched = false;
        foreach (var masker in _maskers)
        {
            matched = matched || masker.TryMask(e);
        }
        return matched;
    }
}

internal interface IMasker
{
    bool TryMask(IEvent @event);
}

internal class ActionMasker<T> : IMasker where T : notnull
{
    private readonly Action<T> _masking;

    public ActionMasker(Action<T> masking)
    {
        _masking = masking;
    }

    public bool TryMask(IEvent @event)
    {
        if (@event is IEvent<T> e)
        {
            _masking(e.Data);
            return true;
        }
        return false;
    }
}

internal class FuncMasker<T> : IMasker where T : notnull
{
    private readonly Func<T, T> _masking;

    public FuncMasker(Func<T, T> masking)
    {
        _masking = masking;
    }

    public bool TryMask(IEvent @event)
    {
        if (@event is Event<T> e)
        {
            e.Data = _masking(e.Data);
            return true;
        }
        return false;
    }
}

/// <summary>
///     Metadata and wrapping logic for a single event type.
///     Implements IEventType from JasperFx.Events.
/// </summary>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: Wrap uses Type.MakeGenericType(typeof(Event<>), eventType) to construct Event<T> envelopes — runtime code generation. Event types are preserved by registration on the caller side per the AOT publishing guide.")]
public class PolecatEventType : IEventType
{
    private readonly Type _eventType;

    public PolecatEventType(Type eventType)
    {
        _eventType = eventType;
        EventTypeName = EventGraph.ToEventTypeName(eventType.Name);
        DotNetTypeName = $"{eventType.FullName}, {eventType.Assembly.GetName().Name}";
    }

    public Type EventType => _eventType;
    public string EventTypeName { get; set; }
    public string DotNetTypeName { get; set; }
    public string Alias => EventTypeName;

    /// <summary>
    ///     Wrap raw event data into an Event&lt;T&gt; with type metadata.
    /// </summary>
    public IEvent Wrap(object eventData)
    {
        var genericType = typeof(Event<>).MakeGenericType(_eventType);
        var @event = (IEvent)Activator.CreateInstance(genericType, eventData)!;
        @event.EventTypeName = EventTypeName;
        @event.DotNetTypeName = DotNetTypeName;
        return @event;
    }
}
