using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Polecat.Events.Schema;
using Polecat.Projections;
using Polecat.Serialization;
using Polecat.Storage;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Events;

/// <summary>
///     Central configuration and registry for the Polecat event store.
///     Analogous to Marten's EventGraph. Manages event type registration
///     and wrapping of raw event data into IEvent instances.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: extends JasperFx.Events.EventRegistry (annotated RUC) for type-aliased event construction; also wires aggregator sources via reflection. Event types are preserved by registration on the caller side per the AOT publishing guide.")]
[UnconditionalSuppressMessage("Trimming", "IL2057:UnrecognizedTypeName",
    Justification = "Class-level: ResolveEventType uses Type.GetType(string) to resolve the dotnet_type name persisted on each event row. Event types are preserved by EventGraph registration on the caller side; AOT consumers should register all event types ahead of time per the AOT publishing guide. Aggregate types are NOT resolved this way — TryResolveAggregateType is a registry lookup over registered aliases and projections, no reflection over type names.")]
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

    /// <summary>
    ///     Master switch for per-tenant event partitioning (Polecat #163 / CritterWatch #209).
    ///     When enabled, the SQL Server append path moves from a global
    ///     <c>seq_id BIGINT IDENTITY</c> to per-tenant <c>pc_events_sequence_{suffix}</c>
    ///     SEQUENCE objects, and the async daemon's high-water detector returns
    ///     <see cref="JasperFx.Events.Daemon.HighWater.IHighWaterDetector.SupportsTenantPartitioning"/> = true
    ///     so tenant-scoped rebuilds and high-water reads run bounded by a single
    ///     tenant's sequence ceiling rather than the database-wide event-sequence scan.
    ///     <para>
    ///     <b>Default is false</b> — existing stores keep the global IDENTITY path
    ///     byte-for-byte. The per-tenant SEQUENCE + tenant-registry + daemon-side
    ///     surface land in subsequent phases (#163 Phase 1 + Phase 2); Phase 0 only
    ///     introduces this flag so downstream phases can gate cleanly.
    ///     </para>
    ///     <para>
    ///     Polecat is single-append-path (QuickAppend only — direct INSERTs, no stored
    ///     procedures), so no append-mode compatibility guard is needed.
    ///     </para>
    /// </summary>
    public bool UseTenantPartitionedEvents { get; set; }

    /// <summary>
    ///     Process projection side effects (slice.PublishMessage) when running
    ///     projections under the Inline lifecycle. Off by default — flip on to
    ///     route inline-projection-emitted messages through the configured
    ///     <see cref="StoreOptions.MessageOutbox"/>. Inline appended events
    ///     (slice.AppendEvent) are not supported and will throw at runtime.
    /// </summary>
    // Read from the configured StoreOptions.Events so it can be set in AddPolecat(...)/DocumentStore.For
    // config (m.Events.EnableSideEffectsOnInlineProjections = true), mirroring EnableCorrelationId etc.
    // and Marten's StoreOptions.Events.EnableSideEffectsOnInlineProjections.
    public bool EnableSideEffectsOnInlineProjections => _options.Events.EnableSideEffectsOnInlineProjections;

    internal string StreamsTableName => Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, "pc_streams");
    internal string EventsTableName => Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, "pc_events");

    internal string ProgressionTableName =>
        Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, "pc_event_progression");

    internal string TenantPartitionsTableName =>
        Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, "pc_tenant_partitions");

    /// <summary>
    ///     The unqualified DCB tag table name for a registered tag type.
    /// </summary>
    /// <remarks>
    ///     #390: <c>TableSuffix</c> is a public-API argument
    ///     (<see cref="RegisterTagType{TTag}(string)" />), and a dozen call sites used to compose this
    ///     name by hand. Two paths building the same object name by different means is both an
    ///     injection risk and a correctness divergence — they agree right up until quoting is needed.
    ///     Every caller now goes through this pair, and <see cref="TagTableName" /> applies the
    ///     identifier escaping.
    /// </remarks>
    internal static string TagTableNameFor(ITagTypeRegistration registration)
        => "pc_event_tag_" + registration.TableSuffix;

    /// <summary>
    ///     The schema-qualified, bracket-escaped DCB tag table name for a registered tag type.
    /// </summary>
    internal string TagTableName(ITagTypeRegistration registration)
        => Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, TagTableNameFor(registration));

    /// <summary>
    ///     The unqualified <c>pc_natural_key_*</c> lookup table name for a <c>[NaturalKey]</c> aggregate.
    /// </summary>
    /// <remarks>
    ///     Type-derived rather than caller-supplied, so not itself an injection risk — but #390's
    ///     one-builder-per-object-name rule applies regardless: five sites composed this name
    ///     independently, which is how two paths quietly stop agreeing.
    /// </remarks>
    internal static string NaturalKeyTableNameFor(Type aggregateType)
        => "pc_natural_key_" + aggregateType.Name.ToLowerInvariant();

    /// <summary>
    ///     The schema-qualified, bracket-escaped <c>pc_natural_key_*</c> table name for an aggregate.
    /// </summary>
    internal string NaturalKeyTableName(Type aggregateType)
        => Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, NaturalKeyTableNameFor(aggregateType));

    private ManagedTenantPartitions? _tenantPartitions;

    /// <summary>
    ///     The single Weasel.SqlServer managed per-tenant partition strategy (polecat#171) — maps each
    ///     tenant to a compact integer ordinal, owns the <c>pc_tenant_partitions</c> registry, and
    ///     physically partitions every managed table by that ordinal (RANGE RIGHT): <c>pc_events</c>
    ///     and <c>pc_streams</c> under <see cref="UseTenantPartitionedEvents" />, plus the
    ///     tenant-partitioned document tables (#335). One instance is shared between the table DDL and
    ///     the runtime partition split so Weasel can match each table to this strategy by reference —
    ///     one registry per database.
    /// </summary>
    internal ManagedTenantPartitions TenantPartitionManager =>
        _tenantPartitions ??= new ManagedTenantPartitions(
            "pc_events_tenants",
            new DbObjectName(DatabaseSchemaName, "pc_tenant_partitions"),
            column: "tenant_ordinal");

    private PolecatDatabase? _tenantPartitionDatabase;
    private TenantPartitionOrdinalRegistry? _tenantOrdinals;
    private TenantEventSequenceRegistry? _tenantSequences;

    /// <summary>
    ///     True when anything in the store partitions by tenant — the event store tables
    ///     (<see cref="UseTenantPartitionedEvents" />) or the document tables
    ///     (<see cref="StorePolicies.PartitionMultiTenantedDocumentsUsingPolecatManagement" />, #335).
    ///     Gates the shared <c>pc_tenant_partitions</c> registry feature schema.
    /// </summary>
    internal bool AnyTenantPartitioning =>
        UseTenantPartitionedEvents || _options.Policies.DocumentTenantPartitioningEnabled;

    /// <summary>
    ///     Wire the owning database so per-tenant provisioning can SPLIT physical partitions at runtime.
    ///     Set by <see cref="DocumentStore" /> during construction.
    /// </summary>
    internal void AttachTenantPartitionDatabase(PolecatDatabase database)
        => _tenantPartitionDatabase = database;

    /// <summary>
    ///     The store's single tenant → partition-ordinal registry (#335), shared by the append
    ///     planner, the stream-row SQL, the document write pipeline, and the runtime tenant
    ///     onboarding APIs. Conjoined tenancy is a precondition for any tenant partitioning, so all
    ///     tenants share the one configured connection/database.
    /// </summary>
    internal TenantPartitionOrdinalRegistry TenantOrdinals =>
        _tenantOrdinals ??= new TenantPartitionOrdinalRegistry(
            TenantPartitionManager,
            _tenantPartitionDatabase ?? throw new InvalidOperationException(
                "The owning database has not been attached for per-tenant partitioning."));

    /// <summary>
    ///     Resolves (and lazily provisions) each tenant's ordinal, physical partition, and per-tenant
    ///     event sequence when <see cref="UseTenantPartitionedEvents" /> is enabled.
    /// </summary>
    internal TenantEventSequenceRegistry TenantSequences =>
        _tenantSequences ??= new TenantEventSequenceRegistry(
            TenantOrdinals,
            _options.ConnectionString, DatabaseSchemaName, _options.ResiliencePipeline);

    /// <summary>
    ///     Validate the per-tenant partitioning configuration (#163 / polecat#171). Per-tenant event
    ///     sequencing only makes sense with conjoined event tenancy (there must be a tenant to slice
    ///     by), and it physically partitions <c>pc_events</c> by tenant — which a SQL Server table can
    ///     only do under one partition scheme, so it cannot also use the <c>is_archived</c> scheme.
    /// </summary>
    internal void AssertTenantPartitioningValidity()
    {
        if (_options.Policies.DocumentTenantPartitioningEnabled && TenancyStyle != TenancyStyle.Conjoined)
        {
            throw new InvalidOperationException(
                "Tenant-partitioned documents (StoreOptions.Policies.AllDocumentsAreMultiTenantedWithPartitioning / " +
                "PartitionMultiTenantedDocumentsUsingPolecatManagement) require Events.TenancyStyle = " +
                "TenancyStyle.Conjoined — there is nothing to partition by when every document lives in " +
                "the default tenant.");
        }

        if (!UseTenantPartitionedEvents) return;

        if (TenancyStyle != TenancyStyle.Conjoined)
        {
            throw new InvalidOperationException(
                "Events.UseTenantPartitionedEvents requires Events.TenancyStyle = TenancyStyle.Conjoined " +
                "— there is nothing to partition by when every event lives in the default tenant.");
        }

        if (UseArchivedStreamPartitioning)
        {
            throw new InvalidOperationException(
                "Events.UseTenantPartitionedEvents cannot be combined with " +
                "Events.UseArchivedStreamPartitioning — a SQL Server table supports only one partition " +
                "scheme, and per-tenant partitioning already uses it for pc_events.");
        }
    }

    public override EventAppendMode AppendMode
    {
        get => base.AppendMode;
        set => base.AppendMode = value;
    }

    private object? _closedShapeEventStorage;

    /// <summary>
    ///     The shared closed-shape <c>Weasel.Storage.EventStorage&lt;TId&gt;</c> for this event graph
    ///     (#273 event-dialect convergence), built once from <see cref="Storage.SqlServerEventStoreDialect" />
    ///     and cached. Boxed as <see cref="object" /> because <c>TId</c> is fixed by
    ///     <see cref="StreamIdentity" /> (Guid vs string) — the append planner downcasts to the concrete
    ///     <c>EventStorage&lt;Guid&gt;</c> / <c>EventStorage&lt;string&gt;</c>.
    /// </summary>
    internal object ClosedShapeEventStorage =>
        _closedShapeEventStorage ??= BuildClosedShapeEventStorage();

    private object BuildClosedShapeEventStorage()
    {
        var dialect = new Storage.SqlServerEventStoreDialect();
        var serializer = Serialization.StorageSerializerAdapter.For(Serializer);
        return StreamIdentity == StreamIdentity.AsGuid
            ? Weasel.Storage.EventStorageBuilder.Build<Guid>(dialect, AppendMode, this, serializer)
            : Weasel.Storage.EventStorageBuilder.Build<string>(dialect, AppendMode, this, serializer);
    }

    // #318: route the event-store auxiliary operations (archive / tombstone / progression) through the
    // shared Weasel.Storage.EventStorage<TId> seam instead of instantiating the operation classes at the
    // call sites. TId is fixed by StreamIdentity, so downcast the boxed storage accordingly; the
    // operations themselves are supplied by SqlServerEventStoreDialect.BuildAuxiliaryOperations.
    private Weasel.Storage.EventStorage<Guid> GuidEventStorage
        => (Weasel.Storage.EventStorage<Guid>)ClosedShapeEventStorage;

    private Weasel.Storage.EventStorage<string> StringEventStorage
        => (Weasel.Storage.EventStorage<string>)ClosedShapeEventStorage;

    internal Weasel.Storage.IStorageOperation ArchiveStreamOperation(object streamId, string tenantId, bool archived)
        => StreamIdentity == StreamIdentity.AsGuid
            ? GuidEventStorage.ArchiveStream(streamId, tenantId, archived)
            : StringEventStorage.ArchiveStream(streamId, tenantId, archived);

    internal Weasel.Storage.IStorageOperation TombstoneStreamOperation(object streamId, string tenantId)
        => StreamIdentity == StreamIdentity.AsGuid
            ? GuidEventStorage.TombstoneStream(streamId, tenantId)
            : StringEventStorage.TombstoneStream(streamId, tenantId);

    internal Weasel.Storage.IStorageOperation UpdateProgressOperation(string shardIdentity, long sequence, bool upsert)
        => StreamIdentity == StreamIdentity.AsGuid
            ? GuidEventStorage.UpdateProgress(shardIdentity, sequence, upsert)
            : StringEventStorage.UpdateProgress(shardIdentity, sequence, upsert);

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
        // Resolve the identity type the same way Projections.Snapshot<T>() does — by
        // inspecting TDoc's Id property via DocumentMapping. Strong-typed-id aggregates
        // (TDoc.Id is a wrapper struct over Guid / string / int / long) must be closed
        // with their *wrapper* type so the SG-emitted IGeneratedSyncEvolver<TDoc, TId>
        // dispatcher matches the runtime SingleStreamProjection<TDoc, TId> instance.
        // Falling back to typeof(Guid) / typeof(string) (the underlying stream-identity
        // primitive) misses the wrapper and trips the post-FEC fail-fast in
        // JasperFxAggregationProjectionBase.tryUseAssemblyRegisteredEvolver (JasperFx#276).
        var idType = ResolveAggregateIdType(typeof(TDoc));
        var projectionType = typeof(SingleStreamProjection<,>).MakeGenericType(typeof(TDoc), idType);
#pragma warning disable CS8714 // notnull constraint mismatch
        var projection = (ProjectionBase)Activator.CreateInstance(projectionType)!;
#pragma warning restore CS8714
        projection.Lifecycle = ProjectionLifecycle.Live;
        projection.AssembleAndAssertValidity();
        foreach (var et in projection.IncludedEventTypes) AddEventType(et);
        return projection as IAggregatorSource<IQuerySession>;
    }

    /// <summary>
    ///     The identity type to close <see cref="SingleStreamProjection{TDoc,TId}" /> over for a
    ///     conventionally-aggregated type.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     <see cref="BoundaryAggregateAttribute" /> is an explicit opt-out of single-stream
    ///     identity (polecat#521). A DCB boundary aggregate is <em>defined</em> by spanning streams
    ///     by tag rather than being keyed to one, so demanding an <c>Id</c> asks its author for a
    ///     member the model has no use for — and Polecat's own DCB docs told them they did not need
    ///     one. The marker is the source generator's own opt-in: it emits an
    ///     <c>IGeneratedSyncEvolver&lt;TDoc, string&gt;</c> for a marked identity-less type, so
    ///     <c>string</c> is the only answer that finds that dispatcher. It is vestigial — nothing on
    ///     the DCB path reads the id — but it has to agree with what the generator emitted.
    ///     </para>
    ///     <para>
    ///     Not inherited: the generator reads the attribute off the declaring type in its own
    ///     compilation, so a subclass that merely inherited the marker would resolve to
    ///     <c>string</c> and then fail to find a dispatcher of its own.
    ///     </para>
    ///     <para>
    ///     An identity-less aggregate WITHOUT the marker still throws, deliberately: the generator
    ///     emits nothing for one, and a missing <c>Id</c> is far more often an oversight than a
    ///     boundary aggregate. The message is re-thrown here rather than left to
    ///     <see cref="DocumentMapping" /> because that one is phrased for documents and never
    ///     mentions the marker that would fix this case.
    ///     </para>
    /// </remarks>
    private Type ResolveAggregateIdType(Type aggregateType)
    {
        if (aggregateType.GetCustomAttribute<BoundaryAggregateAttribute>(false) is not null)
        {
            return typeof(string);
        }

        try
        {
            return new DocumentMapping(aggregateType, _options).IdType;
        }
        catch (InvalidOperationException e)
        {
            throw new InvalidOperationException(
                $"Aggregate type '{aggregateType.FullName}' has no identity member. Single stream " +
                "aggregates need a public property named 'Id', or one marked with [Identity], of " +
                "type Guid, string, int, or long. An aggregate reached only through a DCB tag " +
                "boundary has no stream to be keyed to — mark it [BoundaryAggregate] " +
                "(JasperFx.Events.Aggregation) instead of giving it an identity it has no use for.",
                e);
        }
    }

    public override Type AggregateTypeFor(string aggregateTypeName)
    {
        if (_aggregateTypes.TryGetValue(aggregateTypeName, out var type)) return type;
        throw new ArgumentOutOfRangeException(nameof(aggregateTypeName),
            $"Unknown aggregate type name '{aggregateTypeName}'.");
    }

    /// <summary>
    ///     #370/#373: resolve the alias persisted in <c>pc_streams.type</c> back to its aggregate type, or
    ///     null when this deployment has no registration for it. The single answer to "what type is this
    ///     alias" — <see cref="StreamState.AggregateType" /> hydration and the event store explorer's
    ///     rehydrate-by-name path both come through here.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     Unlike <see cref="AggregateTypeFor" /> this never throws. A stream tagged by a deployment that
    ///     knew a type this one does not must still report its version and timestamps, and the explorer
    ///     turns the null into its own argument exception with a message aimed at that caller.
    ///     </para>
    ///     <para>
    ///     Two sources, in order. The alias registry (<c>_aggregateTypes</c>) is authoritative for anything
    ///     this process wrote, because #373 made the stream insert go through
    ///     <see cref="AggregateAliasFor" /> as it stamps the column. The registered projections cover
    ///     everything else — streams written by another process or an earlier run, where nothing has
    ///     populated the registry yet.
    ///     </para>
    ///     <para>
    ///     Note the alias is the SIMPLE name, because that is what the column stores. Two aggregate types
    ///     with the same <c>Name</c> in different namespaces therefore share an alias and the first one
    ///     seen wins. That ambiguity lives in the persisted format, not here — resolving it would mean
    ///     changing what the column holds.
    ///     </para>
    /// </remarks>
    internal Type? TryResolveAggregateType(string? aggregateTypeName)
    {
        if (string.IsNullOrEmpty(aggregateTypeName)) return null;

        if (_aggregateTypes.TryGetValue(aggregateTypeName, out var known)) return known;

        foreach (var source in _options.Projections.All)
        {
            foreach (var published in source.PublishedTypes())
            {
                if (string.Equals(published.Name, aggregateTypeName, StringComparison.Ordinal)
                    || string.Equals(published.FullName, aggregateTypeName, StringComparison.Ordinal))
                {
                    // Cache it so the next row on this reader is a dictionary hit.
                    _aggregateTypes.TryAdd(aggregateTypeName, published);
                    return published;
                }
            }
        }

        return null;
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

    // ---- #388 / #475: pluggable binary event serialization ------------------------------------
    //
    // Explicit per-type registrations from UseBinarySerializer<TEvent>(...). Types marked with
    // [BinaryEvent] but not registered explicitly fall back to DefaultBinarySerializer; that
    // resolution is cached in _binarySerializerResolution so the attribute probe (and the throw for a
    // misconfigured type) happens once per event type rather than once per row.
    //
    // #475: the contract is JasperFx.Events.IEventBinarySerializer, promoted out of Marten in
    // JasperFx 2.50.0 so one serializer implementation serves every Critter Stack store rather than
    // one identical copy per flavour. Polecat's own IEventBinarySerializer / BinaryEventAttribute
    // from #388 are GONE rather than kept as deriving aliases: a consumer with both JasperFx.Events
    // and Polecat.Events in scope -- which is every consumer this promotion exists for -- got CS0104
    // on the bare name, so keeping them broke the shape they were kept for. Proven, not theorized:
    // Polecat's own binary_event_serialization_tests failed exactly that way.
    private readonly ConcurrentDictionary<Type, JasperFx.Events.IEventBinarySerializer> _binarySerializerByType = new();
    private readonly ConcurrentDictionary<Type, JasperFx.Events.IEventBinarySerializer?> _binarySerializerResolution = new();

    /// <summary>
    ///     Store-wide fallback <see cref="JasperFx.Events.IEventBinarySerializer" /> used for event
    ///     types marked with <see cref="JasperFx.Events.BinaryEventAttribute" /> that have no explicit
    ///     per-type registration. Null by default, which leaves every event type on the JSON path.
    /// </summary>
    public JasperFx.Events.IEventBinarySerializer? DefaultBinarySerializer
    {
        get => _defaultBinarySerializer;
        set
        {
            _defaultBinarySerializer = value;
            _binarySerializerResolution.Clear();
        }
    }

    private JasperFx.Events.IEventBinarySerializer? _defaultBinarySerializer;

    /// <summary>
    ///     Opt <typeparamref name="TEvent" /> into binary serialization (#388): its payload is written
    ///     to the <c>bdata</c> column instead of <c>data</c>, and read back through the same
    ///     serializer. Wins over <c>[BinaryEvent]</c> + <see cref="DefaultBinarySerializer" />.
    /// </summary>
    public EventGraph UseBinarySerializer<TEvent>(JasperFx.Events.IEventBinarySerializer serializer)
        where TEvent : notnull
    {
        ArgumentNullException.ThrowIfNull(serializer);
        _binarySerializerByType[typeof(TEvent)] = serializer;
        _binarySerializerResolution.Clear();
        return this;
    }

    /// <summary>
    ///     The <see cref="JasperFx.Events.IEventBinarySerializer" /> governing
    ///     <paramref name="eventType" />, or null when that type stays on the JSON path. Explicit
    ///     registration beats <c>[BinaryEvent]</c> + <see cref="DefaultBinarySerializer" />.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     The type carries <c>[BinaryEvent]</c> but no serializer is configured for it.
    ///     Deliberately a throw rather than a silent fall back to JSON: a store that quietly ignored
    ///     the attribute would have write-amplification characteristics that do not match its
    ///     configuration, which is the whole reason the feature exists.
    /// </exception>
    internal JasperFx.Events.IEventBinarySerializer? ResolveBinarySerializerFor(Type eventType)
        => _binarySerializerResolution.GetOrAdd(eventType, static (type, graph) =>
        {
            if (graph._binarySerializerByType.TryGetValue(type, out var explicitSerializer))
            {
                return explicitSerializer;
            }

            if (!type.IsDefined(typeof(JasperFx.Events.BinaryEventAttribute), inherit: false))
            {
                return null;
            }

            return graph._defaultBinarySerializer ?? throw new InvalidOperationException(
                $"Event type '{type.FullName}' is marked with [BinaryEvent] but no IEventBinarySerializer "
                + $"is registered. Either call opts.Events.UseBinarySerializer<{type.Name}>(...) explicitly, "
                + "or set opts.Events.DefaultBinarySerializer to a store-wide fallback.");
        }, this);

    /// <summary>
    ///     The <c>bdata</c> bytes for an event on the write path, or null when the event's type stays
    ///     on the JSON path. The two are mutually exclusive per row — see
    ///     <see cref="JsonPlaceholderForBinaryEvent" />.
    /// </summary>
    internal byte[]? SerializeEventBdata(IEvent @event)
    {
        // Deliberately NOT short-circuited on UsesBinaryEventSerialization: a type marked
        // [BinaryEvent] in a store with no serializer configured has to throw here, and skipping the
        // resolve for "performance" would turn that into a silent write of JSON. The resolve is a
        // ConcurrentDictionary hit cached per event type, so a JSON-only store pays one lookup per
        // appended event and nothing more.
        var eventType = @event.EventType ?? @event.Data.GetType();
        var serializer = ResolveBinarySerializerFor(eventType);
        return serializer?.Serialize(eventType, @event.Data);
    }

    /// <summary>
    ///     What goes in the <c>data</c> column of a binary event's row. An empty JSON object rather
    ///     than NULL, because <c>data</c> is NOT NULL and typed <c>json</c> on SQL Server 2025 — a
    ///     row still has to hold something the engine will parse.
    /// </summary>
    internal const string JsonPlaceholderForBinaryEvent = "{}";

    /// <summary>
    ///     The per-row read counterpart of <see cref="SerializeEventBdata" />: <paramref name="bdata" />
    ///     being non-null is the on-row discriminator, so JSON rows written before the feature was
    ///     switched on keep deserializing through <paramref name="serializer" /> unchanged.
    /// </summary>
    internal object DeserializeEventData(Type resolvedType, string json, byte[]? bdata, ISerializer serializer)
    {
        if (bdata is null) return serializer.FromJson(resolvedType, json);

        var binary = ResolveBinarySerializerFor(resolvedType)
                     ?? throw new InvalidOperationException(
                         $"A pc_events row for '{resolvedType.FullName}' has a non-null bdata column but no "
                         + "IEventBinarySerializer is registered for that type. Configure it with "
                         + $"opts.Events.UseBinarySerializer<{resolvedType.Name}>(...) or set "
                         + "opts.Events.DefaultBinarySerializer — the event cannot be read without it.");

        return binary.Deserialize(resolvedType, bdata);
    }

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

    internal DcbTagVersionTable BuildDcbTagVersionTable()
    {
        return new DcbTagVersionTable(this);
    }

    /// <summary>The schema-qualified name of the DCB tag-version side table (gh-515).</summary>
    internal string DcbTagVersionTableName
        => Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, DcbTagVersionTable.TableName);

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
            // |= and NOT ||=. With the short-circuiting form, the first rule to match an event
            // stopped every later rule from even being invoked, so only one of several applicable
            // rules ever ran -- and the operation still reported success. For a right-to-erasure
            // feature that means protected information silently survives a masking pass. See #422.
            matched |= masker.TryMask(e);
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
