using System.Data.Common;
using System.Buffers;
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using JasperFx.Events.Upcasting;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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

    // jasperfx#1043 (polecat#806): the masking rules themselves live on EventRegistry, which exposes no
    // way to list them, so the rule types are recorded here for AssertMaskingRulesCanRun (#860).
    private readonly List<Type> _maskedEventTypes = new();
    private readonly Dictionary<Type, Type> _upcastSources = new();

    internal EventGraph(StoreOptions options)
    {
        _options = options;
        AppendMode = EventAppendMode.Quick;

        // pc_streams is keyed by a clustered uniqueidentifier, so the Guid stream ids JasperFx makes up
        // (StartStream without an id, and EventBoundary's new streams) are in SQL Server order
        StreamIdGenerator = SequentialGuid.SqlServerOrder;
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

    /// <summary>#935: whether the key and metadata columns are <c>nvarchar</c>.</summary>
    internal bool UnicodeKeys => _options.Advanced.UseUnicodeKeys;

    /// <summary>#935: the SQL type of a key or metadata column, at <paramref name="length" /> characters.</summary>
    internal string KeyText(int length = Polecat.Storage.KeyColumns.Width) => _options.KeyText(length);

    /// <summary>#935: how to bind a value compared against a key or metadata column.</summary>
    internal System.Data.SqlDbType KeyDbType => _options.KeyDbType;

    /// <inheritdoc cref="StoreOptions.ToStorageJson" />
    internal string ToStorageJson(object value) => _options.ToStorageJson(value);

    internal EventStoreOptions EventOptions => _options.Events;

    internal string JsonColumnType => _options.JsonColumnType;

    /// <summary>
    ///     Whether extended progression tracking columns are enabled.
    /// </summary>
    public bool EnableExtendedProgressionTracking => _options.Events.EnableExtendedProgressionTracking;

    /// <summary>
    ///     #813: whether pc_event_projection_floors is part of the schema. See
    ///     <see cref="EventStoreOptions.EnableProjectionFloors" />.
    /// </summary>
    public bool EnableProjectionFloors => _options.Events.EnableProjectionFloors;

    /// <summary>
    ///     #1054: whether skipped high water ranges are recorded and checked for late events. See
    ///     <see cref="EventStoreOptions.EnableLateEventDetection" />.
    /// </summary>
    public bool EnableLateEventDetection => _options.Events.EnableLateEventDetection;

    /// <summary>
    ///     jasperfx#1215 / #1222: whether pc_event_range_marks is part of the schema. See
    ///     <see cref="EventStoreOptions.EnableEventRangeMarks" />.
    /// </summary>
    public bool EnableEventRangeMarks => _options.Events.EnableEventRangeMarks;

    /// <summary>
    ///     marten#6003 / #1053: whether pc_parked_identities is part of the schema. See
    ///     <see cref="EventStoreOptions.EnableParkedIdentities" />.
    /// </summary>
    public bool EnableParkedIdentities => _options.Events.EnableParkedIdentities;

    /// <summary>
    ///     marten#6003 / #1053: can this store write and read a projection's slice index? It needs the parked identities
    ///     and the event-range marks, where the index's start is kept.
    /// </summary>
    internal bool SupportsSliceIndex => EnableParkedIdentities && EnableEventRangeMarks;

    internal TimeSpan LateEventDetectionWindow => _options.Projections.LateEventDetectionWindow;

    internal TimeSpan LateEventProbeInterval => _options.Projections.LateEventProbeInterval;

    internal Polecat.Internal.OpenTelemetry.OpenTelemetryOptions OpenTelemetry => _options.OpenTelemetry;

    /// <summary>
    ///     #973: whether appends maintain a <c>pc_dcb_tag_version</c> row per (tag value, event type) as well
    ///     as the one per tag value. See <see cref="DcbConflictScope" />.
    /// </summary>
    internal bool RecordsDcbEventTypeVersions => _options.Events.DcbConflictScope != DcbConflictScope.TagValue;

    /// <summary>
    ///     #973: whether a boundary condition naming an event type checks that type's own version rather
    ///     than the tag value's. See <see cref="DcbConflictScope" />.
    /// </summary>
    internal bool ChecksDcbEventTypeVersions =>
        _options.Events.DcbConflictScope == DcbConflictScope.TagValueAndEventType;

    /// <summary>
    ///     #905: the tenant that narrows a search of a unit of work for a stream. Under conjoined tenancy a
    ///     stream id is only unique within a tenant, and <c>ForTenant</c> puts several tenants in one save,
    ///     so it is the stream's own. Otherwise the id is the whole key of pc_streams and two tenants naming
    ///     it are naming the same stream, so there is nothing to narrow by and this is null.
    /// </summary>
    internal string? StreamIdScope(string tenantId) => TenancyStyle == TenancyStyle.Conjoined ? tenantId : null;

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
    /// <remarks>
    ///     Off by default, as in Polecat 5 and Marten. The first schema migration after you turn it on moves an
    ///     existing pc_events onto the partition scheme in place, which rewrites the table (#1041). It cannot be
    ///     combined with <see cref="UseTenantPartitionedEvents" />, because a SQL Server table has only one
    ///     partition scheme and per-tenant partitioning uses it; setting both is refused. See the archiving docs
    ///     for what it costs and when it is worth it.
    /// </remarks>
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
    ///     projections under the Inline lifecycle. On by default since Polecat 6 — it
    ///     routes inline-projection-emitted messages through the configured
    ///     <see cref="StoreOptions.MessageOutbox"/>. Inline appended events
    ///     (slice.AppendEvent) are not supported and will throw at runtime.
    /// </summary>
    // Read from the configured StoreOptions.Events so it can be set in AddPolecat(...)/DocumentStore.For
    // config (m.Events.EnableSideEffectsOnInlineProjections = true), mirroring EnableCorrelationId etc.
    // and Marten's StoreOptions.Events.EnableSideEffectsOnInlineProjections.
    public bool EnableSideEffectsOnInlineProjections => _options.Events.EnableSideEffectsOnInlineProjections;

    internal string StreamsTableName => Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, "pc_streams");
    private string? _quickAppendPrefix;
    private string? _quickAppendPrefixPartitioned;

    /// <summary>
    ///     The static text of a QuickAppend event insert, from <c>insert into</c> through
    ///     <c>values (</c>. It depends only on options that are fixed once the store serves sessions, so
    ///     it is built once per shape rather than once per appended event (#802). Bound parameters
    ///     stay per event; only this text is shared.
    /// </summary>
    internal string QuickAppendInsertPrefix(bool partitioned)
    {
        if (partitioned)
            return _quickAppendPrefixPartitioned ??= buildQuickAppendInsertPrefix(true);
        return _quickAppendPrefix ??= buildQuickAppendInsertPrefix(false);
    }

    private string buildQuickAppendInsertPrefix(bool partitioned)
    {
        var sb = new System.Text.StringBuilder("insert into ");
        sb.Append(EventsTableName).Append(" (");
        if (partitioned) sb.Append("seq_id, ").Append(TenantPartitionManager.Column).Append(", ");
        sb.Append("id, stream_id, version, data, bdata, type, timestamp, tenant_id, dotnet_type");
        if (EventOptions.EnableCorrelationId) sb.Append(", correlation_id");
        if (EventOptions.EnableCausationId) sb.Append(", causation_id");
        if (EventOptions.EnableHeaders) sb.Append(", headers");
        if (EventOptions.EnableUserName) sb.Append(", user_name");
        sb.Append(") output inserted.seq_id into ").Append(Polecat.Events.Storage.PolecatQuickAppendEventsOperation.SeqTableVarName);
        sb.Append(" values (");
        return sb.ToString();
    }

    internal string EventsTableName => Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, "pc_events");

    internal string ProgressionTableName =>
        Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, "pc_event_progression");

    // #813 (jasperfx#809): the persisted projection start floors. See EventProjectionFloorsTable.
    internal string ProjectionFloorsTableName =>
        Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, Schema.EventProjectionFloorsTable.TableName);

    // jasperfx#1215 / #1222: the ceilings and cursors of rewrites and backfills. See EventRangeMarksTable.
    internal string EventRangeMarksTableName =>
        Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, Schema.EventRangeMarksTable.TableName);

    // marten#6003 / #1053: the parked identities of async projections. See ParkedIdentitiesTable.
    internal string ParkedIdentitiesTableName =>
        Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, Schema.ParkedIdentitiesTable.TableName);

    // #1054: the skipped high water ranges and the events that committed inside them late
    internal string HighWaterSkipsTableName =>
        Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, Schema.HighWaterSkipsTable.TableName);

    internal string HighWaterLateEventsTableName =>
        Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, Schema.HighWaterLateEventsTable.TableName);

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
    ///     Whether deleting a <c>pc_events</c> row takes its tag rows through the tag table's foreign key.
    ///     True everywhere except under <see cref="UseArchivedStreamPartitioning" />, where the tag tables
    ///     have no foreign key (see <see cref="Schema.EventTagTable" />) and whatever deletes events has
    ///     to delete their tag rows itself, or they are orphaned (#841).
    /// </summary>
    internal bool TagRowsCascadeWithEvents => !UseArchivedStreamPartitioning;

    /// <summary>
    ///     Whether deleting a <c>pc_streams</c> row takes its natural key rows through the lookup table's
    ///     foreign key. True everywhere except under conjoined tenancy, where the table has no foreign key
    ///     (see <see cref="Schema.NaturalKeyTable" />) and whatever deletes a stream has to delete its
    ///     lookup rows itself, or the key stays mapped to a stream that no longer exists (#865).
    /// </summary>
    internal bool NaturalKeyRowsCascadeWithStreams => TenancyStyle != TenancyStyle.Conjoined;

    /// <summary>
    ///     The join between a tag row and its event. <c>seq_id</c> alone does not identify an event: under
    ///     <see cref="UseTenantPartitionedEvents" /> every tenant numbers its own events from 1, so a bare
    ///     join reads across tenants (#842). Everything that joins the two goes through this.
    /// </summary>
    internal string TagToEventJoin(string tagAlias, string eventAlias)
    {
        var join = $"{eventAlias}.seq_id = {tagAlias}.seq_id";

        if (TenancyStyle == TenancyStyle.Conjoined)
        {
            join += $" AND {eventAlias}.tenant_id = {tagAlias}.tenant_id";
        }

        if (UseTenantPartitionedEvents)
        {
            var ordinal = TenantPartitionManager.Column;
            join += $" AND {eventAlias}.{ordinal} = {tagAlias}.{ordinal}";
        }

        return join;
    }

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

    /// <summary>
    ///     Every registered natural key definition — one per aggregate projection that declares a
    ///     <c>[NaturalKey]</c> property, and therefore one lookup table each.
    /// </summary>
    /// <remarks>
    ///     #556: needed by the explicit archive path, which has a stream identity but no event to
    ///     route on, so it has to touch every lookup table rather than the one an event's aggregate
    ///     type would have named. Reads the same registered projections
    ///     <c>BuildInlineProjections</c> derives its <c>NaturalKeyProjection</c> instances from, so
    ///     the two paths cannot drift apart over which tables exist.
    /// </remarks>
    internal IEnumerable<NaturalKeyDefinition> NaturalKeyDefinitions()
        => _options.Projections.All
            .OfType<IAggregateProjection>()
            .Select(x => x.NaturalKeyDefinition)
            .Where(x => x != null)!;

    /// <summary>
    ///     #1134 (marten#6144): since Polecat 6, side effects run on inline projections by default, inside the
    ///     <c>SaveChangesAsync()</c> that touches the projection. In Polecat 5 a <c>RaiseSideEffects()</c> override on
    ///     an Inline projection never ran, so an upgraded application can start publishing messages on commit without
    ///     having changed a line. Said once per projection when the store is built, so it shows up in the
    ///     application's own log.
    /// </summary>
    internal void WarnAboutInlineSideEffects(ILogger logger)
    {
        foreach (var name in InlineProjectionsThatRaiseSideEffects())
        {
            logger.LogWarning(
                "Inline projection {Projection} overrides RaiseSideEffects(), so its side effects run inside every SaveChangesAsync() that touches it, in the same transaction, and any messages it publishes go out with that commit. This is the Polecat 6 default; in Polecat 5 the override never ran for an Inline projection. Set opts.Events.EnableSideEffectsOnInlineProjections = false to keep the Polecat 5 behavior",
                name);
        }
    }

    /// <summary>
    ///     The names of the Inline projections whose side effects run on commit: none unless
    ///     <see cref="EnableSideEffectsOnInlineProjections" /> is on, and then each one that overrides
    ///     <c>RaiseSideEffects()</c> itself.
    /// </summary>
    internal IReadOnlyList<string> InlineProjectionsThatRaiseSideEffects()
    {
        if (!EnableSideEffectsOnInlineProjections) return [];

        return _options.Projections.All
            .OfType<ProjectionBase>()
            .Where(x => x.Lifecycle == ProjectionLifecycle.Inline && overridesRaiseSideEffects(x.GetType()))
            .Select(x => x.Name)
            .ToList();
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070",
        Justification = "Projection types are registered by the application and preserved per the AOT publishing guide. A trimmed RaiseSideEffects override only means the startup warning is not written.")]
    private static bool overridesRaiseSideEffects(Type projectionType)
    {
        try
        {
            // An override declared by the application's own projection, not one inherited from JasperFx or Polecat
            return projectionType
                .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .Any(m => m.Name == "RaiseSideEffects"
                          && m.DeclaringType != m.GetBaseDefinition().DeclaringType
                          && m.DeclaringType!.Assembly != typeof(ProjectionBase).Assembly
                          && m.DeclaringType.Assembly != typeof(EventGraph).Assembly);
        }
        catch (Exception)
        {
            // Only feeds a warning, so a reflection failure must never stop the store from building
            return false;
        }
    }

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
        LazyInitializer.EnsureInitialized(ref _tenantPartitions, ref _tenantPartitionLock, buildTenantPartitionManager);

    private ManagedTenantPartitions buildTenantPartitionManager()
    {
        var partitions = new ManagedTenantPartitions(
            "pc_events_tenants",
            new DbObjectName(DatabaseSchemaName, "pc_tenant_partitions"),
            column: "tenant_ordinal");

        if (UnicodeKeys)
        {
            // #1042: Weasel declares the registry's tenant_id and bucket as varchar(200), so two tenant ids
            // outside the database's code page (東京 and 大阪 are both ?? there) would share one ordinal, and
            // the partition, high water marks and sequences keyed through it. Weasel has no setting for it, so
            // widen the registry's own table here, at the width Weasel gives it. Weasel binds its registry
            // parameters with AddWithValue, which is already nvarchar. An existing varchar registry migrates
            // like Polecat's other key columns.
            var registry = partitions.Objects.OfType<Weasel.SqlServer.Tables.Table>()
                .Single(x => x.Identifier.Equals(partitions.RegistryTableIdentifier));
            registry.ColumnFor("tenant_id")!.Type = "nvarchar(200)";
            registry.ColumnFor("bucket")!.Type = "nvarchar(200)";
        }

        return partitions;
    }

    private PolecatDatabase? _tenantPartitionDatabase;
    private TenantPartitionOrdinalRegistry? _tenantOrdinals;
    private TenantEventSequenceRegistry? _tenantSequences;

    // The three above are built on first use by concurrent sessions, and each holds a cache a second
    // copy wouldn't share. Two sessions starting at once used to build one each, so a tenant resolved
    // through one was missing from the other, and an append failed with "no resolved partition ordinal".
    private object? _tenantPartitionLock;

    /// <summary>
    ///     #1057: the name of the function that answers a tenant's partition ordinal.
    /// </summary>
    internal const string TenantOrdinalFunctionName = "pc_tenant_ordinal";

    /// <summary>
    ///     #1057: whether any table of the application's own is partitioned per tenant
    ///     (<see cref="Polecat.Storage.StorageFeatures.PartitionPerTenant" />).
    /// </summary>
    internal bool HasTablesPartitionedPerTenant => _options.Storage.TablesPartitionedPerTenant.Count > 0;

    /// <summary>
    ///     #1057: <c>{schema}.pc_tenant_ordinal(@tenant_id)</c>, the tenant's partition ordinal from the
    ///     <c>pc_tenant_partitions</c> registry, or <c>NULL</c> for a tenant that has none yet.
    /// </summary>
    internal Weasel.SqlServer.Functions.Function BuildTenantOrdinalFunction()
    {
        var name = Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, TenantOrdinalFunctionName);
        return new Weasel.SqlServer.Functions.Function(
            new Weasel.SqlServer.SqlServerObjectName(DatabaseSchemaName, TenantOrdinalFunctionName),
            $"""
             CREATE FUNCTION {name}(@tenant_id {KeyText()})
             RETURNS int
             AS
             BEGIN
                 RETURN (SELECT ordinal FROM {TenantPartitionsTableName} WHERE tenant_id = @tenant_id);
             END
             """);
    }

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
        LazyInitializer.EnsureInitialized(ref _tenantOrdinals, ref _tenantPartitionLock, () =>
            new TenantPartitionOrdinalRegistry(
                TenantPartitionManager,
                _tenantPartitionDatabase ?? throw new InvalidOperationException(
                    "The owning database has not been attached for per-tenant partitioning."),
                _options,
                _options.DotNetLogger ?? _options.LogFactory?.CreateLogger<TenantPartitionOrdinalRegistry>()
                ?? NullLogger<TenantPartitionOrdinalRegistry>.Instance));

    /// <summary>
    ///     Resolves (and lazily provisions) each tenant's ordinal, physical partition, and per-tenant
    ///     event sequence when <see cref="UseTenantPartitionedEvents" /> is enabled.
    /// </summary>
    internal TenantEventSequenceRegistry TenantSequences =>
        LazyInitializer.EnsureInitialized(ref _tenantSequences, ref _tenantPartitionLock, () =>
            new TenantEventSequenceRegistry(
                TenantOrdinals,
                _options.ConnectionString, DatabaseSchemaName, _options.ResiliencePipeline));

    /// <summary>
    ///     #852: see <see cref="PolecatProjectionOptions.SkipStaleGapsDespiteLiveTransactionsAfter" />.
    /// </summary>
    internal TimeSpan? SkipStaleGapsDespiteLiveTransactionsAfter =>
        _options.Projections.SkipStaleGapsDespiteLiveTransactionsAfter;

    /// <summary>
    ///     The error number <see cref="LockedStreamReadSql" /> throws when it can't take the lock on a
    ///     stream that doesn't exist yet (a lock timeout, or the lock request chosen as a deadlock victim).
    ///     Like 1222, it ends the statement and leaves the transaction open.
    /// </summary>
    internal const int StreamLockFailure = 51072;

    /// <summary>
    ///     The read of stream rows that a version check rests on: locked until the transaction ends, so
    ///     that nothing can move the version it returns, or start a stream it found missing.
    /// </summary>
    /// <param name="columns">The columns to select, from the alias <c>s</c> if there are <paramref name="ids" />.</param>
    /// <param name="match">
    ///     The predicate naming the rows, on the same terms. It names the tenant as <c>@tenant_id</c>, and
    ///     without <paramref name="ids" /> the stream as <c>@id</c>.
    /// </param>
    /// <param name="tenantId">The tenant in <paramref name="match" />.</param>
    /// <param name="command">Takes the ordinal's parameter, when there is one.</param>
    /// <param name="ids">
    ///     A query for several streams' ids, in a column called <c>id</c>, if <paramref name="match" /> does
    ///     not name one stream.
    /// </param>
    /// <remarks>
    ///     <para>
    ///         <b>#1072: a stream that exists is locked by its row, and one that doesn't by its id.</b> The
    ///         read used to be <c>UPDLOCK, HOLDLOCK</c>. For a missing row <c>HOLDLOCK</c> locks the range up
    ///         to the next key in the index, so an append creating stream B waited on an open append creating
    ///         an unrelated stream A whenever A was B's next key. Now an existing row is read with
    ///         <c>UPDLOCK</c> alone, held to the end of the transaction, and a stream that isn't there takes a
    ///         transaction-owned <c>sp_getapplock</c> on its own tenant and id before the read. Two appends
    ///         creating the same stream still take turns (#852): the second waits on the lock, then finds
    ///         the first one's row. Rows are locked first, in the order of the ids, and then the ids that
    ///         are missing, in the order of their lock names, so two saves over the same streams take their
    ///         locks in the same order.
    ///     </para>
    ///     <para>
    ///         The lock name is a hash of the streams table, the tenant and the id, upper-cased, because
    ///         <c>sp_getapplock</c> compares names in binary and the id column follows the database's
    ///         collation. Two ids that differ only by case share a lock, which costs nothing but a wait.
    ///         Outside a transaction no lock is taken: each statement would release it as it finished.
    ///     </para>
    ///     <para>
    ///         <b>#852: the read has to reach each row by its whole key.</b> Under
    ///         <see cref="UseTenantPartitionedEvents" /> the key is <c>(tenant_id, id, tenant_ordinal)</c>, so
    ///         <c>(tenant_id, id)</c> is a range, walked once per partition. The locked read names the
    ///         ordinal. Several streams at once read as <c>id IN (...)</c> left the plan to the optimizer,
    ///         and on any conjoined store it walked the tenant, so the loop join is what guarantees one
    ///         singleton per id. It also takes them in the order the ids were sorted into. The missing ids are
    ///         found the same way, by locking the rows that are there and taking the rest away from the ids,
    ///         not with a join to the rows that aren't: SQL Server turned that into an anti semi join that
    ///         sought the tenant alone, and waited on an open append's row in it.
    ///     </para>
    ///     <para>
    ///         <b>The second branch is not there for the lock.</b> A stream row keeps the ordinal it was
    ///         written with, and a tenant removed with <c>RetainData</c> and added back after its ordinal
    ///         went to somebody else gets a new one. Finding nothing under the new ordinal would make that
    ///         stream look new, and the append would fork it, numbering from 1 again. An append does move
    ///         such a row, in the second update in <c>SqlServerEventStoreDialect</c>, but that update names
    ///         the version this read returned, and if it finds no row the append fails. So <c>NOLOCK</c>
    ///         can't lose a version here. Two appends racing on such a stream can both get past this read,
    ///         and the loser then fails, on the event's unique index or on that check. Such a stream takes
    ///         the lock on its id as well, as one missing from its tenant's partition.
    ///     </para>
    ///     <para>
    ///         An ordinal this process has not resolved yet reads every partition. Looking it up here could
    ///         mean DDL with this transaction open, which is #864.
    ///     </para>
    /// </remarks>
    internal string LockedStreamReadSql(string columns, string match, string tenantId, SqlCommand command,
        string? ids = null)
    {
        var withOrdinal = false;
        if (UseTenantPartitionedEvents && TenantOrdinals.TryGetOrdinal(tenantId, out var ordinal))
        {
            withOrdinal = true;
            command.Parameters.Add("@tenant_ordinal", SqlDbType.Int).Value = ordinal;
        }

        // #1128: the text depends on nothing else, and building it cost about 9 KB of strings on every
        // append. The call sites pass a handful of fixed combinations, so the cache stays that small.
        var key = (DatabaseSchemaName, columns, match, ids, withOrdinal);
        if (_lockedStreamReads.TryGetValue(key, out var sql)) return sql;

        sql = buildLockedStreamReadSql(columns, match, ids, withOrdinal);
        _lockedStreamReads[key] = sql;
        return sql;
    }

    private readonly ConcurrentDictionary<(string schema, string columns, string match, string? ids, bool withOrdinal), string>
        _lockedStreamReads = new();

    private string buildLockedStreamReadSql(string columns, string match, string? ids, bool withOrdinal)
    {
        string from(string hint) => ids == null
            ? $"{StreamsTableName} WITH ({hint})"
            : $"({ids}) ids INNER LOOP JOIN {StreamsTableName} s WITH ({hint}) ON s.id = ids.id";

        var ordinalPredicate = "";
        string read;

        if (withOrdinal)
        {
            var column = (ids == null ? "" : "s.") + TenantPartitionManager.Column;
            ordinalPredicate = $" AND {column} = @tenant_ordinal";

            read = $"""
                SELECT {columns} FROM {from("UPDLOCK")} WHERE {match}{ordinalPredicate}
                UNION ALL
                SELECT {columns} FROM {from("NOLOCK")} WHERE {match} AND {column} <> @tenant_ordinal;
                """;
        }
        else
        {
            read = $"SELECT {columns} FROM {from("UPDLOCK")} WHERE {match};";
        }

        // sp_getapplock reports a timeout or a deadlock through its return value rather than an error, so a
        // negative answer is thrown here: going on without the lock would let two appends create one stream.
        var table = Polecat.Internal.SqlEscaping.LiteralBody(StreamsTableName);
        var takeLock = $"""
                EXEC @pc_stream_lock = sp_getapplock @Resource = @pc_stream_lock_name, @LockMode = 'Exclusive',
                    @LockOwner = 'Transaction';
                IF @pc_stream_lock < 0
                BEGIN
                    DECLARE @pc_stream_lock_failure nvarchar(2048) = N'Polecat could not lock a new stream in {table} '
                        + N'(sp_getapplock answered ' + CONVERT(nvarchar(11), @pc_stream_lock) + N').';
                    THROW {StreamLockFailure}, @pc_stream_lock_failure, 1;
                END;
            """;

        if (ids == null)
        {
            return $"""
                DECLARE @pc_stream_lock int, @pc_stream_lock_name nvarchar(255);
                IF @@TRANCOUNT > 0
                   AND NOT EXISTS (SELECT 1 FROM {from("UPDLOCK")} WHERE {match}{ordinalPredicate})
                BEGIN
                    SET @pc_stream_lock_name = {streamLockName("@id")};
                {takeLock}
                END;
                {read}
                """;
        }

        return $"""
            DECLARE @pc_stream_lock int, @pc_stream_lock_name nvarchar(255);
            IF @@TRANCOUNT > 0
            BEGIN
                DECLARE @pc_streams_found TABLE (name nvarchar(255) NOT NULL PRIMARY KEY);
                INSERT INTO @pc_streams_found (name)
                SELECT DISTINCT {streamLockName("s.id")} FROM {from("UPDLOCK")} WHERE {match}{ordinalPredicate};

                DECLARE @pc_stream_locks TABLE (name nvarchar(255) NOT NULL PRIMARY KEY);
                INSERT INTO @pc_stream_locks (name)
                SELECT DISTINCT missing.name
                FROM (SELECT {streamLockName("ids.id")} AS name FROM ({ids}) ids) missing
                WHERE missing.name NOT IN (SELECT name FROM @pc_streams_found);

                SET @pc_stream_lock_name = (SELECT MIN(name) FROM @pc_stream_locks);
                WHILE @pc_stream_lock_name IS NOT NULL
                BEGIN
                {takeLock}
                    SET @pc_stream_lock_name = (SELECT MIN(name) FROM @pc_stream_locks WHERE name > @pc_stream_lock_name);
                END;
            END;
            {read}
            """;
    }

    /// <summary>
    ///     The <c>sp_getapplock</c> name for a stream of <c>@tenant_id</c> whose id is
    ///     <paramref name="id" />, for <see cref="LockedStreamReadSql" />.
    /// </summary>
    private string streamLockName(string id)
        => $"N'polecat:stream:' + CONVERT(nvarchar(64), HASHBYTES('SHA2_256', UPPER(CONCAT(N'{Polecat.Internal.SqlEscaping.LiteralBody(StreamsTableName)}', NCHAR(1), @tenant_id, NCHAR(1), CONVERT(nvarchar(450), {id})))), 2)";

    /// <summary>
    ///     The statement that reads one stream's events.
    /// </summary>
    /// <param name="select">The query as far as its stream and tenant predicate.</param>
    /// <param name="rest">What follows that predicate, through the <c>ORDER BY</c>.</param>
    /// <param name="stream">Something naming the stream id, which this may use more than once.</param>
    /// <param name="tenant">Something naming the tenant, on the same terms.</param>
    /// <remarks>
    ///     <para>
    ///         <b>#916: under <see cref="UseTenantPartitionedEvents" /> the read has to name
    ///         <c>tenant_ordinal</c></b>, which leads <c>ix_pc_events_stream_and_version</c> there. Without it
    ///         the read scans every partition, and on locking read committed it queues behind any open append
    ///         in the store.
    ///     </para>
    ///     <para>
    ///         <b>The ordinal comes from the registry, not this process's cache.</b> A process can read a
    ///         tenant it has never written, and resolving one here could mean DDL with a transaction open,
    ///         which is #864. Asking the registry is one seek. It is <c>NOLOCK</c> so that a Serializable
    ///         session doesn't hold a lock the next tenant's registration would have to wait for.
    ///     </para>
    ///     <para>
    ///         <b>The current ordinal isn't enough by itself.</b> A stream row keeps the ordinal it was written
    ///         with, and a tenant removed with <c>RetainData</c> can come back on a different one (see
    ///         <see cref="LockedStreamReadSql" />). Its old events stay where they are, and there is no listing
    ///         old ordinals to seek them by, because nothing records one and <c>MERGE RANGE</c> took the
    ///         boundary. So a stream that isn't wholly on the current ordinal, and any stream of a tenant the
    ///         registry no longer has, keeps the read that doesn't name one, and pays for the scan.
    ///     </para>
    ///     <para>
    ///         The first <c>EXISTS</c> is the ordinary case: two singletons, and the end of one stream in the
    ///         index. The row being on the current ordinal isn't proof by itself, because a tenant can come
    ///         back to an ordinal it had before, with the row still there and its newest events somewhere
    ///         else, so the row's version also has to be the last one found there. If it isn't, the cost is
    ///         the scan, and never events. The second <c>EXISTS</c> tells a stream that isn't there, which has
    ///         nothing to scan for, from one that is elsewhere. It is one seek per partition, and <c>NOLOCK</c>
    ///         for the reason given on <see cref="LockedStreamReadSql" />.
    ///     </para>
    /// </remarks>
    internal string StreamEventsSql(string select, string rest, string stream, string tenant)
    {
        if (!UseTenantPartitionedEvents) return $"{select}{rest};";

        return $"""
            {StreamOrdinalGuardSql(stream, tenant)}
                {select}{StreamOrdinalPredicate}{rest};
            ELSE
                {select}{rest};
            """;
    }

    /// <summary>
    ///     What <see cref="StreamEventsSql" /> starts with, up to the statement for a stream on the tenant's
    ///     current ordinal, which narrows with <see cref="StreamOrdinalPredicate" />.
    /// </summary>
    internal string StreamOrdinalGuardSql(string stream, string tenant)
    {
        var column = TenantPartitionManager.Column;
        var row = $"s.tenant_id = {tenant} AND s.id = {stream}";

        return $"""
            DECLARE @pc_stream_ordinal int =
                (SELECT ordinal FROM {TenantPartitionsTableName} WITH (NOLOCK) WHERE tenant_id = {tenant});
            IF EXISTS (SELECT 1 FROM {StreamsTableName} s
                       WHERE {row} AND s.{column} = @pc_stream_ordinal AND s.version =
                          (SELECT MAX(e.version) FROM {EventsTableName} e
                           WHERE e.{column} = @pc_stream_ordinal AND e.stream_id = {stream}))
               OR NOT EXISTS (SELECT 1 FROM {StreamsTableName} s WITH (NOLOCK) WHERE {row})
            """;
    }

    internal string StreamOrdinalPredicate => $" AND {TenantPartitionManager.Column} = @pc_stream_ordinal";

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
        var serializer = _options.StorageSerializer;
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
    ///     The progression write of an async daemon batch, checked against the floor the batch was built from. Floor 0 is
    ///     the shard's first commit, a MERGE because the row may not exist yet; any other floor is an UPDATE. Either one
    ///     fails with <see cref="JasperFx.Events.Daemon.ProgressionProgressOutOfOrderException" /> when the row has moved
    ///     past the floor (#1060, #1118).
    /// </summary>
    internal Polecat.Internal.Operations.RecordProgressionOperation ProjectionBatchProgressOperation(string shardIdentity,
        long floor, long ceiling)
        => new(ProgressionTableName, shardIdentity, ceiling, EnableExtendedProgressionTracking, upsert: floor == 0,
            UnicodeKeys, expectedFloor: floor);

    /// <summary>
    ///     Guard for identity-sensitive entry points that take a Guid stream id (marten#5244's
    ///     Polecat twin). Downstream code branches on the store's configured
    ///     <see cref="StreamIdentity" /> rather than on which overload the caller used, so a
    ///     mismatched overload otherwise either silently matches nothing or fails with an error
    ///     naming nothing actionable.
    /// </summary>
    /// <remarks>
    ///     #653: the message names the setting that decides this and both ways out — use the other
    ///     overload, or reconfigure the store — because the fact on its own ("configured to identify
    ///     streams with strings") left the caller to go find which setting controls it. Fisher's
    ///     wording is the model.
    /// </remarks>
    internal void EnsureAsGuidStorage()
    {
        if (StreamIdentity == StreamIdentity.AsString)
        {
            throw new InvalidOperationException(
                "This Polecat event store is configured for string stream identity (opts.Events.StreamIdentity = StreamIdentity.AsString). Use the string stream key overloads, or set opts.Events.StreamIdentity = StreamIdentity.AsGuid if streams should be keyed by Guid.");
        }
    }

    /// <summary>
    ///     Guard for identity-sensitive entry points that take a string stream key. See
    ///     <see cref="EnsureAsGuidStorage" />.
    /// </summary>
    internal void EnsureAsStringStorage()
    {
        if (StreamIdentity == StreamIdentity.AsGuid)
        {
            throw new InvalidOperationException(
                "This Polecat event store is configured for Guid stream identity (opts.Events.StreamIdentity = StreamIdentity.AsGuid). Use the Guid stream id overloads, or set opts.Events.StreamIdentity = StreamIdentity.AsString if streams should be keyed by string.");
        }
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

        // #808 / jasperfx#1053: a C# union is stored as its active case, so each case gets its own event
        // type name and the stored JSON (which STJ writes as the bare case anyway) reads back by that name.
        // The base EventRegistry.BuildEvent does this too, but this override never reaches it. Always the
        // value unchanged before .NET 11.
        eventData = UnionTypes.Unwrap(eventData);

        var eventType = EventMappingFor(eventData.GetType());
        return eventType.Wrap(eventData);
    }

    /// <summary>
    ///     Get or create event type metadata for the given .NET type.
    /// </summary>
    // Roots PolecatEventType<>'s constructor for trimming / Native AOT — without this root the
    // constructor metadata is trimmed and CloseAndBuildAs throws MissingMethodException at runtime.
    // Same pattern as Marten's EventGraph rooting EventMapping<>.
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(PolecatEventType<>))]
    public override PolecatEventType EventMappingFor(Type eventType)
    {
        // The generic PolecatEventType<T> is closed ONCE per event type here at registration,
        // so the per-event Wrap call is a plain `new Event<T>((T)data)` with no reflection
        // (mirrors JasperFx.Events.EventTypeData<T>.Wrap). See gh-537.
        if (_eventTypes.TryGetValue(eventType, out var existing)) return existing;

        var mapping = _eventTypes.GetOrAdd(eventType,
            static type => typeof(PolecatEventType<>).CloseAndBuildAs<PolecatEventType>(type));

        // jasperfx#1043: tell EventRegistry's dotnet_type cache about the registration, which seeds its
        // names and expires any cached miss a newly loaded type could now answer.
        RegisterDotNetTypeNames(mapping);
        return mapping;
    }

    public override void AddEventType(Type eventType)
    {
        // #808: a union is never stored as itself (BuildEvent unwraps it), so registering one registers
        // each of its cases, nested unions included. Just eventType when it is not a union.
        foreach (var type in eventType.ExpandToCaseTypes())
        {
            EventMappingFor(type);
        }
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

        // ⚠️ #733: the four canonical identity types are closed STATICALLY, for the same reason
        // PolecatClosedShapeRegistration.BuildProviderFor does it -- TDoc is already a type parameter
        // here, so these are ordinary generic calls ILC can see and compile. Closing TId with
        // MakeGenericType works under CoreCLR and throws in a native image on the first live
        // aggregation:
        //
        //   NotSupportedException: 'SingleStreamProjection`2[Quest,System.Guid]' is missing native
        //   code or metadata.
        //
        // The reflective tail stays for a STRONG-TYPED id, whose wrapper is a runtime value nothing
        // here can name -- the same split as the closed-shape registration, and the same one Marten
        // and Fisher make.
#pragma warning disable CS8714 // notnull constraint mismatch
        ProjectionBase projection =
            idType == typeof(Guid) ? new SingleStreamProjection<TDoc, Guid>()
            : idType == typeof(string) ? new SingleStreamProjection<TDoc, string>()
            : idType == typeof(int) ? new SingleStreamProjection<TDoc, int>()
            : idType == typeof(long) ? new SingleStreamProjection<TDoc, long>()
            : (ProjectionBase)Activator.CreateInstance(
                typeof(SingleStreamProjection<,>).MakeGenericType(typeof(TDoc), idType))!;
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
            return new DocumentMapping(aggregateType, _options).AggregateIdType;
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

    /// <summary>
    ///     #370/#373: the alias persisted in <c>pc_streams.type</c> is the aggregate type's SIMPLE name.
    ///     <see cref="EventRegistry.AggregateAliasFor" /> caches it, and that cache is what
    ///     <see cref="EventRegistry.TryResolveAggregateType" /> reads first (jasperfx#1043).
    /// </summary>
    /// <remarks>
    ///     Two aggregate types with the same <c>Name</c> in different namespaces share an alias and the
    ///     first one seen wins. That ambiguity lives in the persisted format, not here.
    /// </remarks>
    protected override string AliasFor(Type aggregateType) => aggregateType.Name;

    /// <summary>
    ///     An alias no <see cref="EventRegistry.AggregateAliasFor" /> call has seen yet, such as a stream
    ///     written by another process or an earlier run: the registered projections cover those.
    /// </summary>
    protected override Type? FindAggregateType(string aggregateTypeName)
    {
        foreach (var source in _options.Projections.All)
        {
            foreach (var published in source.PublishedTypes())
            {
                if (string.Equals(published.Name, aggregateTypeName, StringComparison.Ordinal)
                    || string.Equals(published.FullName, aggregateTypeName, StringComparison.Ordinal))
                {
                    return published;
                }
            }
        }

        return null;
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

    internal EventProjectionFloorsTable BuildEventProjectionFloorsTable()
    {
        return new EventProjectionFloorsTable(this);
    }

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
    /// <summary>
    ///     Opt <typeparamref name="TEvent" /> into binary serialization (#388): its payload is written
    ///     to the <c>bdata</c> column instead of <c>data</c>, and read back through the same
    ///     serializer. Wins over <c>[BinaryEvent]</c> + <see cref="EventRegistry.DefaultBinarySerializer" />.
    ///     The registration lives on <see cref="EventRegistry" /> (jasperfx#1043); this overload keeps the
    ///     fluent return type.
    /// </summary>
    public new EventGraph UseBinarySerializer<TEvent>(JasperFx.Events.IEventBinarySerializer serializer)
        where TEvent : notnull
    {
        base.UseBinarySerializer<TEvent>(serializer);
        return this;
    }

    /// <summary>
    ///     The <c>bdata</c> bytes for an event on the write path, or null when the event's type stays
    ///     on the JSON path. The two are mutually exclusive per row — see
    ///     <see cref="JsonPlaceholderForBinaryEvent" />.
    /// </summary>
    internal byte[]? SerializeEventBdata(IEvent @event)
    {
        // Deliberately NOT short-circuited on UsesBinaryEventSerialization: a type marked
        // [BinaryEvent] in a store with no serializer configured has to throw here, and skipping the
        // resolve would turn that into a silent write of JSON.
        return SerializeBinaryEventData(@event);
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
    /// <summary>
    ///     Resolve the upcast transformation registered for a stored event type name, if any. This is
    ///     the call every Polecat hydration path makes BEFORE default deserialization.
    /// </summary>
    /// <param name="eventTypeName">The stored <c>pc_events.type</c> alias.</param>
    /// <param name="isBinary">
    ///     Whether the row's <c>bdata</c> column is non-null, which means the row was written by an
    ///     <see cref="IEventBinarySerializer" /> (#388), and upcasting deliberately does not apply:
    ///     the shared <c>IUpcastPayload</c> is a JSON contract — <c>As&lt;T&gt;</c> and
    ///     <c>AsJsonDocument</c> both presuppose a JSON body — so a binary row has nothing an upcast
    ///     transformation can read. Such a row keeps its ordinary binary read path.
    /// </param>
    /// <remarks>
    ///     <para>
    ///         <b>The marten#4680 authority rule lives here, and in Polecat it is structural rather
    ///         than a guard.</b> A registered transformation is the authoritative interpretation of
    ///         its SOURCE event type name: once this returns true, the caller must not consult
    ///         <see cref="ResolveEventType" /> at all. That matters because Polecat resolves an
    ///         event's CLR type from the stored <c>dotnet_type</c> hint, so appending the OLD CLR type
    ///         typed into the very store that carries the upcaster writes a row whose hint points
    ///         straight back at the old type — and a read that consulted the hint first would hand
    ///         back the old type and leave the upcaster silently doing nothing, for exactly the rows
    ///         most likely to exist during a migration.
    ///     </para>
    ///     <para>
    ///         Marten needs an explicit <c>!mapping.IsUpcastTarget</c> condition on its alt-mapping
    ///         swap for the same reason. Polecat has no alt-mapping swap, so ordering the check first
    ///         is the whole of it — which is why every hydration path must ask THIS method rather than
    ///         reaching for the registry itself.
    ///     </para>
    ///     <para>
    ///         <see cref="UpcastingRegistry.HasAny" /> short-circuits first so a store with no
    ///         upcasters registered — the overwhelming majority — pays one boolean field read per row
    ///         and keeps its unmodified hydration path.
    ///     </para>
    /// </remarks>
    internal bool TryFindUpcast(string? eventTypeName, bool isBinary,
        [NotNullWhen(true)] out UpcastTransformation? transformation)
    {
        if (isBinary || !Upcasters.HasAny || eventTypeName is null)
        {
            transformation = null;
            return false;
        }

        return Upcasters.TryFindTransformation(eventTypeName, out transformation);
    }

    /// <summary>
    ///     Run a transformation over a stored JSON payload on a synchronous read path. An async-only
    ///     registration throws <see cref="UpcastingException" /> from here, by contract.
    /// </summary>
    internal object Upcast(UpcastTransformation transformation, string json, ISerializer serializer)
        => transformation.Upcast(new Upcasting.PolecatUpcastPayload(json, serializer));

    /// <summary>
    ///     Run a transformation over a stored JSON payload on an asynchronous read path.
    /// </summary>
    /// <remarks>
    ///     Every Polecat read that a consumer awaits must come through here rather than
    ///     <see cref="Upcast" />: a store that routed its async reads through the synchronous delegate
    ///     would make an async-only registration unreachable, which the contract turns into an
    ///     <see cref="UpcastingException" /> rather than a silent fallback precisely so the mistake is
    ///     loud.
    /// </remarks>
    internal ValueTask<object> UpcastAsync(UpcastTransformation transformation, string json,
        ISerializer serializer, CancellationToken token)
        => transformation.UpcastAsync(new Upcasting.PolecatUpcastPayload(json, serializer), token);

    /// <summary>
    ///     Pre-register the TARGET event type of every registered transformation.
    /// </summary>
    /// <remarks>
    ///     Called once at <see cref="DocumentStore" /> construction. Without it the new event type is
    ///     unknown to the store until something appends one, so an upcast row read before any append
    ///     of the new type would have no mapping to wrap its payload in. The SOURCE name deliberately
    ///     is not registered as an event type: it is a name in the database, not a type this
    ///     deployment appends, and after a raw-JSON upcast the old CLR type may not exist at all.
    /// </remarks>
    internal void RegisterUpcastTargetTypes()
    {
        foreach (var transformation in Upcasters.AllTransformations)
        {
            AddEventType(transformation.EventType);
        }
    }

    /// <summary>
    ///     Deserialize the event payload of the row <paramref name="reader" /> is on: the JSON
    ///     <paramref name="json" /> when its <c>bdata</c> column is null, otherwise the binary payload
    ///     through the type's <see cref="IEventBinarySerializer" /> (#388).
    /// </summary>
    /// <remarks>
    ///     #981 (jasperfx#1187): the binary payload is read into a buffer rented from
    ///     <see cref="ArrayPool{T}.Shared" /> and handed to the serializer as a span, rather than first
    ///     copied into a new exact-length array per event. Deserialization is synchronous and the
    ///     serializer must not hold on to the span, so the buffer goes back to the pool before this
    ///     returns. It is cleared on return, because event payloads can carry data a masking rule exists
    ///     to protect.
    /// </remarks>
    internal object DeserializeEventData(Type resolvedType, string json, DbDataReader reader, int bdataOrdinal,
        ISerializer serializer)
    {
        if (reader.IsDBNull(bdataOrdinal)) return serializer.FromJson(resolvedType, json);

        var binary = ResolveBinarySerializerFor(resolvedType)
                     ?? throw new InvalidOperationException(
                         $"A pc_events row for '{resolvedType.FullName}' has a non-null bdata column but no "
                         + "IEventBinarySerializer is registered for that type. Configure it with "
                         + $"opts.Events.UseBinarySerializer<{resolvedType.Name}>(...) or set "
                         + "opts.Events.DefaultBinarySerializer — the event cannot be read without it.");

        var length = checked((int)reader.GetBytes(bdataOrdinal, 0, null, 0, 0));
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var read = 0;
            while (read < length)
            {
                var chunk = (int)reader.GetBytes(bdataOrdinal, read, buffer, read, length - read);
                if (chunk == 0) break;
                read += chunk;
            }

            return binary.Deserialize(resolvedType, new ReadOnlySpan<byte>(buffer, 0, read));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    /// <summary>
    ///     All currently registered event types.
    /// </summary>
    public IReadOnlyList<PolecatEventType> AllKnownEventTypes() => _eventTypes.Values.ToList();

    internal EventTagTable BuildEventTagTable(ITagTypeRegistration registration)
    {
        return new EventTagTable(this, registration);
    }

    internal DcbTagVersionTable BuildDcbTagVersionTable()
    {
        return new DcbTagVersionTable(this);
    }

    internal Weasel.SqlServer.Sequence BuildDcbTagVersionSequence()
    {
        return new Weasel.SqlServer.Sequence(
            new Weasel.SqlServer.SqlServerObjectName(DatabaseSchemaName, DcbTagVersionTable.SequenceName),
            DcbTagVersionTable.SequenceStart);
    }

    /// <summary>The schema-qualified name of the DCB tag-version side table (gh-515).</summary>
    internal string DcbTagVersionTableName
        => Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, DcbTagVersionTable.TableName);

    /// <summary>The schema-qualified name of the sequence behind every tag version (#890).</summary>
    internal string DcbTagVersionSequenceName
        => Polecat.Internal.SqlEscaping.QualifiedName(DatabaseSchemaName, DcbTagVersionTable.SequenceName);

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
    public new void AddMaskingRuleForProtectedInformation<T>(Action<T> action) where T : notnull
    {
        base.AddMaskingRuleForProtectedInformation(action);
        _maskedEventTypes.Add(typeof(T));
    }

    /// <summary>
    ///     Register a policy for how to remove or mask protected information
    ///     for an event type T, replacing the event data with a new instance.
    /// </summary>
    public new void AddMaskingRuleForProtectedInformation<T>(Func<T, T> func) where T : notnull
    {
        base.AddMaskingRuleForProtectedInformation(func);
        _maskedEventTypes.Add(typeof(T));
    }

    /// <summary>
    ///     #860: refuse a masking rule that can never run. An upcast source's rows are read as the upcast's
    ///     target and masked as what they read as, so a rule naming the source matches nothing, the pass
    ///     reports success, and the protected value stays in the row. Checked when the store is built
    ///     because the rule and the upcast can be registered in either order.
    /// </summary>
    /// <remarks>
    ///     A type that an upcast also produces is left alone, because some rows do read as it: that is
    ///     <c>Upcast&lt;T&gt;(Func&lt;JsonDocument, T&gt;)</c>, where the source and target are one name,
    ///     and the middle of a chain. The registry keeps stored names rather than old CLR types, so this
    ///     goes by name, which also covers class-based upcasters and is why the overloads taking an
    ///     explicit name report the old type through <see cref="RecordUpcastSource" />.
    /// </remarks>
    internal void AssertMaskingRulesCanRun()
    {
        if (!Upcasters.HasAny) return;

        var targets = Upcasters.AllTransformations.Select(x => x.EventType).ToHashSet();

        foreach (var eventType in _maskedEventTypes.Distinct())
        {
            if (targets.Contains(eventType)) continue;

            var target = findUpcastTargetOf(eventType);
            if (target == null) continue;

            throw new InvalidOperationException(
                $"The masking rule for event type {eventType.FullNameInCode()} can never run. That type is an upcast source, so its rows are read as {target.FullNameInCode()}, and masking works on what an event reads as: the rule would match nothing, the pass would report success, and the protected data would stay in the row. Register the rule with AddMaskingRuleForProtectedInformation<{target.FullNameInCode()}>(...) instead.");
        }
    }

    private Type? findUpcastTargetOf(Type eventType)
    {
        if (_upcastSources.TryGetValue(eventType, out var recorded)) return recorded;

        foreach (var name in new[] { eventType.GetEventTypeName(), ToEventTypeName(eventType.Name) })
        {
            if (Upcasters.TryFindTransformation(name, out var transformation)) return transformation.EventType;
        }

        return null;
    }

    /// <summary>
    ///     An upcast that claims an explicit stored name, whose old type therefore can't be found from
    ///     that name. See <see cref="AssertMaskingRulesCanRun" />.
    /// </summary>
    internal void RecordUpcastSource(Type oldEventType, Type eventType) => _upcastSources[oldEventType] = eventType;
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
    /// <remarks>
    ///     This base implementation is a reflection fallback for directly constructed instances.
    ///     Everything registered through <see cref="EventGraph.EventMappingFor" /> gets the generic
    ///     <see cref="PolecatEventType{T}" /> subclass, whose override constructs the
    ///     <see cref="Event{T}" /> envelope with no per-event reflection (gh-537).
    /// </remarks>
    public virtual IEvent Wrap(object eventData)
    {
        var genericType = typeof(Event<>).MakeGenericType(_eventType);
        var @event = (IEvent)Activator.CreateInstance(genericType, eventData)!;
        @event.EventTypeName = EventTypeName;
        @event.DotNetTypeName = DotNetTypeName;
        return @event;
    }
}

/// <summary>
///     Reflection-free <see cref="PolecatEventType.Wrap" /> for a known event type. Closed once
///     per event type at registration in <see cref="EventGraph.EventMappingFor" />, so the
///     per-event cost is a constructor call and a cast — the same shape as
///     JasperFx.Events' <c>EventTypeData&lt;T&gt;.Wrap</c> (gh-537).
/// </summary>
public class PolecatEventType<T> : PolecatEventType where T : notnull
{
    public PolecatEventType() : base(typeof(T))
    {
    }

    public override IEvent Wrap(object eventData)
    {
        return new Event<T>((T)eventData) { EventTypeName = EventTypeName, DotNetTypeName = DotNetTypeName };
    }
}
