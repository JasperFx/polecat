using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Tags;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Events.Internal;
using Polecat.Storage;

using Polecat.Internal;
namespace Polecat;

/// <summary>
///     IEventStore explorer / diagnostic methods (CritterWatch #143).
///     Implements the eight methods added in JasperFx.Events 1.36 against
///     Polecat's pc_streams / pc_events / pc_event_progression tables.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2075:DynamicallyAccessedMembers",
    Justification = "Class-level: RehydrateAtVersionByNameAsync reflects on Task<T>.Result + .State/.Version/.EventsApplied properties of the returned strong-typed result. The framework Task<TResult> intrinsic and the strong-typed result type are preserved by the registered projection boundary.")]
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: invokes ISerializer.ToJson, which is annotated RUC because the default STJ-reflection serializer requires unreferenced code. AOT consumers supply a source-generator-backed ISerializer impl per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: RehydrateAtVersionByNameAsync uses MethodInfo.MakeGenericMethod with the resolved aggregate type — runtime code generation. AOT consumers should prefer the strong-typed RehydrateAtVersionAsync<T> overload (covered by the AOT publishing guide).")]
public partial class DocumentStore
{
    Task<IReadOnlyList<StreamSummary>> IEventStore.GetRecentStreamsAsync(int count, CancellationToken ct)
        => ((IEventStore)this).GetRecentStreamsAsync(count, null, ct);

    // #584 / jasperfx#810 — the database dimension. Reads the listing from ONE database rather than
    // from whichever database the store's default session resolves. A tool enumerates AllDatabases()
    // and calls this per database, so every row it renders is attributable to the database it came
    // from. On a single-database store this is the same read as the store-global overload; the
    // argument is simply the store's one database.
    Task<IReadOnlyList<StreamSummary>> IEventStore.GetRecentStreamsAsync(
        IEventDatabase database, int count, string? tenantId, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(database);
        return recentStreamsAsync(RequirePolecatDatabase(database), count, tenantId, ct);
    }

    // #782 / jasperfx#503 — tenant-scoped recent-streams listing. On a conjoined multi-tenant
    // Polecat store the pc_streams rows carry a tenant_id column, so a non-null tenantId bounds
    // the listing with a tenant_id predicate. Null preserves the store-global listing across
    // every tenant within a database. The database axis is independent and is handled by the
    // IEventDatabase overload above (#584); the tenant_id predicate here applies on top of whichever
    // database ends up being opened.
    Task<IReadOnlyList<StreamSummary>> IEventStore.GetRecentStreamsAsync(
        int count, string? tenantId, CancellationToken ct)
        // #584 — on a multi-database store a store-global listing is a fan-out, not one database's
        // rows. See FanOutRecentStreamsAsync for why this one read merges where its siblings refuse.
        //
        // #593 — but only when the read really IS store-global. A tenant names the database it lives
        // in, so a tenant-scoped listing goes to that one database and keeps the tenant_id predicate
        // on top of it. Fanning out for a named tenant would open every database in the pool to find
        // rows that can only be in one of them.
        => tenantId == null
            ? IsSingleDatabase
                ? recentStreamsAsync(null, count, tenantId, ct)
                : FanOutRecentStreamsAsync(count, null, ct)
            : recentStreamsAsync(DatabaseForTenant(tenantId), count, tenantId, ct);

    private async Task<IReadOnlyList<StreamSummary>> recentStreamsAsync(
        PolecatDatabase? database, int count, string? tenantId, CancellationToken ct)
    {
        if (count <= 0) return Array.Empty<StreamSummary>();

        var results = new List<StreamSummary>(capacity: count);

        // #148: run through a QuerySession so the command is covered by
        // Options.ResiliencePipeline (Polly) like the rest of the read path.
        await using var session = ExplorerSession(database);
        await using var cmd = new SqlCommand();
        // #57: share the column projection + row read with FetchStreamStateAsync
        // and GetStreamMetadataAsync via PcStreamsRowReader so all three sites
        // stay aligned when pc_streams' shape evolves.
        var tenantFilter = tenantId == null ? "" : "WHERE tenant_id = @tenant_id\n            ";
        cmd.CommandText = $"""
            SELECT TOP (@count) {PcStreamsRowReader.SelectColumns}
            FROM {Events.StreamsTableName}
            {tenantFilter}ORDER BY timestamp DESC;
            """;
        cmd.Parameters.AddWithValue("@count", count);
        if (tenantId != null) cmd.Parameters.AddVarChar("@tenant_id", tenantId);

        await using var reader = await session.ExecuteReaderAsync(cmd, ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add(PcStreamsRowReader.ReadStreamSummary(reader, JasperFx.StorageConstants.DefaultTenantId));
        }

        return results;
    }

    IAsyncEnumerable<EventRecord> IEventStore.ReadStreamAsync(string streamId, CancellationToken ct)
        => ((IEventStore)this).ReadStreamAsync(streamId, null, ct);

    // #584 / jasperfx#810 — the database dimension matters more here than for a listing: the same
    // stream id can exist in several databases, each with its own version sequence, so "which
    // database" is part of the stream's identity rather than a filter over one answer.
    IAsyncEnumerable<EventRecord> IEventStore.ReadStreamAsync(
        IEventDatabase database, string streamId, string? tenantId, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(database);
        return readStreamAsync(RequirePolecatDatabase(database), streamId, tenantId, ct);
    }

    // #782 / jasperfx#503 — tenant-scoped stream read. On a conjoined multi-tenant Polecat
    // store the same stream id can exist under two tenants; the tenant-less overload reads
    // across every tenant and returns their ambiguous union. A non-null tenantId filters the
    // read to that tenant's rows via a tenant_id predicate. Null preserves the read across every
    // tenant in the database. The database axis is the IEventDatabase overload above (#584).
    IAsyncEnumerable<EventRecord> IEventStore.ReadStreamAsync(
        string streamId, string? tenantId, CancellationToken ct)
    {
        // #593 — a tenant names the database it lives in, so a tenant-scoped read is not the
        // ambiguous cross-database read the refusal below exists for. It resolves to one database
        // and keeps the tenant_id predicate: under conjoined tenancy the identity of a stream is
        // (tenant, id), not id alone, so both axes apply.
        if (tenantId != null)
        {
            return readStreamAsync(DatabaseForTenant(tenantId), streamId, tenantId, ct);
        }

        // #584 — concatenating one stream id's events out of several databases would interleave two
        // independent version sequences into something that reads as a single stream and is not.
        RequireSingleDatabase(nameof(IEventStore.ReadStreamAsync));
        return readStreamAsync(null, streamId, null, ct);
    }

    private async IAsyncEnumerable<EventRecord> readStreamAsync(
        PolecatDatabase? database, string streamId, string? tenantId,
        [EnumeratorCancellation] CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);

        // Resolve to the configured identity type
        object resolvedStreamId = Events.StreamIdentity == StreamIdentity.AsGuid
            ? Guid.Parse(streamId)
            : streamId;

        // #148: run through a QuerySession so the command is covered by
        // Options.ResiliencePipeline (Polly). The session is kept alive for the
        // duration of the enumeration via await using.
        await using var session = ExplorerSession(database);
        await using var cmd = new SqlCommand();

        // #57 pc_events half: share the column projection + row hydration
        // with QueryEventStore.FetchStreamAsync via PcEventsRowReader. The
        // canonical SELECT projects all enabled metadata columns; the
        // explorer's EventRecord shape ignores the columns it doesn't surface
        // (dotnet_type, is_archived, correlation_id, causation_id) per the
        // polecat#57 Q2 design note — wire cost is negligible against the
        // readability of one canonical projection.
        var tenantFilter = tenantId == null ? "" : "AND tenant_id = @tenant_id\n            ";
        cmd.CommandText = $"""
            SELECT {PcEventsRowReader.ComposeSelectColumns(Events.EventOptions)}
            FROM {Events.EventsTableName}
            WHERE stream_id = @stream_id
            {tenantFilter}ORDER BY version;
            """;
        cmd.Parameters.AddIdParameter("@stream_id", resolvedStreamId);
        if (tenantId != null) cmd.Parameters.AddVarChar("@tenant_id", tenantId);

        var ctx = new EventHydrationContext(
            Events,
            Options.Serializer,
            resolvedStreamId,
            defaultTenantId: JasperFx.StorageConstants.DefaultTenantId);

        // Per-batch hoist: optional-metadata column ordinals computed once.
        // Explorer path doesn't need EventTypeCache (no EventMappingFor call).
        var slots = MetadataSlots.Compute(Events.EventOptions);

        await using var reader = await session.ExecuteReaderAsync(cmd, ct);
        while (await reader.ReadAsync(ct))
        {
            yield return PcEventsRowReader.ReadEventRecord(reader, ctx, slots);
        }
    }

    Task<StreamMetadata?> IEventStore.GetStreamMetadataAsync(
        string streamId, CancellationToken ct)
        => ((IEventStore)this).GetStreamMetadataAsync(streamId, null, ct);

    /// <summary>
    ///     #593 — the tenant dimension of the metadata read, which used to refuse.
    /// </summary>
    /// <remarks>
    ///     Both axes apply and they are independent. The tenant selects the database it lives in
    ///     (a no-op on a single-database store), and the <c>tenant_id</c> predicate selects its rows
    ///     within that database — which matters because sharded tenancy co-locates many tenants in
    ///     each database, so "the tenant's database" is not "the tenant's data". Deciding from
    ///     cardinality alone, as Marten does, drops the predicate exactly there (marten#5383 §2).
    /// </remarks>
    Task<StreamMetadata?> IEventStore.GetStreamMetadataAsync(
        string streamId, string? tenantId, CancellationToken ct)
    {
        if (tenantId != null)
        {
            return streamMetadataAsync(DatabaseForTenant(tenantId), streamId, tenantId, ct);
        }

        // #584 — this read returns ONE row. On a multi-database store two databases can each hold a
        // stream with this id, and there is no answer that names both.
        RequireSingleDatabase(nameof(IEventStore.GetStreamMetadataAsync));
        return streamMetadataAsync(null, streamId, null, ct);
    }

    // #584 / jasperfx#810 — the database dimension. A null answer from this overload means "no such
    // stream in THIS database" rather than "no such stream", which is exactly the distinction the
    // store-global read cannot make once there is more than one database.
    Task<StreamMetadata?> IEventStore.GetStreamMetadataAsync(
        IEventDatabase database, string streamId, string? tenantId, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(database);

        // #593: the tenant axis used to refuse here. Naming the database does not turn the tenant
        // predicate off -- the database says where to look, the tenant says which rows.
        return streamMetadataAsync(RequirePolecatDatabase(database), streamId, tenantId, ct);
    }

    private async Task<StreamMetadata?> streamMetadataAsync(
        PolecatDatabase? database, string streamId, string? tenantId, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);

        object resolvedStreamId = Events.StreamIdentity == StreamIdentity.AsGuid
            ? Guid.Parse(streamId)
            : streamId;

        // #148: run through a QuerySession so the command is covered by
        // Options.ResiliencePipeline (Polly).
        await using var session = ExplorerSession(database);
        await using var cmd = new SqlCommand();
        // #57: share the pc_streams column projection + row read with the
        // other two stream-reading sites via PcStreamsRowReader. The JOIN'd
        // first_event_at column is appended after the canonical projection
        // (index 8, following jasperfx#740's compacted_version at 7) and
        // threaded into ReadStreamMetadata explicitly.
        // #593: under conjoined tenancy pc_streams holds one row per (tenant, id), so without this
        // predicate a tenant-scoped read returns whichever of them the scan reached first -- a
        // plausible, complete-looking answer about the wrong tenant.
        var tenantFilter = tenantId == null ? "" : "\n              AND s.tenant_id = @tenant_id";
        cmd.CommandText = $"""
            SELECT {PcStreamsRowReader.SelectColumnsWithAlias("s")},
                   MIN(e.timestamp) AS first_event_at
            FROM {Events.StreamsTableName} s
            LEFT JOIN {Events.EventsTableName} e ON e.stream_id = s.id AND e.tenant_id = s.tenant_id
            WHERE s.id = @id{tenantFilter}
            GROUP BY s.id, s.type, s.version, s.created, s.timestamp, s.tenant_id, s.is_archived, s.compacted_version;
            """;
        cmd.Parameters.AddIdParameter("@id", resolvedStreamId);
        if (tenantId != null) cmd.Parameters.AddVarChar("@tenant_id", tenantId);

        await using var reader = await session.ExecuteReaderAsync(cmd, ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        var firstEventAt = reader.IsDBNull(8)
            ? (DateTimeOffset?)null
            : reader.GetFieldValue<DateTimeOffset>(8);

        return PcStreamsRowReader.ReadStreamMetadata(reader, JasperFx.StorageConstants.DefaultTenantId, firstEventAt);
    }

    IAsyncEnumerable<EventRecord> IEventStore.QueryByTagsAsync(
        IReadOnlyDictionary<string, string> tags, CancellationToken ct)
    {
        throw new NotSupportedException(
            "DCB tag-set queries are not yet supported on Polecat. " +
            "Tracked at https://github.com/JasperFx/polecat/issues — see the CritterWatch master plan for the DCB roadmap.");
    }

    /// <summary>
    ///     #353 / jasperfx#555 — tenant-scoped DCB tag query, companion to the jasperfx#503 stream reads.
    ///     On a conjoined multi-tenant store the same tag value can be attached to events living under two
    ///     tenants, so an untenanted query reads an ambiguous cross-tenant union. This overload isolates a
    ///     single tenant with an <c>e.tenant_id</c> predicate on pc_events, AND-combining a <c>seq_id</c>
    ///     sub-select per requested tag (matching the Marten companion's shape).
    ///     <para>
    ///     A null <paramref name="tenantId"/> delegates to the tenant-less overload — still a
    ///     NotSupportedException, because store-global DCB tag queries are a separate, pre-existing Polecat
    ///     gap. The DATABASE axis is independent of this one and is now closed (#584): the IEventDatabase
    ///     overload picks the database, and the <c>e.tenant_id</c> predicate below applies within it.
    ///     </para>
    /// </summary>
    // #584 / jasperfx#810 — the database dimension. Tag values are not unique across databases, so
    // a store-global tag query on a sharded store would answer from one database and show no sign of
    // it. This overload names the database; the tenant predicate below is the independent second
    // axis, and both apply.
    IAsyncEnumerable<EventRecord> IEventStore.QueryByTagsAsync(
        IEventDatabase database, IReadOnlyDictionary<string, string> tags, string? tenantId,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(database);
        return queryByTagsAsync(RequirePolecatDatabase(database), tags, tenantId, ct);
    }

    IAsyncEnumerable<EventRecord> IEventStore.QueryByTagsAsync(
        IReadOnlyDictionary<string, string> tags, string? tenantId, CancellationToken ct)
        // #593: a named tenant selects the database it lives in, exactly as it does for the stream
        // reads. Null stays on the default routing and reaches the store-global refusal below.
        => queryByTagsAsync(tenantId == null ? null : DatabaseForTenant(tenantId), tags, tenantId, ct);

    private async IAsyncEnumerable<EventRecord> queryByTagsAsync(
        PolecatDatabase? database,
        IReadOnlyDictionary<string, string> tags,
        string? tenantId,
        [EnumeratorCancellation] CancellationToken ct)
    {
        if (tenantId == null)
        {
            // Store-global tag query is not yet supported on Polecat — defer to the tenant-less overload,
            // which throws NotSupportedException as soon as this iterator is enumerated.
            await foreach (var e in ((IEventStore)this).QueryByTagsAsync(tags, ct).ConfigureAwait(false))
            {
                yield return e;
            }

            yield break;
        }

        ArgumentNullException.ThrowIfNull(tags);
        if (tags.Count == 0) yield break;

        var options = Events.EventOptions;
        var registered = Events.TagTypes;

        // #148: run through a QuerySession so the command is covered by Options.ResiliencePipeline (Polly),
        // like the rest of the explorer read path. The e.tenant_id predicate below — not the session's own
        // tenant — does the scoping, matching how the other explorer reads run raw SQL over every tenant.
        await using var session = (Internal.QuerySession)QuerySession();
        await using var cmd = new SqlCommand();

        var sb = new StringBuilder();
        sb.Append(
            $"SELECT {PcEventsRowReader.ComposeSelectColumnsWithAlias(options, "e")} FROM {Events.EventsTableName} e WHERE ");

        var idx = 0;
        foreach (var (tagName, tagValue) in tags)
        {
            // #575: resolved through the shared matcher rather than an inline copy of it. Polecat's
            // behaviour here — CLR simple name OR registered table suffix, case-insensitively — is
            // what jasperfx#801 standardized on, but Marten's copy of this same overload matched the
            // CLR name only and compared values case-sensitively, and the divergence survived because
            // the dictionary overload has no compliance coverage. Sharing the matcher is what stops
            // the two drifting apart again, here and in ApplyTagValues.
            var registration = registered.RequireByTagName(tagName, nameof(tags));

            if (idx > 0) sb.Append(" AND ");

            // A seq_id sub-select per tag, AND-combined: the event must carry every requested tag. The tag
            // value arrives as a string from the explorer, so it's compared case-insensitively via nvarchar —
            // matching whatever native type (uniqueidentifier, etc.) the tag's value column stores.
            sb.Append(
                $"e.seq_id IN (SELECT seq_id FROM {Events.TagTableName(registration)} WHERE LOWER(CONVERT(nvarchar(4000), value)) = LOWER(@tag_value_{idx}))");
            cmd.Parameters.AddWithValue($"@tag_value_{idx}", tagValue);
            idx++;
        }

        sb.Append(" AND e.tenant_id = @tenant_id ORDER BY e.seq_id");
        cmd.Parameters.AddVarChar("@tenant_id", tenantId);
        cmd.CommandText = sb.ToString();

        var ctx = new EventHydrationContext(
            Events,
            Options.Serializer,
            streamId: string.Empty,
            defaultTenantId: tenantId);
        var slots = MetadataSlots.Compute(options);

        await using var reader = await session.ExecuteReaderAsync(cmd, ct);
        while (await reader.ReadAsync(ct))
        {
            yield return PcEventsRowReader.ReadEventRecord(reader, ctx, slots);
        }
    }

    Task<DcbProjectedState?> IEventStore.GetProjectedStateForTagsAsync(
        string projectionName, IReadOnlyDictionary<string, string> tags, CancellationToken ct)
    {
        throw new NotSupportedException(
            "DCB tag-set projection rehydration is not yet supported on Polecat. " +
            "Tracked at https://github.com/JasperFx/polecat/issues — see the CritterWatch master plan for the DCB roadmap.");
    }

    async Task<AggregateAtVersion<TAggregate>> IEventStore.RehydrateAtVersionAsync<TAggregate>(
        object identity, long version, CancellationToken ct) where TAggregate : class
    {
        ArgumentNullException.ThrowIfNull(identity);

        await using var session = QuerySession();

        IReadOnlyList<IEvent> events;
        if (Events.StreamIdentity == StreamIdentity.AsGuid)
        {
            var guid = identity is Guid g ? g : Guid.Parse(identity.ToString()!);
            events = await session.Events.FetchStreamAsync(guid, version: version, token: ct);
        }
        else
        {
            events = await session.Events.FetchStreamAsync(identity.ToString()!, version: version, token: ct);
        }

        TAggregate? state = default;
        long applied = 0;
        if (events.Count > 0)
        {
            var aggregator = Options.Projections.AggregatorFor<TAggregate>();
            state = await aggregator.BuildAsync(events, session, default!, ct);
            applied = events.Count;
        }

        return new AggregateAtVersion<TAggregate>(state!, version, applied);
    }

    async Task<AggregateAtVersion?> IEventStore.RehydrateAtVersionByNameAsync(
        string aggregateTypeName, object identity, long version, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(aggregateTypeName);
        ArgumentNullException.ThrowIfNull(identity);

        var aggregateType = ResolveAggregateType(aggregateTypeName)
            ?? throw new ArgumentException(
                $"Unknown aggregate type '{aggregateTypeName}'. Polecat resolves aggregate types from registered projections — register the projection or use the strong-typed RehydrateAtVersionAsync overload.",
                nameof(aggregateTypeName));

        var method = typeof(IEventStore)
            .GetMethod(nameof(IEventStore.RehydrateAtVersionAsync))!
            .MakeGenericMethod(aggregateType);

        var typedTask = (Task)method.Invoke(this, new[] { identity, version, (object)ct })!;
        await typedTask.ConfigureAwait(false);

        var resultProperty = typedTask.GetType().GetProperty("Result")!;
        var result = resultProperty.GetValue(typedTask)!;
        var stateValue = result.GetType().GetProperty("State")!.GetValue(result);
        var resolvedVersion = (long)result.GetType().GetProperty("Version")!.GetValue(result)!;
        var eventsApplied = (long)result.GetType().GetProperty("EventsApplied")!.GetValue(result)!;

        var stateJson = stateValue is null
            ? default
            : JsonDocument.Parse(Options.Serializer.ToJson(stateValue)).RootElement.Clone();

        return new AggregateAtVersion(aggregateType.FullName ?? aggregateTypeName, stateJson, resolvedVersion, eventsApplied);
    }

    Task<IReadOnlyList<ProjectionStatus>> IEventStore.GetProjectionStatusesAsync(CancellationToken ct)
    {
        // #584 — progression rows live in the database whose events they track. Merging them across
        // databases would collapse N rows named "MyProjection:All" into one, so a store-global
        // snapshot on a sharded store describes one database and reads as the whole store's.
        RequireSingleDatabase(nameof(IEventStore.GetProjectionStatusesAsync));
        return projectionStatusesAsync(SingleDatabase, tenantId: null, ct);
    }

    /// <summary>
    ///     #589 — the tenant dimension, which used to refuse.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The argument means one of two things depending on how the store is tenanted, exactly as
    ///         it does for <c>BuildProjectionDaemonAsync</c>, and the difference is not cosmetic.
    ///     </para>
    ///     <list type="bullet">
    ///     <item>
    ///         <b>One database</b> (conjoined tenancy with per-tenant event partitioning): the tenant
    ///         lives inside that database and its shards carry the trailing
    ///         <c>{Proj}:{ShardKey}:{tenant}</c> suffix, so the progression lookup must be made under
    ///         the tenant-bearing identity.
    ///     </item>
    ///     <item>
    ///         <b>Database-per-tenant</b>: the tenant names a physical database whose shards are NOT
    ///         suffixed — each database runs its own daemon over the same shard identities — so the
    ///         tenant selects where to read, and nothing else.
    ///     </item>
    ///     </list>
    /// </remarks>
    Task<IReadOnlyList<ProjectionStatus>> IEventStore.GetProjectionStatusesAsync(
        string? tenantId, CancellationToken ct)
    {
        if (tenantId == null)
        {
            RequireSingleDatabase(nameof(IEventStore.GetProjectionStatusesAsync));
            return projectionStatusesAsync(SingleDatabase, tenantId: null, ct);
        }

        // On a multi-database store the tenant resolves to the database it lives in; on a single
        // database store there is only one place to read and the tenant narrows the shard identities.
        var database = IsSingleDatabase
            ? SingleDatabase
            : Options.Tenancy!.GetDatabase(tenantId);

        return projectionStatusesAsync(database, tenantId, ct);
    }

    // #584 / jasperfx#810 — the database dimension, and the counterpart to the per-cell lag that
    // IEventDatabase.FetchProjectionLagAsync already reports one database at a time.
    Task<IReadOnlyList<ProjectionStatus>> IEventStore.GetProjectionStatusesAsync(
        IEventDatabase database, string? tenantId, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(database);

        // #589: the tenant axis used to refuse here. It now composes the tenant-bearing shard identity
        // rather than filtering rows and hoping they line up with the untenanted registrations — see
        // projectionStatusesAsync.
        return projectionStatusesAsync(RequirePolecatDatabase(database), tenantId, ct);
    }

    /// <summary>
    ///     #589 — no daemon in this process was asked about these shards, so their runtime state is not
    ///     something this read can report.
    /// </summary>
    /// <remarks>
    ///     Deliberately not <c>"Stopped"</c>, which is what this method used to answer for every shard
    ///     unconditionally. <see cref="ShardStatus.State" /> is a fact about the RUNNING DAEMON, and
    ///     pc_event_progression does not know it — so "Stopped" was not a partial answer but a wrong
    ///     one, indistinguishable from a daemon that really had stopped, and it is the reading an
    ///     operator acts on. Marten answers "Unknown" here for the same reason. Reporting the real
    ///     state, as Fisher does, needs a daemon accessor that does not CREATE a daemon as a side
    ///     effect of being asked — Polecat's AllDaemonsAsync() builds one per database — so it is a
    ///     follow-up rather than something to improvise inside a defect fix. See polecat#589,
    ///     jasperfx#818.
    /// </remarks>
    private const string UnknownShardState = "Unknown";

    private async Task<IReadOnlyList<ProjectionStatus>> projectionStatusesAsync(
        PolecatDatabase database, string? tenantId, CancellationToken ct)
    {
        // #589: the shard identities carry the tenant ONLY when the tenant lives inside this database.
        // Under database-per-tenant each database runs its own daemon over the same unsuffixed
        // identities, so there the tenant has already done its work by selecting the database.
        var shardsAreTenantScoped = tenantId != null && IsSingleDatabase;

        var progress = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        // AllProjectionProgress runs inside Options.ResiliencePipeline the same way the session-bound
        // explorer reads do, and it is already per-database -- which is the whole point here.
        foreach (var state in await database.AllProjectionProgress(
                     shardsAreTenantScoped ? tenantId : null, ct))
        {
            // The high-water row is not a shard's progress, and it is no longer where
            // EventStoreSequence comes from either — see headSequenceAsync.
            if (!string.Equals(state.ShardName, ShardState.HighWaterMark, StringComparison.OrdinalIgnoreCase))
            {
                progress[state.ShardName] = state.Sequence;
            }
        }

        var head = await headSequenceAsync(database, ct);

        // #200: drive off the registered projection sources (not
        // AllProjectionNames(), which quote-wraps each name for SQL-list
        // display). Each source exposes its real Lifecycle and ShardNames,
        // mirroring Marten's DocumentStore.EventStoreExplorer.GetProjectionStatusesAsync.
        var statuses = new List<ProjectionStatus>();
        foreach (var source in Options.Projections.All)
        {
            var shards = source.Shards()
                .Select(shard =>
                {
                    // #589: COMPOSE the tenant-bearing identity from the registration rather than
                    // filtering rows and hoping the two spellings meet. The registrations are
                    // untenanted, the rows are suffixed, and matching them by string was the defect.
                    var effectiveName = shardsAreTenantScoped
                        ? ShardName.Compose(shard.Name.Name, shard.Name.ShardKey, tenantId, shard.Name.Version)
                        : shard.Name;

                    progress.TryGetValue(effectiveName.Identity, out var processed);
                    return new ShardStatus(effectiveName.Identity, UnknownShardState, processed, head, Error: null!);
                })
                .ToList();

            // #589: a projection with no shards reports an EMPTY shard list. It used to be given a
            // synthesised shard whose State slot held source.Lifecycle.ToString(), which made that
            // field mean a daemon state on some rows and a lifecycle on others with nothing telling a
            // consumer which it was holding. ProjectionStatus.Lifecycle already says why the list is
            // empty, so a console can render "Inline — no shards" without inferring anything.
            statuses.Add(new ProjectionStatus(source.Name, source.Lifecycle.ToString(), shards));
        }

        return statuses;
    }

    /// <summary>
    ///     #589 — the head of the event store, from <c>MAX(seq_id)</c> rather than from the persisted
    ///     high-water progression row.
    /// </summary>
    /// <remarks>
    ///     The two agree on a store whose daemon is current, and they differ exactly when it matters:
    ///     the row is WHERE THE DAEMON GOT TO, and a daemon that is not running leaves it behind.
    ///     Reporting it as <see cref="ShardStatus.EventStoreSequence" /> made every shard on a stopped
    ///     daemon look caught up — the opposite of what a projections page is opened to find out.
    ///     Marten and Fisher both read the head this way.
    /// </remarks>
    private static async Task<long> headSequenceAsync(PolecatDatabase database, CancellationToken ct)
    {
        try
        {
            return await database.FetchHighestEventSequenceNumber(ct);
        }
        catch (Exception)
        {
            // The likeliest reason this fails is that the event schema does not exist yet, which is
            // precisely when a console is most likely to be pointed at the store. Failing the whole
            // page over one number answers nothing at all — the same judgement TryCreateUsage makes
            // about the same read.
            return 0;
        }
    }

    // ---- #584 / jasperfx#810: the database dimension of the explorer reads ----
    //
    // Every explorer read below used to open the store's default session, which on a store with more
    // than one database silently picks whichever database that session resolves. The result is
    // indistinguishable from a complete answer — a console over 512 shard databases lists recent
    // streams from one of them and says nothing about the other 511 (CritterWatch#1231).
    //
    // The fix has two halves, and they are independent axes:
    //   * the database-scoped overloads open the database they are GIVEN, so a tool enumerates
    //     AllDatabases() and attributes each answer to the database it came from; and
    //   * the store-global overloads stop answering from one database. Each either fans out (the
    //     recent-streams listing, where merging is well defined) or refuses and names the database
    //     overload. Answering from one database is the one option ruled out.
    //
    // The tenant predicate is the OTHER axis and is unchanged: Polecat already applies tenant_id
    // wherever events are conjoined, and it applies on top of whichever database is opened.

    private bool IsSingleDatabase => (Options.Tenancy?.Cardinality ?? DatabaseCardinality.Single)
        == DatabaseCardinality.Single;

    /// <summary>
    ///     The store's one database, for the store-global reads that are only reachable when there is
    ///     exactly one. Callers must have passed <see cref="RequireSingleDatabase" /> first.
    /// </summary>
    private PolecatDatabase SingleDatabase =>
        Options.Tenancy?.AllDatabases() is { Count: > 0 } databases
            ? databases[0]
            : throw new InvalidOperationException(
                "This Polecat store has no database configured, so the event store explorer has nothing to read.");

    /// <summary>
    ///     #593 — the database a tenant's data lives in, or null to leave the read on the store's
    ///     default routing.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Null on a single-database store, deliberately: there is one place to read and
    ///         <see cref="ExplorerSession" /> already opens it. That keeps every single-database
    ///         store on exactly the session it used before this change.
    ///     </para>
    ///     <para>
    ///         The tenant is the <em>database</em> axis. It is not the row filter, and resolving it
    ///         here never removes one: the <c>tenant_id</c> predicate is passed separately and applies
    ///         on top, because a sharded store co-locates many tenants in each database.
    ///     </para>
    /// </remarks>
    private PolecatDatabase? DatabaseForTenant(string tenantId)
        => IsSingleDatabase ? null : Options.Tenancy!.GetDatabase(tenantId);

    private void RequireSingleDatabase(string member)
    {
        if (IsSingleDatabase) return;

        throw new NotSupportedException(
            $"Store-global {member} is not supported on this Polecat store, whose DatabaseCardinality is " +
            $"{Options.Tenancy!.Cardinality}. It would answer from whichever database the default session " +
            "resolved, and the result would be indistinguishable from a complete answer. Enumerate " +
            $"IEventStore.AllDatabases() and call the IEventDatabase overload of {member} per database " +
            "instead (jasperfx#810, polecat#584).");
    }

    private static PolecatDatabase RequirePolecatDatabase(IEventDatabase database) =>
        database as PolecatDatabase
        ?? throw new ArgumentException(
            $"Expected a Polecat database but got '{database.GetType().FullName}'. The database-scoped event " +
            "store explorer reads only accept databases this store produced through AllDatabases().",
            nameof(database));

    /// <summary>
    ///     Opens the session an explorer read runs on: the database it was given, or the store's
    ///     default routing when it was given none.
    /// </summary>
    private Internal.QuerySession ExplorerSession(PolecatDatabase? database) =>
        (Internal.QuerySession)(database == null
            ? QuerySession()
            : QuerySession(SessionOptions.ForDatabase(database)));

    /// <summary>
    ///     The store-global recent-streams listing on a multi-database store. This is the one explorer
    ///     read whose store-global answer stays meaningful across databases: "the N most recently
    ///     updated streams" is well defined over a union, because <see cref="StreamSummary" /> carries
    ///     the LastUpdatedAt the merge orders on. Its siblings refuse instead — a stream's events, a
    ///     stream's metadata and a database's projection progression are each scoped to one database
    ///     by nature, and merging them would fabricate a stream or a shard that does not exist.
    ///     <para>
    ///     Each database is capped at <paramref name="count" /> because no database can contribute more
    ///     than that to the top <paramref name="count" /> overall, so the cap costs nothing and bounds
    ///     the fan-out.
    ///     </para>
    /// </summary>
    private async Task<IReadOnlyList<StreamSummary>> FanOutRecentStreamsAsync(
        int count, string? tenantId, CancellationToken ct)
    {
        if (count <= 0) return Array.Empty<StreamSummary>();

        var databases = await ((IEventStore)this).AllDatabases();

        var merged = new List<StreamSummary>(capacity: count * Math.Max(databases.Count, 1));
        foreach (var database in databases)
        {
            merged.AddRange(await recentStreamsAsync(RequirePolecatDatabase(database), count, tenantId, ct));
        }

        return merged
            .OrderByDescending(x => x.LastUpdatedAt)
            .Take(count)
            .ToList();
    }

    // #373: one answer to "what type is this alias". This used to be a second, near-identical copy of the
    // scan that now lives on EventGraph — same projection walk, same fallback, but with a bare `catch` that
    // swallowed every exception rather than only the ArgumentOutOfRangeException an unknown alias raises.
    // Delegating also means the explorer picks up the alias registry the stream writer populates.
    private Type? ResolveAggregateType(string aggregateTypeName)
        => Events.TryResolveAggregateType(aggregateTypeName);
}
