using JasperFx;
using JasperFx.Events;
using JasperFx.Events.ComplianceTests;
using JasperFx.Events.Fetching;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Batching;
using Polecat.Events;
using Polecat.Linq;

namespace Polecat.Tests.Compliance;

/// <summary>
///     Polecat's implementation of the cross-store event sourcing compliance seam, closing it over
///     Polecat's <c>IEventStore&lt;IDocumentSession, IQuerySession&gt;</c> session pair.
/// </summary>
public class PolecatComplianceFixture : EventStoreComplianceFixture<IDocumentSession, IQuerySession>
{
    private readonly List<object> _disposables = new();
    private DocumentStore _store = null!;

    public DocumentStore Store => _store;

    protected override async Task BuildStoreAsync(ComplianceStoreConfig config)
    {
        var options = OptionsFor(config);

        _store = new DocumentStore(options);
        _disposables.Add(_store);

        // Polecat applies schema changes explicitly rather than lazily -- one of the eight
        // divergences the compliance seam exists to absorb.
        await _store.Database.ApplyAllConfiguredChangesToDatabaseAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Translate the store-neutral configuration into Polecat's own <see cref="StoreOptions" />.
    /// </summary>
    /// <remarks>
    ///     Split out of <see cref="BuildStoreAsync" /> for jasperfx#732: the coordinator suite needs
    ///     the SAME configuration replayed onto a store built by the documented DI registration
    ///     rather than by hand, and pointed at the same database and schema so the per-test
    ///     <see cref="CleanEventDataAsync" /> isolation covers both.
    /// </remarks>
    private static StoreOptions OptionsFor(ComplianceStoreConfig config)
    {
        var schemaName = (config.SchemaName ?? "compliance").ToLowerInvariant();

        var options = new StoreOptions
        {
            ConnectionString = connectionStringFor(config),
            AutoCreateSchemaObjects = AutoCreate.All,
            DatabaseSchemaName = schemaName,
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        };

        if (config.MaxConcurrentRebuildsPerDatabase.HasValue)
        {
            options.DaemonSettings.MaxConcurrentRebuildsPerDatabase = config.MaxConcurrentRebuildsPerDatabase;
        }

        if (config.StreamIdentity.HasValue)
        {
            options.Events.StreamIdentity = config.StreamIdentity.Value;
        }

        if (config.EnableCorrelationTracking)
        {
            options.Events.EnableCorrelationId = true;
            options.Events.EnableCausationId = true;
        }

        // jasperfx#737: opt-in like correlation — the event query suite filters on user_name.
        if (config.EnableUserNameTracking)
        {
            options.Events.EnableUserName = true;
        }

        if (config.EnableHeaders)
        {
            options.Events.EnableHeaders = true;
        }

        // Wave 8: conjoined event tenancy. TenancyStyle is already the shared
        // JasperFx.MultiTenancy.TenancyStyle (see GlobalUsings), so this is the whole mapping --
        // opening a tenant-scoped session needs nothing, because OpenSession(IEventDatabase,
        // tenantId) is already on the shared IEventStore<,>.
        if (config.ConjoinedEventTenancy)
        {
            options.Events.TenancyStyle = TenancyStyle.Conjoined;
        }

        config.ApplyTo(new PolecatComplianceRegistrar(options));

        return options;
    }

    private static string connectionStringFor(ComplianceStoreConfig config)
    {
        if (!config.MaxPoolSize.HasValue)
        {
            return ConnectionSource.ConnectionString;
        }

        return new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            MaxPoolSize = config.MaxPoolSize.Value
        }.ConnectionString;
    }

    public override IDocumentSession OpenSession() => _store.LightweightSession();

    public override Task SaveChangesAsync(IDocumentSession session, CancellationToken token)
        => session.SaveChangesAsync(token);

    public override Task<T?> LoadDocumentAsync<T>(IQuerySession session, object id, CancellationToken token)
        where T : class
        => id switch
        {
            Guid guidId => session.LoadAsync<T>(guidId, token),
            int intId => session.LoadAsync<T>(intId, token),
            long longId => session.LoadAsync<T>(longId, token),
            string stringId => session.LoadAsync<T>(stringId, token),
            _ => throw new ArgumentOutOfRangeException(nameof(id),
                $"Polecat cannot load documents by an identity of type {id.GetType().FullName}")
        };

    public override void StoreDocument<T>(IDocumentSession session, T document) => session.Store(document);

    public override IEventStoreOperations EventsFor(IDocumentSession session) => session.Events;

    public override string? CorrelationIdFor(IDocumentSession session) => session.CorrelationId;

    public override string? CausationIdFor(IDocumentSession session) => session.CausationId;

    public override void SetCorrelationId(IDocumentSession session, string? correlationId)
        => session.CorrelationId = correlationId;

    // jasperfx#737: the event query suite filters on the user_name column, which Polecat stamps
    // from the session's LastModifiedBy (#237/#239) when EnableUserName is on.
    public override void SetUserName(IDocumentSession session, string? userName)
        => session.LastModifiedBy = userName;

    public override IEventStore EventStore => _store;

    public override IEnumerable<Type> AllAggregateTypes() => _store.Options.Projections.AllAggregateTypes();

    public override IComplianceBatch CreateBatch(IQuerySession session)
        => new PolecatComplianceBatch(session.CreateBatchQuery());

    public override IEventRegistry Registry => _store.Options.EventGraph;

    public override async Task CleanEventDataAsync()
    {
        await _store.Advanced.Clean.DeleteAllEventDataAsync().ConfigureAwait(false);
        await _store.Advanced.Clean.DeleteAllDocumentsAsync().ConfigureAwait(false);
    }

    public override async Task<IProjectionDaemon> StartDaemonAsync()
    {
        var daemon = await _store.BuildProjectionDaemonAsync().ConfigureAwait(false);
        _disposables.Add(daemon);

        await daemon.StartAllAsync().ConfigureAwait(false);

        return daemon;
    }

    public override Task WaitForNonStaleProjectionDataAsync(TimeSpan timeout)
        => _store.Database.WaitForNonStaleProjectionDataAsync(timeout);

    // A flat table is not a document, so there is no supported Polecat read path for its rows. The
    // schema comes from the store rather than the caller so the compliance suite never has to spell
    // a qualified name, and the reader is deliberately untyped: the suite asserts values, not the
    // SqlClient types they arrive as.
    // IEventDataMasking is shared (lifted in jasperfx#635), but the entry point that hands one out
    // is not: Polecat spells it on its own Advanced surface, Marten on IDocumentStore.Advanced, and
    // the two share no interface. This member is the whole of that gap.
    public override Task ApplyEventDataMaskingAsync(
        Action<JasperFx.Events.Protected.IEventDataMasking> configure, CancellationToken token)
        => _store.Advanced.ApplyEventDataMasking(configure, token);

    public override async Task<IReadOnlyList<IReadOnlyDictionary<string, object?>>> QueryTableAsync(
        string tableName, CancellationToken token)
    {
        var rows = new List<IReadOnlyDictionary<string, object?>>();

        await using var conn = _store.Database.CreateStorageConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await using var command = conn.CreateCommand();
        command.CommandText =
            $"select * from [{_store.Options.DatabaseSchemaName}].[{tableName}]";

        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = await reader.IsDBNullAsync(i, token).ConfigureAwait(false)
                    ? null
                    : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    /// <summary>
    ///     Polecat derives live aggregators automatically from self-aggregating types; there is no
    ///     explicit registration call to make.
    /// </summary>
    public override bool SupportsLiveAggregationRegistration => false;

    // ---------------------------------------------------------------------------------------------
    // Wave 15 (JasperFx 2.64.0). Everything below is a capability the compliance library added a gate
    // for, defaulting false so a store can enroll the suite across the bump and flip the gate when
    // the behaviour lands. Polecat already shipped all of them, so the gates go true here and the
    // seam members are one-line forwards into the surface each suite is actually about.
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    ///     jasperfx#764 / #549. Polecat maintains a <c>pc_natural_key_{type}</c> lookup table and
    ///     routes the FetchForWriting / FetchForExclusiveWriting / FetchLatest triple through it.
    /// </summary>
    public override bool SupportsNaturalKeys => true;

    /// <summary>
    ///     #561 / jasperfx#752. Polecat routes every event hydration path through the shared
    ///     <c>EventRegistry.Upcasters</c> and implements <c>IUpcastPayload</c> over its own row
    ///     reader and serializer.
    /// </summary>
    public override bool SupportsUpcasting => true;

    // #364. Both operators are extensions in Polecat.Events over the store's raw-event queryable,
    // which is why they cannot be reached through any shared interface. The single Where() is the
    // seam's contract, not a convenience: the HasTag facts next door depend on the predicate staying
    // in ONE tree.
    public override bool SupportsAggregateToLinqOperators => true;

    public override Task<T?> AggregateEventsToAsync<T>(IQuerySession session,
        System.Linq.Expressions.Expression<Func<IEvent, bool>>? filter, T? initialState, CancellationToken token)
        where T : class
    {
        IQueryable<IEvent> queryable = session.Events.QueryAllRawEvents();
        if (filter != null)
        {
            queryable = queryable.Where(filter);
        }

        return queryable.AggregateToAsync(initialState, token);
    }

    public override Task<IReadOnlyList<T>> AggregateEventsToManyAsync<T>(IQuerySession session,
        System.Linq.Expressions.Expression<Func<IEvent, bool>>? filter, CancellationToken token)
        where T : class
    {
        IQueryable<IEvent> queryable = session.Events.QueryAllRawEvents();
        if (filter != null)
        {
            queryable = queryable.Where(filter);
        }

        return queryable.AggregateToManyAsync<T>(token);
    }

    // The DCB HasTag LINQ marker. HasTagFilter has to invoke Polecat's OWN extension rather than
    // hand-roll an equivalent lambda: HasTagParser matches on the method's declaring type, so an
    // expression built any other way carries the wrong MethodInfo and is never recognized. Building
    // the expression deliberately does not validate the tag type -- an unregistered tag throws at
    // query translation, which is the behaviour has_tag_for_an_unregistered_tag_type_throws pins.
    public override bool SupportsHasTagLinqPredicates => true;

    public override async Task<IReadOnlyList<IEvent>> QueryRawEventsAsync(IQuerySession session,
        System.Linq.Expressions.Expression<Func<IEvent, bool>> filter, CancellationToken token)
        => await session.Events.QueryAllRawEvents().Where(filter).ToListAsync(token).ConfigureAwait(false);

    public override System.Linq.Expressions.Expression<Func<IEvent, bool>> HasTagFilter<TTag>(TTag value)
        => e => e.HasTag(value);

    // #370 (parity with marten#5053). Polecat ships FetchStreamStatePlan and FetchStreamPlan, both
    // implementing IQueryPlan<T> AND IBatchQueryPlan<T>, so one plan instance serves both routes --
    // which is exactly what the suite's [Theory(batched)] parameter exists to tell apart.
    public override bool SupportsStreamQueryPlans => true;

    public override async Task<StreamState?> FetchStreamStateByPlanAsync(
        IQuerySession session, object streamIdentity, bool batched, CancellationToken token)
    {
        var plan = streamIdentity switch
        {
            Guid streamId => new FetchStreamStatePlan(streamId),
            string streamKey => new FetchStreamStatePlan(streamKey),
            _ => throw new ArgumentOutOfRangeException(nameof(streamIdentity),
                $"Polecat streams are identified by Guid or string, not {streamIdentity.GetType().FullName}")
        };

        if (!batched)
        {
            return await session.QueryByPlanAsync(plan, token).ConfigureAwait(false);
        }

        var batch = session.CreateBatchQuery();
        var task = batch.QueryByPlan(plan);
        await batch.Execute(token).ConfigureAwait(false);

        return await task.ConfigureAwait(false);
    }

    public override async Task<IReadOnlyList<IEvent>> FetchStreamByPlanAsync(
        IQuerySession session, object streamIdentity, long version, bool batched, CancellationToken token)
    {
        var plan = streamIdentity switch
        {
            Guid streamId => new FetchStreamPlan(streamId, version),
            string streamKey => new FetchStreamPlan(streamKey, version),
            _ => throw new ArgumentOutOfRangeException(nameof(streamIdentity),
                $"Polecat streams are identified by Guid or string, not {streamIdentity.GetType().FullName}")
        };

        if (!batched)
        {
            return await session.QueryByPlanAsync(plan, token).ConfigureAwait(false);
        }

        var batch = session.CreateBatchQuery();
        var task = batch.QueryByPlan(plan);
        await batch.Execute(token).ConfigureAwait(false);

        return await task.ConfigureAwait(false);
    }

    // #318. UnArchiveStream is genuinely Polecat-only -- Marten has no equivalent, which is why the
    // gate defaults false and the operation is off the shared IEventStoreOperations. Like
    // ArchiveStream it takes effect on SaveChanges rather than immediately.
    public override bool SupportsUnarchiveStream => true;

    public override void UnArchiveStream(IDocumentSession session, object streamIdentity)
    {
        switch (streamIdentity)
        {
            case Guid streamId:
                session.Events.UnArchiveStream(streamId);
                break;
            case string streamKey:
                session.Events.UnArchiveStream(streamKey);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(streamIdentity),
                    $"Polecat streams are identified by Guid or string, not {streamIdentity.GetType().FullName}");
        }
    }

    // jasperfx#763 / #420. Polecat has a real message outbox (StoreOptions.MessageOutbox, defaulting
    // to NulloMessageOutbox) that the projection batch vends from, so the outbox facts run rather
    // than skip.
    public override bool SupportsMessageOutbox => true;

    /// <summary>
    ///     SQL Server READ COMMITTED SNAPSHOT is NOT on by default, so a second session probing a row
    ///     the open write transaction is mid-way through writing takes a shared lock and blocks until
    ///     that transaction commits -- and the transaction is being held open by the very hook doing
    ///     the probing. That is a deadlock, not a slow test, so the gate stays false.
    /// </summary>
    /// <remarks>
    ///     The suite is explicit that skipping costs one fact and guessing wrong hangs the suite.
    ///     Polecat's own commit-hook ordering is covered by the un-probed fact next to it, which is
    ///     not gated. Revisit if the compliance store is ever built with RCSI enabled.
    /// </remarks>
    public override bool SupportsCommitVisibilityProbe => false;

    // The declared allow list has to survive the hop into Polecat's own SubscriptionBase wrapper --
    // see the registrar's Subscribe, which replays each type onto ISubscriptionOptions.IncludeType.
    public override bool SupportsSubscriptionEventFilters => true;

    // jasperfx#769. Polecat subclasses the shared harness as Polecat.Events.TestSupport
    // .ProjectionScenario and exposes it on Advanced.EventProjectionScenario.
    public override bool SupportsProjectionScenario => true;

    /// <remarks>
    ///     A FORWARD into Polecat's own documented entry point, deliberately not a re-implementation.
    ///     The three lines behind EventProjectionScenario (construct, configure, ExecuteAsync) are
    ///     trivial to inline here, and inlining them would pass the whole suite while Polecat's
    ///     advertised entry point was missing or wired to the wrong store. The route is under test as
    ///     much as the harness is.
    /// </remarks>
    public override Task RunProjectionScenarioAsync(
        Action<JasperFx.Events.TestSupport.ProjectionScenario<IDocumentSession, IQuerySession>> configure,
        CancellationToken token)
        => _store.Advanced.EventProjectionScenario(scenario => configure(scenario), token);

    /// <remarks>
    ///     For the one fact the run entry point structurally cannot reach: a scenario's steps are
    ///     consumed by its first run, so proving a second ExecuteAsync fails loudly rather than
    ///     passing as a silent no-op needs a handle on the instance, and the entry point constructs
    ///     one and throws it away.
    /// </remarks>
    public override JasperFx.Events.TestSupport.ProjectionScenario<IDocumentSession, IQuerySession>
        CreateProjectionScenario() => new Polecat.Events.TestSupport.ProjectionScenario(_store);

    /// <summary>
    ///     jasperfx#732. Build and START a host registering Polecat the way the docs say to, so the
    ///     suite can observe whether the documented DI registration actually produces a reachable
    ///     <see cref="IProjectionCoordinator" />. Every other daemon suite drives a daemon this
    ///     fixture built by hand, which can never see that gap -- fisher#138 shipped exactly it and
    ///     passed all 37 suites while it did.
    /// </summary>
    /// <remarks>
    ///     <c>AddProjectionCoordinator</c> rather than <c>AddAsyncDaemon</c>, and the two are
    ///     mutually exclusive: only the coordinator registers
    ///     <see cref="IProjectionCoordinator" />, so a host built on the daemon path would fail the
    ///     first fact on a GetRequiredService throw. Solo mode because the suite runs one node and
    ///     HotCold would have it race itself for the shard locks.
    /// </remarks>
    protected override async Task<IComplianceCoordinatorHost<IDocumentSession>> StartCoordinatorHostAsync(
        ComplianceStoreConfig config, bool includeAncillaryStore)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddLogging();

        // The same options the fixture's own store was built from -- same database, same schema --
        // so CleanEventDataAsync between tests covers the hosted store too.
        builder.Services.AddPolecat(OptionsFor(config))
            .AddProjectionCoordinator(DaemonMode.Solo);

        if (includeAncillaryStore)
        {
            var ancillary = OptionsFor(config);
            ancillary.DatabaseSchemaName = ancillary.DatabaseSchemaName + "_ancillary";

            builder.Services.AddPolecatStore<IComplianceAncillaryStore>(_ => ancillary);
            builder.Services.AddPolecat(OptionsFor(config))
                .AddProjectionCoordinator<IComplianceAncillaryStore>(DaemonMode.Solo);
        }

        var host = builder.Build();
        await host.StartAsync(Cancellation).ConfigureAwait(false);

        return new PolecatCoordinatorHost(host);
    }

    /// <summary>
    ///     Polecat supports ancillary stores through <c>AddPolecatStore&lt;T&gt;</c>, and
    ///     <c>AddProjectionCoordinator&lt;T&gt;</c> gives each one its own marker-typed coordinator.
    /// </summary>
    public override bool SupportsAncillaryCoordinators => true;

    /// <remarks>
    ///     The marker-typed registration lands on Polecat's OWN
    ///     <c>Polecat.Events.Daemon.Coordination.IProjectionCoordinator&lt;T&gt;</c> only -- unlike
    ///     the non-generic case there is no forwarder onto the JasperFx base interface, so resolving
    ///     the shared closed generic here would be a DI failure rather than an assertion failure.
    /// </remarks>
    public override IProjectionCoordinator AncillaryCoordinatorFrom(IServiceProvider services)
        => services.GetRequiredService<
            Polecat.Events.Daemon.Coordination.IProjectionCoordinator<IComplianceAncillaryStore>>();

    /// <summary>
    ///     Marker for the ancillary store the coordinator suite registers. A fixture-local type
    ///     because every product constrains ancillary markers to its own store interface, so only a
    ///     consumer can name one.
    /// </summary>
    public interface IComplianceAncillaryStore : IDocumentStore;

    internal class PolecatCoordinatorHost : IComplianceCoordinatorHost<IDocumentSession>
    {
        private readonly IHost _host;

        public PolecatCoordinatorHost(IHost host)
        {
            _host = host;
        }

        public IServiceProvider Services => _host.Services;

        public IDocumentSession OpenSession()
            => _host.Services.GetRequiredService<IDocumentStore>().LightweightSession();

        /// <summary>
        ///     #591 / jasperfx#818 — the HOSTED store as IEventStore, which is the one store in the
        ///     suite set with a genuinely discoverable running daemon.
        /// </summary>
        /// <remarks>
        ///     Deliberately not the fixture's own store instance. The fixture builds its store by hand
        ///     and registers no coordinator, so asking it what state its shards are in can only ever
        ///     answer "no daemon here to ask" — which is a real case worth pinning, and the reason the
        ///     daemon-visible half of the ruling needs this store instead.
        /// </remarks>
        public JasperFx.Events.IEventStore EventStore
            => (JasperFx.Events.IEventStore)_host.Services.GetRequiredService<IDocumentStore>();

        public async ValueTask DisposeAsync()
        {
            // StopAsync before Dispose, deliberately: IHost.Dispose does NOT stop a started host, so
            // disposing alone leaks the coordinator's daemon agents into the next test -- which on a
            // shared SQL Server means they go on holding shard locks and polling a schema the next
            // test is tearing down.
            await _host.StopAsync().ConfigureAwait(false);
            _host.Dispose();
        }
    }

    public override async ValueTask DisposeAsync()
    {
        foreach (var disposable in _disposables)
        {
            switch (disposable)
            {
                case IAsyncDisposable asyncDisposable:
                    await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                    break;
                case IDisposable syncDisposable:
                    syncDisposable.Dispose();
                    break;
            }
        }

        _disposables.Clear();
    }

    internal class PolecatComplianceRegistrar : IComplianceStoreRegistrar
    {
        private readonly StoreOptions _options;

        public PolecatComplianceRegistrar(StoreOptions options)
        {
            _options = options;
        }

        public void AddEventType(Type eventType) => _options.Events.AddEventType(eventType);

        public ITagTypeRegistration RegisterTagType<TTag>(string tableSuffix) where TTag : notnull
            => _options.Events.RegisterTagType<TTag>(tableSuffix);

        // #475: binary event serialization. Both members carry throwing defaults on the seam because
        // the capability is opt-in, so implementing them is the whole of what enrolls Polecat in
        // BinaryEventSerializationCompliance. The serializer arriving here is the SHARED
        // JasperFx.Events.IEventBinarySerializer, which is exactly the point of the promotion --
        // the suite writes one and every store takes it.
        public void UseBinarySerializer<TEvent>(IEventBinarySerializer serializer) where TEvent : notnull
            => _options.Events.UseBinarySerializer<TEvent>(serializer);

        public void SetDefaultBinarySerializer(IEventBinarySerializer serializer)
            => _options.Events.DefaultBinarySerializer = serializer;

        public void Snapshot<TDoc>(SnapshotLifecycle lifecycle) where TDoc : notnull
            => _options.Projections.Snapshot<TDoc>(lifecycle);

        // Live aggregators are derived automatically -- see SupportsLiveAggregationRegistration.
        public void LiveAggregation<TDoc>() where TDoc : notnull
        {
        }

        // Polecat derives everything it needs about a value type from ValueTypeInfo when it builds
        // the DocumentMapping, so there is no registration call to make here. Marten needs the type
        // registered up front before it can use it in LINQ and identity mapping, which is why the
        // seam member exists at all -- the mirror image of LiveAggregation above.
        public void RegisterValueType<TValue>() where TValue : notnull
        {
        }

        // #424 landed the re-exposure on EventStoreOptions, so this goes through the ordinary
        // configuration surface rather than reaching past it to StoreOptions.EventGraph.
        public void AddMaskingRule<TEvent>(Action<TEvent> rule) where TEvent : notnull
            => _options.Events.AddMaskingRuleForProtectedInformation(rule);

        public void AddMaskingRule<TEvent>(Func<TEvent, TEvent> rule) where TEvent : notnull
            => _options.Events.AddMaskingRuleForProtectedInformation(rule);

        public void AddProjection(ProjectionBase projection, ProjectionLifecycle lifecycle)
            => _options.Projections.Add((IProjectionSource<IDocumentSession, IQuerySession>)projection, lifecycle);

        // #478 / jasperfx#674. Both halves are needed and they are different things: the suite's
        // RecordingAggregateWriteCache has to be the instance the store actually resolves (that is
        // how the suite counts hits at all), and the type has to be enrolled, because caching is
        // opt-in per aggregate type rather than store-wide.
        public void CacheAggregatesForWriting<TDoc>(IAggregateWriteCache cache) where TDoc : class
        {
            _options.Events.AggregateWriteCaching.Cache = cache;
            _options.Events.CacheAggregatesForWriting<TDoc>();
        }

        // Wave 8: the name is pinned to ComplianceSubscription.SubscriptionName rather than left to
        // default, because the products disagree on whether an unnamed subscription takes its short
        // or full type name and daemon progression is keyed on it.
        //
        // Wave 15 / jasperfx#768 adds the second half. A declared event allow list has to survive a
        // hop no shared code can make for it: Subscribe(ISubscription) wraps the subscription in
        // Polecat's own SubscriptionBase, and the daemon reads filters off the WRAPPER, which copies
        // none across. Replaying each type onto ISubscriptionOptions.IncludeType is the whole of it,
        // and is what SupportsSubscriptionEventFilters attests to.
        public void Subscribe(ComplianceSubscription subscription)
            => _options.Projections.Subscribe(subscription, x =>
            {
                x.Name = ComplianceSubscription.SubscriptionName;

                foreach (var eventType in subscription.IncludedEventTypes)
                {
                    x.IncludeType(eventType);
                }
            });

        // #561 / jasperfx#752. One registrar member covers every registration shape -- typed sync,
        // typed async-only, raw JsonDocument -- because UpcastTransformation is the shared carrier
        // they all funnel into, and the suite builds them through the shared factories.
        public void Upcast(JasperFx.Events.Upcasting.UpcastTransformation transformation)
            => _options.Events.Upcasters.Register(transformation);

        // jasperfx#763. Polecat spells the outbox as a single settable property on the event store
        // options, which the projection batch asks for a batch from once per update. The suite's
        // RecordingMessageOutbox has to be the instance the store actually resolves -- that is how
        // it counts publishes at all, and a nonzero count is the fact that separates "implemented"
        // from "silently dropped on the floor" (#420).
        public void UseMessageOutbox(RecordingMessageOutbox outbox)
            => _options.Events.MessageOutbox = outbox;

        // Wave 13 / jasperfx#725: composites. The seam member carries a throwing default because a
        // composite cannot be constructed by a suite -- PolecatCompositeProjection's constructor is
        // internal and needs StoreOptions -- so the forward goes through Polecat's own
        // Projections.CompositeProjectionFor and the builder adapts the one call the suite makes,
        // Snapshot<T>(stageNumber), which Polecat spells identically (void-returning).
        public void AddCompositeProjection(string name, Action<IComplianceCompositeBuilder> configure)
            => _options.Projections.CompositeProjectionFor(name,
                composite => configure(new ComplianceCompositeBuilder(composite)));
    }

    internal class ComplianceCompositeBuilder : IComplianceCompositeBuilder
    {
        private readonly Polecat.Projections.PolecatCompositeProjection _composite;

        public ComplianceCompositeBuilder(Polecat.Projections.PolecatCompositeProjection composite)
        {
            _composite = composite;
        }

        public void Snapshot<TDoc>(int stageNumber) where TDoc : notnull
            => _composite.Snapshot<TDoc>(stageNumber);
    }

    internal class PolecatComplianceBatch : IComplianceBatch
    {
        private readonly IBatchedQuery _batch;

        public PolecatComplianceBatch(IBatchedQuery batch)
        {
            _batch = batch;
        }

        public Task<bool> EventsExist(EventTagQuery query) => _batch.EventsExist(query);

        public Task<IEventBoundary<T>> FetchForWritingByTags<T>(EventTagQuery query) where T : class
            => _batch.FetchForWritingByTags<T>(query);

        public Task Execute(CancellationToken token = default) => _batch.Execute(token);
    }
}
