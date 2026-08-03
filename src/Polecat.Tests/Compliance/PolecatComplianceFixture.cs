using JasperFx;
using JasperFx.Events;
using JasperFx.Events.ComplianceTests;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Batching;

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

        config.ApplyTo(new PolecatComplianceRegistrar(options));

        _store = new DocumentStore(options);
        _disposables.Add(_store);

        // Polecat applies schema changes explicitly rather than lazily -- one of the eight
        // divergences the compliance seam exists to absorb.
        await _store.Database.ApplyAllConfiguredChangesToDatabaseAsync().ConfigureAwait(false);
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

    /// <summary>
    ///     Polecat derives live aggregators automatically from self-aggregating types; there is no
    ///     explicit registration call to make.
    /// </summary>
    public override bool SupportsLiveAggregationRegistration => false;

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

        public void Snapshot<TDoc>(SnapshotLifecycle lifecycle) where TDoc : notnull
            => _options.Projections.Snapshot<TDoc>(lifecycle);

        // Live aggregators are derived automatically -- see SupportsLiveAggregationRegistration.
        public void LiveAggregation<TDoc>() where TDoc : notnull
        {
        }

        public void AddProjection(ProjectionBase projection, ProjectionLifecycle lifecycle)
            => _options.Projections.Add((IProjectionSource<IDocumentSession, IQuerySession>)projection, lifecycle);
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
