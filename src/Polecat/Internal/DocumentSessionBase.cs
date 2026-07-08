using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Events.Aggregation;
using Polecat.Exceptions;
using Polecat.Internal.Operations;
using Polecat.Internal.Sessions;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Metadata;
using Polecat.Projections;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal;

/// <summary>
///     Base class for document sessions. Handles operation queueing, event stream
///     processing, and SaveChangesAsync. Uses IAlwaysConnectedLifetime for
///     persistent connection + transaction management.
/// </summary>
internal abstract class DocumentSessionBase : QuerySession, IDocumentSession
{
    private readonly WorkTracker _workTracker = new();
    private readonly IInlineProjection<IDocumentSession>[] _inlineProjections;
    private readonly IReadOnlyList<IDocumentSessionListener> _sessionListeners;
    private readonly IAlwaysConnectedLifetime _transactional;
    private readonly List<ITransactionParticipant> _transactionParticipants = new();
    private EventOperations? _eventOperations;

    protected DocumentSessionBase(
        StoreOptions options,
        IAlwaysConnectedLifetime lifetime,
        DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer,
        EventGraph eventGraph,
        IInlineProjection<IDocumentSession>[] inlineProjections,
        string tenantId,
        IReadOnlyList<IDocumentSessionListener>? sessionListeners = null)
        : base(options, lifetime, providers, tableEnsurer, eventGraph, tenantId)
    {
        _transactional = lifetime;
        _inlineProjections = inlineProjections;
        _sessionListeners = sessionListeners ?? Array.Empty<IDocumentSessionListener>();
    }

    public IWorkTracker PendingChanges => _workTracker;

    Polecat.Events.IQueryEventStore IQuerySession.Events => EventOps;
    public new Polecat.Events.IEventOperations Events => EventOps;

    private EventOperations EventOps =>
        _eventOperations ??= new EventOperations(this, _eventGraph, Options, _workTracker, TenantId);

    internal WorkTracker WorkTracker => _workTracker;

    internal EventGraph EventGraph => _eventGraph;

    /// <summary>
    ///     Access the transactional connection's active transaction (if any).
    /// </summary>
    internal SqlTransaction? ActiveTransaction => _transactional.Transaction;

    /// <summary>
    ///     Transaction participants registered on this session.
    ///     Exposed internally for batch access by the async daemon.
    /// </summary>
    internal IReadOnlyList<ITransactionParticipant> TransactionParticipants => _transactionParticipants;

    public void AddTransactionParticipant(ITransactionParticipant participant)
    {
        _transactionParticipants.Add(participant);
    }

    internal async Task BeginTransactionAsync(CancellationToken token)
    {
        if (_transactional.Transaction != null) return;
        await _transactional.BeginTransactionAsync(token);
    }

    internal async Task EnsureTableForProviderAsync(DocumentProvider provider, CancellationToken token)
    {
        await _tableEnsurer.EnsureTablesAsync([provider], token);
    }

    public void Store<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _providers.GetProvider<T>();
        _workTracker.Add(BuildClosedShapeWrite(document, provider, WriteKind.Upsert));
        OnDocumentStored(typeof(T), provider.Mapping.GetId(document), document);
    }

    // #273 E2b/E2c/E2e: every document write goes through the shared closed-shape operations
    // (wrapped for the bespoke unit-of-work currency until the op-currency flip); subclass
    // types resolve a SubClassPolecatStorage delegating to the hierarchy root's storage.
    // A tenant override (IDocumentSession.ForTenant) wraps the session in a
    // TenantScopedStorageSession so flush-time metadata binders write the override tenant.
    internal Operations.ClosedShapeOperationAdapter BuildClosedShapeWrite<T>(T document, DocumentProvider provider,
        WriteKind kind, string? tenantOverride = null) where T : notnull
    {
        Weasel.Storage.IStorageSession session = (Weasel.Storage.IStorageSession)this;
        var storage = (Weasel.Storage.IDocumentStorage<T>)session.StorageFor<T>();
        if (tenantOverride is not null && tenantOverride != TenantId)
        {
            session = new TenantScopedStorageSession(session, tenantOverride);
        }

        storage.Store(session, document); // id assignment + identity-map/version bookkeeping

        var op = kind switch
        {
            WriteKind.Insert => storage.Insert(document, session, session.TenantId),
            WriteKind.Update => storage.Update(document, session, session.TenantId),
            _ => storage.Upsert(document, session, session.TenantId)
        };

        CaptureExpectedRevision(op, document);

        return new Operations.ClosedShapeOperationAdapter(op, session, document, provider.Mapping.GetId(document));
    }

    /// <summary>
    ///     Runtime-type (object) variant of <see cref="BuildClosedShapeWrite{T}" /> for
    ///     StoreObjects (#273 E2e) — dispatches through the non-generic object-write bridge.
    /// </summary>
    internal Operations.ClosedShapeOperationAdapter BuildClosedShapeObjectWrite(object document,
        DocumentProvider provider, string? tenantOverride = null)
    {
        Weasel.Storage.IStorageSession session = (Weasel.Storage.IStorageSession)this;
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectWriteStorage)session.StorageFor(document.GetType());
        if (tenantOverride is not null && tenantOverride != TenantId)
        {
            session = new TenantScopedStorageSession(session, tenantOverride);
        }

        storage.StoreObject(session, document);
        var op = storage.UpsertObject(document, session, session.TenantId);
        CaptureExpectedRevision(op, document);

        return new Operations.ClosedShapeOperationAdapter(op, session, document, provider.Mapping.GetId(document));
    }

    // Numeric revisions: the doc-carried version is the equality expectation (0 = new/auto),
    // matching the bespoke pipeline's expectedRevision capture. Shared with the projection
    // storage (#273 E2e).
    internal static void CaptureExpectedRevision(Weasel.Storage.IStorageOperation op, object document)
    {
        if (op is Weasel.Storage.IRevisionedOperation revisioned)
        {
            revisioned.Revision = document switch
            {
                ILongVersioned longVersioned => longVersioned.Version,
                IRevisioned rev => rev.Version,
                _ => 0
            };
        }
    }

    internal enum WriteKind
    {
        Upsert,
        Insert,
        Update
    }


    public void Store<T>(params T[] documents) where T : notnull
    {
        foreach (var doc in documents)
        {
            Store(doc);
        }
    }

    public void StoreObjects(IEnumerable<object> documents)
    {
        // Per-document runtime-type dispatch through the closed-shape object-write bridge
        // (#273 E2e). Mirrors Store<T> otherwise.
        foreach (var document in documents)
        {
            if (document is null) continue;

            var documentType = document.GetType();
            SyncMetadata(document);
            var provider = _providers.GetProvider(documentType);
            _workTracker.Add(BuildClosedShapeObjectWrite(document, provider));
            OnDocumentStored(documentType, provider.Mapping.GetId(document), document);
        }
    }

    public void Insert<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _providers.GetProvider<T>();
        _workTracker.Add(BuildClosedShapeWrite(document, provider, WriteKind.Insert));
        OnDocumentStored(typeof(T), provider.Mapping.GetId(document), document);
    }

    public void Update<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _providers.GetProvider<T>();
        _workTracker.Add(BuildClosedShapeWrite(document, provider, WriteKind.Update));
        OnDocumentStored(typeof(T), provider.Mapping.GetId(document), document);
    }

    public void Delete<T>(T document) where T : notnull
    {
        var provider = _providers.GetProvider<T>();
        var session = (Weasel.Storage.IStorageSession)this;
        var storage = (Weasel.Storage.IDocumentStorage<T>)session.StorageFor<T>();
        var deletion = storage.DeleteForDocument(document, TenantId);
        _workTracker.Add(new Operations.ClosedShapeOperationAdapter(
            deletion, session, document, provider.Mapping.GetId(document)));

        // Sync ISoftDeleted properties in memory
        if (document is ISoftDeleted softDeleted && provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            softDeleted.Deleted = true;
            softDeleted.DeletedAt = DateTimeOffset.UtcNow;
        }
    }

    public void Delete<T>(Guid id) where T : class
    {
        DeleteByObjectId<T>(id);
    }

    public void Delete<T>(string id) where T : class
    {
        DeleteByObjectId<T>(id);
    }

    public void Delete<T>(int id) where T : class
    {
        DeleteByObjectId<T>(id);
    }

    public void Delete<T>(long id) where T : class
    {
        DeleteByObjectId<T>(id);
    }

    // #273 E2c/E2e: all by-id deletions route through the closed-shape storage layer.
    private void DeleteByObjectId<T>(object id) where T : class
    {
        var session = (Weasel.Storage.IStorageSession)this;
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectStorage<T>)session.StorageFor<T>();
        _workTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.DeletionForObjectId(id, TenantId), session, id, id));
    }

    // #273 E2e: hard deletions route through the closed-shape storage layer.
    public void HardDelete<T>(T document) where T : notnull
    {
        var session = (Weasel.Storage.IStorageSession)this;
        var storage = (Weasel.Storage.IDocumentStorage<T>)session.StorageFor<T>();
        _workTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.HardDeleteForDocument(document, TenantId), session, document,
            _providers.GetProvider<T>().Mapping.GetId(document)));
    }

    public void HardDelete<T>(Guid id) where T : class => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(string id) where T : class => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(int id) where T : class => HardDeleteByObjectId<T>(id);

    public void HardDelete<T>(long id) where T : class => HardDeleteByObjectId<T>(id);

    private void HardDeleteByObjectId<T>(object id) where T : class
    {
        var session = (Weasel.Storage.IStorageSession)this;
        var storage = (Polecat.Storage.ClosedShape.IPolecatObjectStorage<T>)session.StorageFor<T>();
        _workTracker.Add(new Operations.ClosedShapeOperationAdapter(
            storage.HardDeletionForObjectId(id, TenantId), session, id, id));
    }

    public void DeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var memberFactory = new MemberFactory(Options, provider.Mapping);
        var whereParser = new WhereClauseParser(memberFactory);
        var fragment = whereParser.Parse(predicate.Body);

        IStorageOperation op = provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? new SoftDeleteWhereOperation(provider.Mapping, TenantId, fragment)
            : new DeleteWhereOperation(provider.Mapping, TenantId, fragment);
        _workTracker.Add(op);
    }

    public void HardDeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var memberFactory = new MemberFactory(Options, provider.Mapping);
        var whereParser = new WhereClauseParser(memberFactory);
        var fragment = whereParser.Parse(predicate.Body);
        var op = new DeleteWhereOperation(provider.Mapping, TenantId, fragment);
        _workTracker.Add(op);
    }

    public void UndoDeleteWhere<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var memberFactory = new MemberFactory(Options, provider.Mapping);
        var whereParser = new WhereClauseParser(memberFactory);
        var fragment = whereParser.Parse(predicate.Body);
        var op = new UndoDeleteWhereOperation(provider.Mapping, TenantId, fragment);
        _workTracker.Add(op);
    }

    private Dictionary<string, NestedTenantSession>? _byTenant;

    public ITenantOperations ForTenant(string tenantId)
    {
        _byTenant ??= new Dictionary<string, NestedTenantSession>();

        if (_byTenant.TryGetValue(tenantId, out var tenantSession))
        {
            return tenantSession;
        }

        tenantSession = new NestedTenantSession(this, tenantId);
        _byTenant[tenantId] = tenantSession;
        return tenantSession;
    }

    public void UpdateExpectedVersion<T>(T document, Guid version) where T : notnull
    {
        if (document is IVersioned versioned)
        {
            versioned.Version = version;
        }

        Store(document);
    }

    public void UpdateRevision<T>(T document, int revision) where T : notnull
    {
        if (document is IRevisioned revisioned)
        {
            revisioned.Version = revision;
        }
        else if (document is ILongVersioned longVersioned)
        {
            longVersioned.Version = revision;
        }

        Store(document);
    }

    public void UpdateRevision<T>(T document, long revision) where T : notnull
    {
        if (document is ILongVersioned longVersioned)
        {
            longVersioned.Version = revision;
        }

        Store(document);
    }

    public void QueueSqlCommand(string sql, params object[] parameterValues)
    {
        var operation = new Operations.ExecuteSqlStorageOperation(sql, parameterValues);
        _workTracker.Add(operation);
    }

    public async Task SaveChangesAsync(CancellationToken token = default)
    {
        // _workTracker tracks document operations and event streams - it does NOT include
        // ITransactionParticipants registered via AddTransactionParticipant. Skipping the entire
        // SaveChangesAsync pipeline when _workTracker is empty therefore silently drops queued
        // participants whose BeforeCommitAsync would otherwise run inside the same SQL transaction.
        //
        // This bit Wolverine.Polecat's scheduled-cascade outbox path (JasperFx/wolverine GH-2941):
        // a handler that emits only a DeliveryMessage<T>.DelayedFor(...) cascade has Wolverine
        // call Session.StoreIncoming(...), which registers a StoreIncomingEnvelopeParticipant
        // whose BeforeCommitAsync inserts the scheduled envelope row. With no doc ops on the
        // session, SaveChangesAsync early-returned and the row never got written.
        if (!_workTracker.HasOutstandingWork() && _transactionParticipants.Count == 0) return;

        using var activity = OpenTelemetry.TracingSessionDecorator.StartSessionActivity(
            "polecat.save_changes", TenantId, Options.OpenTelemetry);
        OpenTelemetry.TracingSessionDecorator.AddOperationEvents(
            activity, _workTracker.Operations, Options.OpenTelemetry);

        try
        {
        await SaveChangesInternalAsync(token);
        }
        catch (Exception ex)
        {
            OpenTelemetry.TracingSessionDecorator.RecordException(activity, ex);
            throw;
        }
    }

    private async Task SaveChangesInternalAsync(CancellationToken token)
    {
        // Call BeforeSaveChangesAsync on all listeners (global then session)
        foreach (var listener in Options.Listeners)
        {
            await listener.BeforeSaveChangesAsync(this, token);
        }

        foreach (var listener in _sessionListeners)
        {
            await listener.BeforeSaveChangesAsync(this, token);
        }

        // Ensure document tables exist for pending operations (skip non-document ops like FlatTable)
        if (_workTracker.Operations.Count > 0)
        {
            var typesNeeded = _workTracker.Operations
                .Select(op => op.DocumentType)
                .Where(t => t != typeof(object))
                .Distinct()
                .Select(t => _providers.GetProvider(t));

            await _tableEnsurer.EnsureTablesAsync(typesNeeded, token);
        }

        // #219: ensure the event store schema exists before appending — the event-sourcing analogue
        // of ensuring document tables above. Runs outside the data transaction (it opens its own
        // connection) and only when there are events to write.
        if (_workTracker.Streams.Any(s => s.Events.Any()))
        {
            await _tableEnsurer.EnsureEventStoreSchemaAsync(token);
        }

        await _transactional.BeginTransactionAsync(token);
        using var tx = _transactional.Transaction!;
        try
        {
            // Run DCB consistency checks BEFORE inserting new events,
            // so that newly appended events don't trigger false violations
            var dcbAssertions = _workTracker.Operations
                .OfType<AssertDcbConsistencyOperation>()
                .ToList();

            if (dcbAssertions.Count > 0)
            {
                await using var dcbBatch = new SqlBatch();
                var dcbBuilder = new BatchBuilder(dcbBatch);

                for (var i = 0; i < dcbAssertions.Count; i++)
                {
                    if (i > 0) dcbBuilder.StartNewCommand();
                    dcbAssertions[i].ConfigureCommand(dcbBuilder);
                }

                dcbBuilder.Compile();

                var dcbExceptions = new List<Exception>();
                await using var dcbReader = await ExecuteReaderAsync(dcbBatch, token);
                for (var i = 0; i < dcbAssertions.Count; i++)
                {
                    await dcbAssertions[i].PostprocessAsync(dcbReader, dcbExceptions, token);
                    if (i < dcbAssertions.Count - 1)
                    {
                        await dcbReader.NextResultAsync(token);
                    }
                }

                if (dcbExceptions.Count > 0)
                {
                    throw new AggregateException(dcbExceptions);
                }
            }

            // Process event streams. #273: the closed-shape path routes appends through the shared
            // Weasel.Storage.EventStorage<TId> hierarchy (SqlServerEventStoreDialect); the bespoke inline
            // SqlCommand path remains the default.
            if (Options.Events.UseClosedShapeEventStorage)
            {
                await ProcessStreamsClosedShapeAsync(token);
            }
            else
            {
                foreach (var stream in _workTracker.Streams)
                {
                    // Handle AlwaysEnforceConsistency with no events — just assert version
                    if (!stream.Events.Any() && stream.AlwaysEnforceConsistency && stream.ExpectedVersionOnServer.HasValue)
                    {
                        await AssertStreamVersionAsync(stream, token);
                        continue;
                    }

                    if (!stream.Events.Any()) continue;

                    if (stream.ActionType == StreamActionType.Start)
                    {
                        await ProcessStartStreamAsync(stream, token);
                    }
                    else
                    {
                        await ProcessAppendStreamAsync(stream, token);
                    }
                }
            }

            // Apply inline projections — projected document ops are queued into _workTracker
            if (_inlineProjections.Length > 0 && _workTracker.Streams.Count > 0)
            {
                // Pre-create document tables for projected types that projections may query
                var projectedDocTypes = Options.Projections.All
                    .Where(x => x.Lifecycle == ProjectionLifecycle.Inline)
                    .SelectMany(x => x.PublishedTypes())
                    .Distinct()
                    .Select(t => _providers.GetProvider(t));
                await _tableEnsurer.EnsureTablesAsync(projectedDocTypes, token);

                var streams = _workTracker.Streams.ToList();
                foreach (var projection in _inlineProjections)
                {
                    await projection.ApplyAsync(this, streams, token);
                }

                // Ensure document tables exist for any additional projected types
                if (_workTracker.Operations.Count > 0)
                {
                    var newTypes = _workTracker.Operations
                        .Select(op => op.DocumentType)
                        .Where(t => t != typeof(object))
                        .Distinct()
                        .Select(t => _providers.GetProvider(t));
                    await _tableEnsurer.EnsureTablesAsync(newTypes, token);
                }
            }

            // Process document operations using BatchBuilder/SqlBatch
            // (excluding DCB assertions which were already run above)
            var remainingOps = _workTracker.Operations
                .Where(op => op is not AssertDcbConsistencyOperation)
                .ToList();
            if (remainingOps.Count > 0)
            {
                await using var batch = new SqlBatch();
                var builder = new BatchBuilder(batch);

                var operations = remainingOps;
                for (var i = 0; i < operations.Count; i++)
                {
                    if (i > 0) builder.StartNewCommand();
                    Operations.StorageOperationExecution.Configure(
                        operations[i], builder, (Weasel.Storage.IStorageSession)this);
                }

                builder.Compile();

                try
                {
                    var exceptions = new List<Exception>();
                    await using var reader = await ExecuteReaderAsync(batch, token);
                    for (var i = 0; i < operations.Count; i++)
                    {
                        await operations[i].PostprocessAsync(reader, exceptions, token);
                        if (i < operations.Count - 1)
                        {
                            await reader.NextResultAsync(token);
                        }
                    }

                    // #273 E2b: drain the reader past every remaining result set. SQL Server
                    // surfaces a per-command batch error (unique index 2601, FK 547, ...) only
                    // when the reader ADVANCES into that command's result set — and the shared
                    // unversioned upsert is deliberately fire-and-forget, so without draining
                    // the error would sit unconsumed and be swallowed by DisposeAsync.
                    while (await reader.NextResultAsync(token))
                    {
                    }

                    if (exceptions.Count == 1)
                    {
                        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exceptions[0]).Throw();
                    }

                    if (exceptions.Count > 0)
                    {
                        throw new AggregateException(exceptions);
                    }
                }
                catch (SqlException ex) when (ex.Number == 2627)
                {
                    // Map duplicate key violation to DocumentAlreadyExistsException
                    var insertOp = operations.FirstOrDefault(op => op.Role() == OperationRole.Insert);
                    if (insertOp != null)
                    {
                        var documentId = insertOp is IStorageOperation bespoke ? bespoke.DocumentId : null;
                        throw new DocumentAlreadyExistsException(insertOp.DocumentType, documentId!);
                    }

                    throw;
                }
            }

            // Call transaction participants (e.g., EF Core DbContext) before commit
            foreach (var participant in _transactionParticipants)
            {
                await participant.BeforeCommitAsync(_transactional.Connection!, tx, token);
            }

            if (_inlineMessageBatch is not null)
            {
                await _inlineMessageBatch.BeforeCommitAsync(token);
            }

            await tx.CommitAsync(token);

            // Runtime append observation (polecat#213 / CritterWatch#500) — capture the committed
            // events before the work tracker is reset, then notify best-effort.
            NotifyAppendObserver();

            // #238: emit the opt-in polecat.event.append OpenTelemetry counter, also before reset.
            RecordEventAppendMetrics();

            // Snapshot the unit of work as an IChangeSet before the tracker is reset, so AfterCommit
            // listeners can see what was inserted/updated/deleted in this save.
            var commit = _workTracker.Clone();

            _workTracker.Reset();

            Logger.RecordSavedChanges(this);

            // Call AfterCommitAsync on all listeners (global then session)
            foreach (var listener in Options.Listeners)
            {
                await listener.AfterCommitAsync(this, commit, token);
            }

            foreach (var listener in _sessionListeners)
            {
                await listener.AfterCommitAsync(this, commit, token);
            }

            if (_inlineMessageBatch is not null)
            {
                await _inlineMessageBatch.AfterCommitAsync(token);
            }
        }
        finally
        {
            _transactional.Transaction = null;
        }
    }

    /// <summary>
    ///     Notify the storage-agnostic <see cref="IEventStoreInstrumentation.AppendObserver" /> with the
    ///     events appended in the just-committed unit of work (polecat#213 / CritterWatch#500). Must be
    ///     called after commit but before the work tracker is reset. Best-effort: the events are already
    ///     durable, so an observer fault is logged rather than surfaced as a SaveChanges failure.
    /// </summary>
    private void NotifyAppendObserver()
    {
        var observer = Options.Events.AppendObserver;
        if (observer == null) return;

        var events = _workTracker.Streams.SelectMany(s => s.Events).ToList();
        if (events.Count == 0) return;

        try
        {
            observer(events);
        }
        catch (Exception ex)
        {
            Logger.LogFailure("IEventStoreInstrumentation.AppendObserver threw", ex);
        }
    }

    /// <summary>
    ///     #238: increment the opt-in <c>polecat.event.append</c> counter once per event committed in
    ///     this unit of work, tagged with the event type and tenant. Mirrors Marten's event-append
    ///     counter. Must be called after commit but before the work tracker is reset. No-op unless
    ///     <see cref="Polecat.Internal.OpenTelemetry.OpenTelemetryOptions.EventCountersEnabled" /> is on.
    /// </summary>
    private void RecordEventAppendMetrics()
    {
        if (!Options.OpenTelemetry.EventCountersEnabled) return;

        var counter = Options.OpenTelemetry.EventAppendCounter;
        foreach (var stream in _workTracker.Streams)
        {
            foreach (var @event in stream.Events)
            {
                counter.Add(1,
                    new KeyValuePair<string, object?>("event_type", @event.EventTypeName),
                    new KeyValuePair<string, object?>("tenant_id", @event.TenantId ?? TenantId));
            }
        }
    }

    // ---- #273 closed-shape event append path (opt-in via Events.UseClosedShapeEventStorage) --------
    // Routes appends through the shared Weasel.Storage.EventStorage<TId> hierarchy
    // (SqlServerEventStoreDialect + PolecatQuickAppendEventsOperation) instead of the bespoke
    // ProcessStart/ProcessAppend/AssertStreamVersion/InsertEvent methods below. Versions are assigned
    // client-side after a locking read so inline projections see them; sequences read back in Postprocess.

    private async Task ProcessStreamsClosedShapeAsync(CancellationToken token)
    {
        var storage = _eventGraph.ClosedShapeEventStorage;
        var isGuid = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid;

        var ops = new List<Weasel.Storage.IStorageOperation>();

        foreach (var stream in _workTracker.Streams)
        {
            if (!stream.Events.Any())
            {
                if (stream.AlwaysEnforceConsistency && stream.ExpectedVersionOnServer.HasValue)
                {
                    // Reuse the bespoke assert: unlike the shared AssertStreamVersionOperation (which
                    // throws on any missing stream), Polecat treats a not-found stream with expected
                    // version 0 as consistent (0 == 0), matching FetchForWriting on a new stream.
                    await AssertStreamVersionAsync(stream, token);
                }

                continue;
            }

            if (stream.ActionType == StreamActionType.Start)
            {
                AssignEventMetadataForClosedShape(stream, 0);
                stream.Version = stream.Events.Count;
                ops.Add(QuickAppendEventsClosedShape(storage, isGuid, stream, Polecat.Events.Storage.StreamWriteMode.Insert));
            }
            else
            {
                var (currentVersion, exists, archived) = await ReadStreamStateForClosedShapeAsync(stream, token);

                var streamId = isGuid ? (object)stream.Id : stream.Key!;
                if (archived)
                {
                    throw new Exceptions.InvalidStreamException(streamId, "Cannot append to an archived stream.");
                }

                if (stream.ExpectedVersionOnServer.HasValue && currentVersion != stream.ExpectedVersionOnServer.Value)
                {
                    throw new EventStreamUnexpectedMaxEventIdException(streamId, stream.AggregateType,
                        stream.ExpectedVersionOnServer.Value, currentVersion);
                }

                AssignEventMetadataForClosedShape(stream, currentVersion);
                stream.Version = currentVersion + stream.Events.Count;

                if (exists)
                {
                    stream.ExpectedVersionOnServer ??= currentVersion;
                    ops.Add(QuickAppendEventsClosedShape(storage, isGuid, stream, Polecat.Events.Storage.StreamWriteMode.Update));
                }
                else
                {
                    ops.Add(QuickAppendEventsClosedShape(storage, isGuid, stream, Polecat.Events.Storage.StreamWriteMode.Insert));
                }
            }
        }

        if (ops.Count == 0) return;

        await ExecuteClosedShapeEventOperationsAsync(ops, token);
    }

    private static Weasel.Storage.IStorageOperation QuickAppendEventsClosedShape(
        object storage, bool isGuid, StreamAction stream, Polecat.Events.Storage.StreamWriteMode mode)
    {
        var op = (Polecat.Events.Storage.PolecatQuickAppendEventsOperation)(isGuid
            ? ((Weasel.Storage.EventStorage<Guid>)storage).QuickAppendEvents(stream)
            : ((Weasel.Storage.EventStorage<string>)storage).QuickAppendEvents(stream));
        op.Mode = mode;
        return op;
    }

    private void AssignEventMetadataForClosedShape(StreamAction stream, long baseVersion)
    {
        var events = stream.Events;
        for (var i = 0; i < events.Count; i++)
        {
            var e = events[i];
            e.Version = baseVersion + i + 1;
            if (e.Id == Guid.Empty) e.Id = Guid.NewGuid();
            e.Timestamp = DateTimeOffset.UtcNow;
            e.TenantId = stream.TenantId;
            e.StreamId = stream.Id;
            e.StreamKey = stream.Key;

            if (CorrelationId != null && e.CorrelationId == null) e.CorrelationId = CorrelationId;
            if (CausationId != null && e.CausationId == null) e.CausationId = CausationId;
            if (LastModifiedBy != null && e.UserName == null) e.UserName = LastModifiedBy;

            if (Headers is { Count: > 0 })
            {
                e.Headers ??= new Dictionary<string, object>();
                foreach (var header in Headers)
                {
                    e.Headers.TryAdd(header.Key, header.Value);
                }
            }
        }
    }

    private async Task<(long currentVersion, bool exists, bool archived)> ReadStreamStateForClosedShapeAsync(
        StreamAction stream, CancellationToken token)
    {
        var streamId = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? (object)stream.Id
            : stream.Key!;

        await using var cmd = new SqlCommand();
        cmd.CommandText =
            $"SELECT version, is_archived FROM {_eventGraph.StreamsTableName} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id AND tenant_id = @tenant_id;";
        cmd.Parameters.AddWithValue("@id", streamId);
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        await using var reader = await ExecuteReaderAsync(cmd, token);
        if (await reader.ReadAsync(token))
        {
            return (reader.GetInt64(0), true, reader.GetBoolean(1));
        }

        return (0, false, false);
    }

    private async Task ExecuteClosedShapeEventOperationsAsync(
        List<Weasel.Storage.IStorageOperation> operations, CancellationToken token)
    {
        await using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);

        for (var i = 0; i < operations.Count; i++)
        {
            if (i > 0) builder.StartNewCommand();
            Operations.StorageOperationExecution.Configure(operations[i], builder, (Weasel.Storage.IStorageSession)this);
        }

        builder.Compile();

        var exceptions = new List<Exception>();
        try
        {
            await using var reader = await ExecuteReaderAsync(batch, token);
            for (var i = 0; i < operations.Count; i++)
            {
                await operations[i].PostprocessAsync(reader, exceptions, token);
                if (i < operations.Count - 1)
                {
                    await reader.NextResultAsync(token);
                }
            }

            while (await reader.NextResultAsync(token))
            {
            }
        }
        catch (SqlException ex)
        {
            foreach (var op in operations)
            {
                if (op is JasperFx.Core.Exceptions.IExceptionTransform transform
                    && transform.TryTransform(ex, out var transformed) && transformed != null)
                {
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(transformed).Throw();
                }
            }

            throw;
        }

        if (exceptions.Count == 1)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exceptions[0]).Throw();
        }

        if (exceptions.Count > 0)
        {
            throw new AggregateException(exceptions);
        }
    }

    private async Task ProcessStartStreamAsync(StreamAction stream, CancellationToken token)
    {
        var events = stream.Events;

        // Assign versions 1, 2, 3...
        for (var i = 0; i < events.Count; i++)
        {
            events[i].Version = i + 1;
            if (events[i].Id == Guid.Empty) events[i].Id = Guid.NewGuid();
            events[i].Timestamp = DateTimeOffset.UtcNow;
            events[i].TenantId = stream.TenantId;
            events[i].StreamId = stream.Id;
            events[i].StreamKey = stream.Key;
        }

        stream.Version = events.Count;

        // INSERT stream row
        {
            await using var cmd = new SqlCommand();
            var streamId = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
                ? (object)stream.Id
                : stream.Key!;

            cmd.CommandText = $"""
                INSERT INTO {_eventGraph.StreamsTableName}
                    (id, type, version, timestamp, created, tenant_id)
                VALUES (@id, @type, @version, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @tenant_id);
                """;
            cmd.Parameters.AddWithValue("@id", streamId);
            cmd.Parameters.AddWithValue("@type",
                (object?)stream.AggregateType?.Name ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@version", stream.Version);
            cmd.Parameters.AddWithValue("@tenant_id", stream.TenantId);

            try
            {
                await ExecuteAsync(cmd, token);
            }
            catch (SqlException ex) when (ex.Number == 2627)
            {
                var id = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
                    ? (object)stream.Id
                    : stream.Key!;
                throw new ExistingStreamIdCollisionException(id);
            }
        }

        // INSERT each event
        foreach (var @event in events)
        {
            await InsertEventAsync(@event, stream, token);
        }
    }

    private async Task AssertStreamVersionAsync(StreamAction stream, CancellationToken token)
    {
        var streamId = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? (object)stream.Id
            : stream.Key!;

        await using var cmd = new SqlCommand();
        cmd.CommandText =
            $"SELECT version FROM {_eventGraph.StreamsTableName} WHERE id = @id AND tenant_id = @tenant_id;";
        cmd.Parameters.AddWithValue("@id", streamId);
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        await using var reader = await ExecuteReaderAsync(cmd, token);
        if (!await reader.ReadAsync(token))
        {
            // Stream doesn't exist — consistent only if expected version is 0
            if (stream.ExpectedVersionOnServer!.Value != 0)
            {
                throw new EventStreamUnexpectedMaxEventIdException(streamId, stream.AggregateType,
                    stream.ExpectedVersionOnServer.Value, 0);
            }
            return;
        }

        var actualVersion = reader.GetInt64(0);
        if (actualVersion != stream.ExpectedVersionOnServer!.Value)
        {
            throw new EventStreamUnexpectedMaxEventIdException(streamId, stream.AggregateType,
                stream.ExpectedVersionOnServer.Value, actualVersion);
        }
    }

    private async Task ProcessAppendStreamAsync(StreamAction stream, CancellationToken token)
    {
        var streamId = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? (object)stream.Id
            : stream.Key!;

        // Step 1: Get current version and archived status with lock
        long currentVersion = 0;
        bool streamExists = false;
        bool isArchived = false;

        {
            await using var cmd = new SqlCommand();
            cmd.CommandText =
                $"SELECT version, is_archived FROM {_eventGraph.StreamsTableName} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id AND tenant_id = @tenant_id;";
            cmd.Parameters.AddWithValue("@id", streamId);
            cmd.Parameters.AddWithValue("@tenant_id", TenantId);
            await using var reader = await ExecuteReaderAsync(cmd, token);
            if (await reader.ReadAsync(token))
            {
                currentVersion = reader.GetInt64(0);
                isArchived = reader.GetBoolean(1);
                streamExists = true;
            }
        }

        // Reject appends to archived streams
        if (isArchived)
        {
            throw new Exceptions.InvalidStreamException(streamId, "Cannot append to an archived stream.");
        }

        // Check expected version if set
        if (stream.ExpectedVersionOnServer.HasValue && currentVersion != stream.ExpectedVersionOnServer.Value)
        {
            throw new EventStreamUnexpectedMaxEventIdException(streamId, stream.AggregateType,
                stream.ExpectedVersionOnServer.Value, currentVersion);
        }

        // Step 2: Assign versions
        var events = stream.Events;
        for (var i = 0; i < events.Count; i++)
        {
            events[i].Version = currentVersion + i + 1;
            if (events[i].Id == Guid.Empty) events[i].Id = Guid.NewGuid();
            events[i].Timestamp = DateTimeOffset.UtcNow;
            events[i].TenantId = stream.TenantId;
            events[i].StreamId = stream.Id;
            events[i].StreamKey = stream.Key;
        }

        var newVersion = currentVersion + events.Count;
        stream.Version = newVersion;

        // Step 3: Upsert stream row
        if (streamExists)
        {
            await using var cmd = new SqlCommand();
            cmd.CommandText =
                $"UPDATE {_eventGraph.StreamsTableName} SET version = @version, timestamp = SYSDATETIMEOFFSET() WHERE id = @id AND tenant_id = @tenant_id;";
            cmd.Parameters.AddWithValue("@id", streamId);
            cmd.Parameters.AddWithValue("@version", newVersion);
            cmd.Parameters.AddWithValue("@tenant_id", TenantId);
            await ExecuteAsync(cmd, token);
        }
        else
        {
            await using var cmd = new SqlCommand();
            cmd.CommandText = $"""
                INSERT INTO {_eventGraph.StreamsTableName}
                    (id, type, version, timestamp, created, tenant_id)
                VALUES (@id, @type, @version, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @tenant_id);
                """;
            cmd.Parameters.AddWithValue("@id", streamId);
            cmd.Parameters.AddWithValue("@type",
                (object?)stream.AggregateType?.Name ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@version", newVersion);
            cmd.Parameters.AddWithValue("@tenant_id", stream.TenantId);
            await ExecuteAsync(cmd, token);
        }

        // Step 4: Insert events
        foreach (var @event in events)
        {
            await InsertEventAsync(@event, stream, token);
        }
    }

    // IL2026 / IL3050 here originate from two Serializer.ToJson(object) call
    // sites below (@event.Data ~line 796, @event.Headers ~line 816). Promoting
    // [RequiresUnreferencedCode] / [RequiresDynamicCode] up the call chain would
    // propagate to IDocumentSession.SaveChangesAsync — out of scope for #46's
    // reflective-callsite tightening (which targets the four projection/LINQ
    // classes). The AOT escape hatch remains: AOT consumers supply a
    // source-generator-backed ISerializer implementation; the default reflective
    // STJ serializer is the only thing that trips RUC/RDC here.
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Serializer.ToJson(@event.Data) + Serializer.ToJson(@event.Headers). Default STJ ISerializer reflects over event types; AOT consumers supply a source-generator-backed ISerializer.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Serializer.ToJson uses STJ runtime codegen by default; AOT consumers supply a source-generator-backed ISerializer.")]
    private async Task InsertEventAsync(IEvent @event, StreamAction stream, CancellationToken token)
    {
        var streamId = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? (object)stream.Id
            : stream.Key!;

        // Propagate session-level metadata to the event
        if (CorrelationId != null && @event.CorrelationId == null)
            @event.CorrelationId = CorrelationId;
        if (CausationId != null && @event.CausationId == null)
            @event.CausationId = CausationId;
        if (LastModifiedBy != null && @event.UserName == null)
            @event.UserName = LastModifiedBy;

        // #240: merge session-level headers into the event. Event-level keys win, so TryAdd only
        // fills keys the event didn't already set.
        if (Headers is { Count: > 0 })
        {
            @event.Headers ??= new Dictionary<string, object>();
            foreach (var header in Headers)
            {
                @event.Headers.TryAdd(header.Key, header.Value);
            }
        }

        var eventOptions = _eventGraph.EventOptions;

        // Build column and value lists dynamically based on enabled metadata
        var columns = "id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type";
        var values = "@id, @stream_id, @version, @data, @type, SYSDATETIMEOFFSET(), @tenant_id, @dotnet_type";

        // Per-tenant partitioning (#163 / polecat#171): seq_id is no longer a global IDENTITY — it is
        // drawn from this tenant's own pc_events_sequence_{ordinal}, and the row carries the tenant's
        // physical-partition ordinal. Both are provisioned on first use (partition SPLIT + sequence
        // create). Off-flag this whole block is skipped and seq_id keeps its IDENTITY default, leaving
        // the append path byte-for-byte.
        int? tenantOrdinal = null;
        if (_eventGraph.UseTenantPartitionedEvents)
        {
            var eventTenantId = @event.TenantId ?? TenantId;
            var storage = await _eventGraph.TenantSequences.ResolveAsync(eventTenantId, token);
            tenantOrdinal = storage.Ordinal;
            columns = $"seq_id, {_eventGraph.TenantPartitionManager.Column}, " + columns;
            values = $"NEXT VALUE FOR {storage.SequenceName}, @tenant_ordinal, " + values;
        }

        if (eventOptions.EnableCorrelationId)
        {
            columns += ", correlation_id";
            values += ", @correlation_id";
        }

        if (eventOptions.EnableCausationId)
        {
            columns += ", causation_id";
            values += ", @causation_id";
        }

        if (eventOptions.EnableHeaders)
        {
            columns += ", headers";
            values += ", @headers";
        }

        if (eventOptions.EnableUserName)
        {
            columns += ", user_name";
            values += ", @user_name";
        }

        await using var cmd = new SqlCommand();

        var tags = @event.Tags;
        var hasTags = tags != null && tags.Count > 0 && _eventGraph.TagTypes.Count > 0;

        if (hasTags)
        {
            // Batch event insert + tag inserts into a single command to minimize round-trips
            var schema = _eventGraph.DatabaseSchemaName;
            var sb = new System.Text.StringBuilder();
            sb.AppendLine("DECLARE @new_seq TABLE (seq_id bigint);");
            sb.AppendLine($"INSERT INTO {_eventGraph.EventsTableName}");
            sb.AppendLine($"    ({columns})");
            sb.AppendLine("OUTPUT inserted.seq_id INTO @new_seq");
            sb.AppendLine($"VALUES ({values});");
            sb.AppendLine("DECLARE @seq_id bigint = (SELECT TOP 1 seq_id FROM @new_seq);");

            var tagIndex = 0;
            // When pc_events is partitioned by is_archived the tag table also carries
            // is_archived (PK + FK columns) — see EventTagTable. Newly-appended events
            // always start as is_archived = 0.
            var partitioned = _eventGraph.UseArchivedStreamPartitioning;
            foreach (var tag in tags!)
            {
                var registration = _eventGraph.FindTagType(tag.TagType);
                if (registration == null) continue;

                var valueParam = $"@tag_value_{tagIndex}";
                var tagTable = $"[{schema}].[pc_event_tag_{registration.TableSuffix}]";
                if (_eventGraph.TenancyStyle == TenancyStyle.Conjoined && partitioned)
                {
                    sb.AppendLine($"IF NOT EXISTS (SELECT 1 FROM {tagTable} WHERE value = {valueParam} AND tenant_id = @tenant_id AND seq_id = @seq_id AND is_archived = 0)");
                    sb.AppendLine($"INSERT INTO {tagTable} (value, tenant_id, seq_id, is_archived) VALUES ({valueParam}, @tenant_id, @seq_id, 0);");
                }
                else if (_eventGraph.TenancyStyle == TenancyStyle.Conjoined)
                {
                    sb.AppendLine($"IF NOT EXISTS (SELECT 1 FROM {tagTable} WHERE value = {valueParam} AND tenant_id = @tenant_id AND seq_id = @seq_id)");
                    sb.AppendLine($"INSERT INTO {tagTable} (value, tenant_id, seq_id) VALUES ({valueParam}, @tenant_id, @seq_id);");
                }
                else if (partitioned)
                {
                    sb.AppendLine($"IF NOT EXISTS (SELECT 1 FROM {tagTable} WHERE value = {valueParam} AND seq_id = @seq_id AND is_archived = 0)");
                    sb.AppendLine($"INSERT INTO {tagTable} (value, seq_id, is_archived) VALUES ({valueParam}, @seq_id, 0);");
                }
                else
                {
                    sb.AppendLine($"IF NOT EXISTS (SELECT 1 FROM {tagTable} WHERE value = {valueParam} AND seq_id = @seq_id)");
                    sb.AppendLine($"INSERT INTO {tagTable} (value, seq_id) VALUES ({valueParam}, @seq_id);");
                }
                cmd.Parameters.AddWithValue(valueParam, registration.ExtractValue(tag.Value));
                tagIndex++;
            }

            sb.AppendLine("SELECT @seq_id;");
            cmd.CommandText = sb.ToString();
        }
        else
        {
            cmd.CommandText = $"""
                INSERT INTO {_eventGraph.EventsTableName}
                    ({columns})
                OUTPUT inserted.seq_id
                VALUES ({values});
                """;
        }

        cmd.Parameters.AddWithValue("@id", @event.Id);
        cmd.Parameters.AddWithValue("@stream_id", streamId);
        cmd.Parameters.AddWithValue("@version", @event.Version);
        if (tenantOrdinal.HasValue) cmd.Parameters.AddWithValue("@tenant_ordinal", tenantOrdinal.Value);
        cmd.Parameters.AddWithValue("@data", Serializer.ToJson(@event.Data));
        cmd.Parameters.AddWithValue("@type", @event.EventTypeName);
        cmd.Parameters.AddWithValue("@tenant_id", @event.TenantId ?? TenantId);
        cmd.Parameters.AddWithValue("@dotnet_type", @event.DotNetTypeName);

        if (eventOptions.EnableCorrelationId)
        {
            cmd.Parameters.AddWithValue("@correlation_id",
                (object?)@event.CorrelationId ?? DBNull.Value);
        }

        if (eventOptions.EnableCausationId)
        {
            cmd.Parameters.AddWithValue("@causation_id",
                (object?)@event.CausationId ?? DBNull.Value);
        }

        if (eventOptions.EnableHeaders)
        {
            var headersJson = @event.Headers != null && @event.Headers.Count > 0
                ? Serializer.ToJson(@event.Headers)
                : null;
            cmd.Parameters.AddWithValue("@headers",
                (object?)headersJson ?? DBNull.Value);
        }

        if (eventOptions.EnableUserName)
        {
            cmd.Parameters.AddWithValue("@user_name",
                (object?)@event.UserName ?? DBNull.Value);
        }

        var seqId = (long)(await ExecuteScalarAsync(cmd, token))!;
        @event.Sequence = seqId;
    }

    // IStorageOperations
    public bool EnableSideEffectsOnInlineProjections => _eventGraph.EnableSideEffectsOnInlineProjections;

    private IMessageBatch? _inlineMessageBatch;

    public async ValueTask<IMessageSink> GetOrStartMessageSink()
    {
        if (_inlineMessageBatch is not null) return _inlineMessageBatch;

        _inlineMessageBatch = await Options.Events.MessageOutbox
            .CreateBatch(this)
            .ConfigureAwait(false);

        return _inlineMessageBatch;
    }

    Task<IProjectionStorage<TDoc, TId>> IStorageOperations.FetchProjectionStorageAsync<TDoc, TId>(
        string tenantId, CancellationToken cancellationToken)
    {
        // Check for custom projection storage providers (e.g., EF Core)
        if (Options.CustomProjectionStorageProviders.TryGetValue(typeof(TDoc), out var factory))
        {
            return Task.FromResult((IProjectionStorage<TDoc, TId>)factory(this, tenantId));
        }

        var provider = _providers.GetProvider(typeof(TDoc));
        IProjectionStorage<TDoc, TId> storage =
#pragma warning disable CS8714 // notnull constraint mismatch with JasperFx interface
            new PolecatProjectionStorage<TDoc, TId>(this, provider, tenantId);
#pragma warning restore CS8714
        return Task.FromResult(storage);
    }


    public void Eject<T>(T document) where T : notnull
    {
        var provider = _providers.GetProvider<T>();
        var id = provider.Mapping.GetId(document);
        _workTracker.EjectDocument(typeof(T), id);
        OnDocumentEjected(typeof(T), id);
        // #273 E2b: the closed-shape load path caches in the shared ItemMap / version tracker.
        if (provider.Mapping.DocumentType == typeof(T))
        {
            var session = (Weasel.Storage.IStorageSession)this;
            session.StorageFor<T>().EjectById(session, id);
        }
    }

    public void EjectAllOfType(Type type)
    {
        _workTracker.EjectAllOfType(type);
        OnAllOfTypeEjected(type);
        // #273 E2b: drop the closed-shape shared-ItemMap cache for the type as well.
        ((Weasel.Storage.IStorageSession)this).ItemMap.Remove(type);
    }

    public void EjectAllPendingChanges()
    {
        _workTracker.Reset();
    }

    /// <summary>
    ///     Called when a document is queued via Store/Insert/Update.
    ///     Override in IdentityMap session to track in the map.
    /// </summary>
    protected virtual void OnDocumentStored(Type documentType, object id, object document)
    {
        // No-op in lightweight session
    }

    /// <summary>
    ///     Called when a document is ejected. Override in IdentityMap session
    ///     to remove from the identity map.
    /// </summary>
    protected virtual void OnDocumentEjected(Type documentType, object id)
    {
        // No-op in lightweight session
    }

    /// <summary>
    ///     Called when all documents of a type are ejected. Override in IdentityMap session
    ///     to clear the identity map for that type.
    /// </summary>
    protected virtual void OnAllOfTypeEjected(Type documentType)
    {
        // No-op in lightweight session
    }

    // #273 phase E1: document sessions resolve the Lightweight closed-shape storage flavor.
    internal override Weasel.Storage.IDocumentStorage<T> SelectClosedShapeStorage<T>(
        Weasel.Storage.DocumentProvider<T> provider)
        => provider.Lightweight;

    private void SyncMetadata(object document)
    {
        if (document is ITracked tracked)
        {
            tracked.CorrelationId = CorrelationId;
            tracked.CausationId = CausationId;
            tracked.LastModifiedBy = LastModifiedBy;
        }

        if (document is Metadata.ITenanted tenanted)
        {
            tenanted.TenantId = TenantId;
        }
    }
}
