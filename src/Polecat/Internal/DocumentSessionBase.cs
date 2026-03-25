using System.Linq.Expressions;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events;
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

    IQueryEventStore IQuerySession.Events => EventOps;
    public new IEventOperations Events => EventOps;

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
        var op = provider.BuildUpsert(document, Serializer, TenantId);
        _workTracker.Add(op);
        OnDocumentStored(typeof(T), provider.Mapping.GetId(document), document);
    }

    public void Store<T>(params T[] documents) where T : notnull
    {
        foreach (var doc in documents)
        {
            Store(doc);
        }
    }

    public void Insert<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildInsert(document, Serializer, TenantId);
        _workTracker.Add(op);
        OnDocumentStored(typeof(T), provider.Mapping.GetId(document), document);
    }

    public void Update<T>(T document) where T : notnull
    {
        SyncMetadata(document);
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildUpdate(document, Serializer, TenantId);
        _workTracker.Add(op);
        OnDocumentStored(typeof(T), provider.Mapping.GetId(document), document);
    }

    public void Delete<T>(T document) where T : notnull
    {
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildDeleteByDocument(document, TenantId);
        _workTracker.Add(op);

        // Sync ISoftDeleted properties in memory
        if (document is ISoftDeleted softDeleted && provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            softDeleted.Deleted = true;
            softDeleted.DeletedAt = DateTimeOffset.UtcNow;
        }
    }

    public void Delete<T>(Guid id) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildDeleteById(id, TenantId);
        _workTracker.Add(op);
    }

    public void Delete<T>(string id) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildDeleteById(id, TenantId);
        _workTracker.Add(op);
    }

    public void Delete<T>(int id) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildDeleteById(id, TenantId);
        _workTracker.Add(op);
    }

    public void Delete<T>(long id) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildDeleteById(id, TenantId);
        _workTracker.Add(op);
    }

    public void HardDelete<T>(T document) where T : notnull
    {
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildHardDeleteByDocument(document, TenantId);
        _workTracker.Add(op);
    }

    public void HardDelete<T>(Guid id) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildHardDeleteById(id, TenantId);
        _workTracker.Add(op);
    }

    public void HardDelete<T>(string id) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildHardDeleteById(id, TenantId);
        _workTracker.Add(op);
    }

    public void HardDelete<T>(int id) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildHardDeleteById(id, TenantId);
        _workTracker.Add(op);
    }

    public void HardDelete<T>(long id) where T : class
    {
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildHardDeleteById(id, TenantId);
        _workTracker.Add(op);
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

        Store(document);
    }

    public void QueueSqlCommand(string sql, params object[] parameterValues)
    {
        var operation = new Operations.ExecuteSqlStorageOperation(sql, parameterValues);
        _workTracker.Add(operation);
    }

    public async Task SaveChangesAsync(CancellationToken token = default)
    {
        if (!_workTracker.HasOutstandingWork()) return;

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

        bool createdTx = _transactional.Transaction == null;
        if (createdTx)
        {
            await _transactional.BeginTransactionAsync(token);
        }

        var tx = _transactional.Transaction!;

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

            // Process event streams
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
                    operations[i].ConfigureCommand(builder);
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
                        throw new DocumentAlreadyExistsException(insertOp.DocumentType, insertOp.DocumentId!);
                    }

                    throw;
                }
            }

            // Call transaction participants (e.g., EF Core DbContext) before commit
            foreach (var participant in _transactionParticipants)
            {
                await participant.BeforeCommitAsync(_transactional.Connection!, tx, token);
            }

            await tx.CommitAsync(token);
            _workTracker.Reset();

            Logger.RecordSavedChanges(this);

            // Call AfterCommitAsync on all listeners (global then session)
            foreach (var listener in Options.Listeners)
            {
                await listener.AfterCommitAsync(this, token);
            }

            foreach (var listener in _sessionListeners)
            {
                await listener.AfterCommitAsync(this, token);
            }
        }
        catch
        {
            await tx.RollbackAsync(token);
            throw;
        }
        finally
        {
            if (createdTx)
            {
                await tx.DisposeAsync();
                _transactional.Transaction = null;
            }
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

        var eventOptions = _eventGraph.EventOptions;

        // Build column and value lists dynamically based on enabled metadata
        var columns = "id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type";
        var values = "@id, @stream_id, @version, @data, @type, SYSDATETIMEOFFSET(), @tenant_id, @dotnet_type";

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
            foreach (var tag in tags!)
            {
                var registration = _eventGraph.FindTagType(tag.TagType);
                if (registration == null) continue;

                var valueParam = $"@tag_value_{tagIndex}";
                sb.AppendLine($"IF NOT EXISTS (SELECT 1 FROM [{schema}].[pc_event_tag_{registration.TableSuffix}] WHERE value = {valueParam} AND seq_id = @seq_id)");
                sb.AppendLine($"INSERT INTO [{schema}].[pc_event_tag_{registration.TableSuffix}] (value, seq_id) VALUES ({valueParam}, @seq_id);");
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

        var seqId = (long)(await ExecuteScalarAsync(cmd, token))!;
        @event.Sequence = seqId;
    }

    // IStorageOperations
    public bool EnableSideEffectsOnInlineProjections => false;

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

    public ValueTask<IMessageSink> GetOrStartMessageSink()
    {
        throw new NotSupportedException("Message sinks are not supported in Polecat.");
    }

    public void Eject<T>(T document) where T : notnull
    {
        var provider = _providers.GetProvider<T>();
        var id = provider.Mapping.GetId(document);
        _workTracker.EjectDocument(typeof(T), id);
        OnDocumentEjected(typeof(T), id);
    }

    public void EjectAllOfType(Type type)
    {
        _workTracker.EjectAllOfType(type);
        OnAllOfTypeEjected(type);
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

    private void SyncMetadata(object document)
    {
        if (document is Metadata.ITracked tracked)
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
