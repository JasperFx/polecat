using System.Linq.Expressions;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Exceptions;
using Polecat.Internal.Operations;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Metadata;
using Polecat.Projections;

namespace Polecat.Internal;

/// <summary>
///     Base class for document sessions. Handles operation queueing, event stream
///     processing, and SaveChangesAsync.
/// </summary>
internal abstract class DocumentSessionBase : QuerySession, IDocumentSession
{
    private readonly WorkTracker _workTracker = new();
    private readonly IInlineProjection<IDocumentSession>[] _inlineProjections;
    private EventOperations? _eventOperations;

    /// <summary>
    ///     The active transaction during SaveChangesAsync. Set so that projection storage
    ///     commands can be enlisted in the same transaction.
    /// </summary>
    internal override SqlTransaction? ActiveTransaction { get; set; }

    protected DocumentSessionBase(
        StoreOptions options,
        ConnectionFactory connectionFactory,
        DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer,
        EventGraph eventGraph,
        IInlineProjection<IDocumentSession>[] inlineProjections,
        string tenantId)
        : base(options, connectionFactory, providers, tableEnsurer, eventGraph, tenantId)
    {
        _inlineProjections = inlineProjections;
    }

    public IWorkTracker PendingChanges => _workTracker;

    IQueryEventStore IQuerySession.Events => EventOps;
    public new IEventOperations Events => EventOps;

    private EventOperations EventOps =>
        _eventOperations ??= new EventOperations(this, _eventGraph, Options, _workTracker, TenantId);

    internal WorkTracker WorkTracker => _workTracker;

    internal EventGraph EventGraph => _eventGraph;

    internal async Task BeginTransactionAsync(CancellationToken token)
    {
        if (ActiveTransaction != null) return;
        var conn = await GetConnectionAsync(token);
        ActiveTransaction = (SqlTransaction)await conn.BeginTransactionAsync(token);
    }

    internal async Task EnsureTableForProviderAsync(DocumentProvider provider, CancellationToken token)
    {
        await _tableEnsurer.EnsureTablesAsync([provider], token);
    }

    public void Store<T>(T document) where T : notnull
    {
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
        var provider = _providers.GetProvider<T>();
        var op = provider.BuildInsert(document, Serializer, TenantId);
        _workTracker.Add(op);
        OnDocumentStored(typeof(T), provider.Mapping.GetId(document), document);
    }

    public void Update<T>(T document) where T : notnull
    {
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

    public async Task SaveChangesAsync(CancellationToken token = default)
    {
        if (!_workTracker.HasOutstandingWork()) return;

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

        var conn = await GetConnectionAsync(token);
        var existingTx = ActiveTransaction;
        SqlTransaction tx;
        bool createdTx;

        if (existingTx != null)
        {
            tx = existingTx;
            createdTx = false;
        }
        else
        {
            tx = (SqlTransaction)await conn.BeginTransactionAsync(token);
            createdTx = true;
        }

        try
        {
            ActiveTransaction = tx;

            // Process event streams first
            foreach (var stream in _workTracker.Streams)
            {
                if (stream.ActionType == StreamActionType.Start)
                {
                    await ProcessStartStreamAsync(stream, conn, tx, token);
                }
                else
                {
                    await ProcessAppendStreamAsync(stream, conn, tx, token);
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

            // Process document operations
            foreach (var operation in _workTracker.Operations)
            {
                await using var cmd = conn.CreateCommand();
                cmd.Transaction = tx;
                operation.ConfigureCommand(cmd);
                await operation.PostprocessAsync(cmd, token);
            }

            await tx.CommitAsync(token);
            _workTracker.Reset();
        }
        catch
        {
            await tx.RollbackAsync(token);
            throw;
        }
        finally
        {
            ActiveTransaction = null;
            if (createdTx) await tx.DisposeAsync();
        }
    }

    private async Task ProcessStartStreamAsync(StreamAction stream, SqlConnection conn,
        SqlTransaction tx, CancellationToken token)
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
        await using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = tx;
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
                await cmd.ExecuteNonQueryAsync(token);
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
            await InsertEventAsync(@event, stream, conn, tx, token);
        }
    }

    private async Task ProcessAppendStreamAsync(StreamAction stream, SqlConnection conn,
        SqlTransaction tx, CancellationToken token)
    {
        var streamId = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? (object)stream.Id
            : stream.Key!;

        // Step 1: Get current version with lock
        long currentVersion = 0;
        bool streamExists = false;

        await using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText =
                $"SELECT version FROM {_eventGraph.StreamsTableName} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id AND tenant_id = @tenant_id;";
            cmd.Parameters.AddWithValue("@id", streamId);
            cmd.Parameters.AddWithValue("@tenant_id", TenantId);
            var result = await cmd.ExecuteScalarAsync(token);
            if (result != null && result != DBNull.Value)
            {
                currentVersion = (long)result;
                streamExists = true;
            }
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
            await using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText =
                $"UPDATE {_eventGraph.StreamsTableName} SET version = @version, timestamp = SYSDATETIMEOFFSET() WHERE id = @id AND tenant_id = @tenant_id;";
            cmd.Parameters.AddWithValue("@id", streamId);
            cmd.Parameters.AddWithValue("@version", newVersion);
            cmd.Parameters.AddWithValue("@tenant_id", TenantId);
            await cmd.ExecuteNonQueryAsync(token);
        }
        else
        {
            await using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
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
            await cmd.ExecuteNonQueryAsync(token);
        }

        // Step 4: Insert events
        foreach (var @event in events)
        {
            await InsertEventAsync(@event, stream, conn, tx, token);
        }
    }

    private async Task InsertEventAsync(IEvent @event, StreamAction stream,
        SqlConnection conn, SqlTransaction tx, CancellationToken token)
    {
        var streamId = _eventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? (object)stream.Id
            : stream.Key!;

        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = $"""
            INSERT INTO {_eventGraph.EventsTableName}
                (id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type)
            OUTPUT inserted.seq_id
            VALUES (@id, @stream_id, @version, @data, @type, SYSDATETIMEOFFSET(), @tenant_id, @dotnet_type);
            """;
        cmd.Parameters.AddWithValue("@id", @event.Id);
        cmd.Parameters.AddWithValue("@stream_id", streamId);
        cmd.Parameters.AddWithValue("@version", @event.Version);
        cmd.Parameters.AddWithValue("@data", Serializer.ToJson(@event.Data));
        cmd.Parameters.AddWithValue("@type", @event.EventTypeName);
        cmd.Parameters.AddWithValue("@tenant_id", @event.TenantId ?? TenantId);
        cmd.Parameters.AddWithValue("@dotnet_type", @event.DotNetTypeName);

        var seqId = (long)(await cmd.ExecuteScalarAsync(token))!;
        @event.Sequence = seqId;
    }

    // IStorageOperations
    public bool EnableSideEffectsOnInlineProjections => false;

    Task<IProjectionStorage<TDoc, TId>> IStorageOperations.FetchProjectionStorageAsync<TDoc, TId>(
        string tenantId, CancellationToken cancellationToken)
    {
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

    /// <summary>
    ///     Called when a document is queued via Store/Insert/Update.
    ///     Override in IdentityMap session to track in the map.
    /// </summary>
    protected virtual void OnDocumentStored(Type documentType, object id, object document)
    {
        // No-op in lightweight session
    }
}
