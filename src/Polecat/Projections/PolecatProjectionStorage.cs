using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Internal;
using Polecat.Internal.Operations;
using Polecat.Metadata;

namespace Polecat.Projections;

/// <summary>
///     Adapts Polecat's document session into the IProjectionStorage interface
///     that JasperFx.Events uses to persist projected documents.
/// </summary>
internal class PolecatProjectionStorage<TDoc, TId> : IProjectionStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    private readonly DocumentSessionBase _session;
    private readonly DocumentProvider _provider;
    private bool _tableEnsured;

    public PolecatProjectionStorage(DocumentSessionBase session, DocumentProvider provider, string tenantId)
    {
        _session = session;
        _provider = provider;
        TenantId = tenantId;
    }

    private async Task EnsureTableExistsAsync(CancellationToken cancellation)
    {
        if (_tableEnsured) return;
        await _session.EnsureTableForProviderAsync(_provider, cancellation);
        _tableEnsured = true;
    }

    public string TenantId { get; }

    public void SetIdentity(TDoc document, TId identity)
    {
        _provider.Mapping.SetId(document, identity!);
    }

    public TId Identity(TDoc document)
    {
        return (TId)_provider.Mapping.GetId(document);
    }

    public void Store(TDoc snapshot)
    {
        var op = _provider.BuildUpsert(snapshot, _session.Serializer, TenantId);
        _session.WorkTracker.Add(op);
    }

    public void Store(TDoc snapshot, TId id, string tenantId)
    {
        SetIdentity(snapshot, id);
        var op = _provider.BuildUpsert(snapshot, _session.Serializer, tenantId);
        _session.WorkTracker.Add(op);

        // Cache snapshot on pc_streams for fast AggregateStreamAsync
        QueueSnapshotUpdate(snapshot, id, tenantId);
    }

    public void Delete(TId identity)
    {
        var op = _provider.BuildDeleteById(identity!, TenantId);
        _session.WorkTracker.Add(op);
    }

    public void Delete(TId identity, string tenantId)
    {
        var op = _provider.BuildDeleteById(identity!, tenantId);
        _session.WorkTracker.Add(op);
    }

    public void HardDelete(TDoc snapshot)
    {
        var id = _provider.Mapping.GetId(snapshot);
        var op = _provider.BuildHardDeleteById(id, TenantId);
        _session.WorkTracker.Add(op);
    }

    public void HardDelete(TDoc snapshot, string tenantId)
    {
        var id = _provider.Mapping.GetId(snapshot);
        var op = _provider.BuildHardDeleteById(id, tenantId);
        _session.WorkTracker.Add(op);
    }

    public void UnDelete(TDoc snapshot)
    {
        if (_provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            var id = _provider.Mapping.GetId(snapshot);
            var op = new UnDeleteByIdOperation(id, _provider.Mapping, TenantId);
            _session.WorkTracker.Add(op);
        }
    }

    public void UnDelete(TDoc snapshot, string tenantId)
    {
        if (_provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            var id = _provider.Mapping.GetId(snapshot);
            var op = new UnDeleteByIdOperation(id, _provider.Mapping, tenantId);
            _session.WorkTracker.Add(op);
        }
    }

    public async Task<TDoc> LoadAsync(TId id, CancellationToken cancellation)
    {
        await EnsureTableExistsAsync(cancellation);
        var conn = await _session.GetConnectionAsync(cancellation);
        await using var cmd = conn.CreateCommand();
        if (_session.ActiveTransaction != null) cmd.Transaction = _session.ActiveTransaction;
        cmd.CommandText = _provider.LoadSql;
        cmd.Parameters.AddWithValue("@id", (object)id);
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellation);
        if (await reader.ReadAsync(cancellation))
        {
            var json = reader.GetString(1);
            return _session.Serializer.FromJson<TDoc>(json)!;
        }

        return default!;
    }

    public async Task<IReadOnlyDictionary<TId, TDoc>> LoadManyAsync(TId[] identities,
        CancellationToken cancellationToken)
    {
        var dict = new Dictionary<TId, TDoc>();
        if (identities.Length == 0) return dict;

        await EnsureTableExistsAsync(cancellationToken);
        var conn = await _session.GetConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        if (_session.ActiveTransaction != null) cmd.Transaction = _session.ActiveTransaction;

        var paramNames = new string[identities.Length];
        for (var i = 0; i < identities.Length; i++)
        {
            paramNames[i] = $"@id{i}";
            cmd.Parameters.AddWithValue(paramNames[i], (object)identities[i]);
        }

        var softDeleteFilter = _provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";
        cmd.CommandText = $"{_provider.SelectSql} WHERE id IN ({string.Join(", ", paramNames)}) AND tenant_id = @tenant_id{softDeleteFilter};";
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var json = reader.GetString(1);
            var doc = _session.Serializer.FromJson<TDoc>(json)!;
            var id = Identity(doc);
            dict[id] = doc;
        }

        return dict;
    }

    public void StoreProjection(TDoc aggregate, IEvent? lastEvent, AggregationScope scope)
    {
        Store(aggregate);

        // Also cache the snapshot on pc_streams for fast AggregateStreamAsync
        if (lastEvent != null)
        {
            var streamId = GetStreamId(lastEvent);
            if (streamId != null)
            {
                var json = _session.Serializer.ToJson(aggregate);
                var snapshotOp = new UpdateSnapshotOperation(
                    _session.EventGraph, streamId, TenantId, json, lastEvent.Version);
                _session.WorkTracker.Add(snapshotOp);
            }
        }
    }

    private void QueueSnapshotUpdate(TDoc snapshot, TId id, string tenantId)
    {
        // Find the matching stream to get the version for the snapshot
        var stream = FindStreamForId(id);
        if (stream == null) return;

        var streamId = _session.EventGraph.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? (object)stream.Id
            : stream.Key!;

        var json = _session.Serializer.ToJson(snapshot);
        var snapshotOp = new UpdateSnapshotOperation(
            _session.EventGraph, streamId, tenantId, json, stream.Version);
        _session.WorkTracker.Add(snapshotOp);
    }

    private StreamAction? FindStreamForId(TId id)
    {
        foreach (var stream in _session.WorkTracker.Streams)
        {
            if (id is Guid guidId && stream.Id == guidId) return stream;
            if (id is string stringId && stream.Key == stringId) return stream;
        }
        return null;
    }

    private static object? GetStreamId(IEvent @event)
    {
        if (@event.StreamId != Guid.Empty) return @event.StreamId;
        if (!string.IsNullOrEmpty(@event.StreamKey)) return @event.StreamKey;
        return null;
    }

    public void ArchiveStream(TId sliceId, string tenantId)
    {
        // Stream archiving not supported yet
    }
}
