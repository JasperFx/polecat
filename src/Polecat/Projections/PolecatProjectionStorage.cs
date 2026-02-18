using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Polecat.Internal;

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

    public PolecatProjectionStorage(DocumentSessionBase session, DocumentProvider provider, string tenantId)
    {
        _session = session;
        _provider = provider;
        TenantId = tenantId;
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
        var op = _provider.BuildDeleteById(id, TenantId);
        _session.WorkTracker.Add(op);
    }

    public void HardDelete(TDoc snapshot, string tenantId)
    {
        var id = _provider.Mapping.GetId(snapshot);
        var op = _provider.BuildDeleteById(id, tenantId);
        _session.WorkTracker.Add(op);
    }

    public void UnDelete(TDoc snapshot)
    {
        // No soft delete support
    }

    public void UnDelete(TDoc snapshot, string tenantId)
    {
        // No soft delete support
    }

    public async Task<TDoc> LoadAsync(TId id, CancellationToken cancellation)
    {
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

        var conn = await _session.GetConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        if (_session.ActiveTransaction != null) cmd.Transaction = _session.ActiveTransaction;

        var paramNames = new string[identities.Length];
        for (var i = 0; i < identities.Length; i++)
        {
            paramNames[i] = $"@id{i}";
            cmd.Parameters.AddWithValue(paramNames[i], (object)identities[i]);
        }

        cmd.CommandText = $"{_provider.SelectSql} WHERE id IN ({string.Join(", ", paramNames)}) AND tenant_id = @tenant_id;";
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
    }

    public void ArchiveStream(TId sliceId, string tenantId)
    {
        // Stream archiving not supported yet
    }
}
