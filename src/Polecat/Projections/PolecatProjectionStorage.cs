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
///     All SQL execution routes through session's Polly-wrapped centralized methods.
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
        var rawId = _provider.Mapping.GetId(document);

        // If TId is the inner type (e.g., Guid/string) but GetId returned the unwrapped value, cast directly
        if (rawId is TId typedId)
        {
            return typedId;
        }

        // If TId is a wrapper type (e.g., PaymentId) and GetId returned the inner value,
        // wrap it back up
        if (_provider.Mapping.ValueTypeId != null)
        {
            object wrapped;
            if (_provider.Mapping.ValueTypeId.Ctor != null)
            {
                wrapped = _provider.Mapping.ValueTypeId.Ctor.Invoke([rawId]);
            }
            else
            {
                wrapped = _provider.Mapping.ValueTypeId.Builder!.Invoke(null, [rawId])!;
            }

            return (TId)wrapped;
        }

        return (TId)rawId;
    }

    /// <summary>
    ///     Unwrap a strongly-typed ID to its inner value for use as a SQL parameter.
    ///     If the ID is not a value type wrapper, returns it unchanged.
    /// </summary>
    private object UnwrapId(TId id)
    {
        if (_provider.Mapping.ValueTypeId != null)
        {
            // Only unwrap if id is actually the wrapper type (not already the inner type)
            var wrapperType = Nullable.GetUnderlyingType(_provider.Mapping.IdType) ?? _provider.Mapping.IdType;
            if (id.GetType() == wrapperType)
            {
                return _provider.Mapping.ValueTypeId.ValueProperty.GetValue(id)!;
            }
        }

        return id;
    }

    public void Store(TDoc snapshot)
    {
        var op = _provider.BuildUpsert(snapshot, _session.Serializer, TenantId);
        _session.WorkTracker.Add(op);
    }

    public void Store(TDoc document, TId id, string tenantId)
    {
        SetIdentity(document, id);
        var op = _provider.BuildUpsert(document, _session.Serializer, tenantId);
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

        await using var cmd = new SqlCommand();
        cmd.CommandText = _provider.LoadSql;
        cmd.Parameters.AddWithValue("@id", UnwrapId(id));
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        await using var reader = await _session.ExecuteReaderAsync(cmd, cancellation);
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

        await using var cmd = new SqlCommand();

        var paramNames = new string[identities.Length];
        for (var i = 0; i < identities.Length; i++)
        {
            paramNames[i] = $"@id{i}";
            cmd.Parameters.AddWithValue(paramNames[i], UnwrapId(identities[i]));
        }

        var softDeleteFilter = _provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";
        cmd.CommandText = $"{_provider.SelectSql} WHERE id IN ({string.Join(", ", paramNames)}) AND tenant_id = @tenant_id{softDeleteFilter};";
        cmd.Parameters.AddWithValue("@tenant_id", TenantId);

        await using var reader = await _session.ExecuteReaderAsync(cmd, cancellationToken);
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
