using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using Microsoft.EntityFrameworkCore;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     IProjectionStorage implementation backed by EF Core DbContext.
///     All mutations go through DbContext change tracking and are flushed
///     at commit time by the DbContextTransactionParticipant.
/// </summary>
internal class EfCoreProjectionStorage<TDoc, TId, TDbContext> : IProjectionStorage<TDoc, TId>
    where TDoc : class
    where TId : notnull
    where TDbContext : DbContext
{
    private readonly TDbContext _dbContext;

    public EfCoreProjectionStorage(TDbContext dbContext, string tenantId)
    {
        _dbContext = dbContext;
        TenantId = tenantId;
    }

    /// <summary>
    ///     Expose DbContext so projection classes can extract it from the storage.
    /// </summary>
    public TDbContext DbContext => _dbContext;

    public string TenantId { get; }

    public void SetIdentity(TDoc document, TId identity)
    {
        var entry = _dbContext.Entry(document);
        var pk = entry.Metadata.FindPrimaryKey();
        if (pk != null)
        {
            var prop = pk.Properties[0];
            entry.Property(prop.Name).CurrentValue = identity;
        }
    }

    public TId Identity(TDoc document)
    {
        var entry = _dbContext.Entry(document);
        var pk = entry.Metadata.FindPrimaryKey();
        if (pk != null)
        {
            var prop = pk.Properties[0];
            return (TId)entry.Property(prop.Name).CurrentValue!;
        }

        throw new InvalidOperationException($"Cannot determine primary key for {typeof(TDoc).Name}");
    }

    public void Store(TDoc snapshot)
    {
        AddOrUpdate(snapshot);
    }

    public void Store(TDoc snapshot, TId id, string tenantId)
    {
        SetIdentity(snapshot, id);
        AddOrUpdate(snapshot);
    }

    public void Delete(TId identity)
    {
        var existing = _dbContext.Find<TDoc>(identity);
        if (existing != null)
        {
            _dbContext.Remove(existing);
        }
    }

    public void Delete(TId identity, string tenantId)
    {
        Delete(identity);
    }

    public void HardDelete(TDoc snapshot)
    {
        _dbContext.Remove(snapshot);
    }

    public void HardDelete(TDoc snapshot, string tenantId)
    {
        _dbContext.Remove(snapshot);
    }

    public void UnDelete(TDoc snapshot)
    {
        // No soft-delete concept for EF Core projection storage
    }

    public void UnDelete(TDoc snapshot, string tenantId)
    {
        // No soft-delete concept for EF Core projection storage
    }

    public async Task<TDoc> LoadAsync(TId id, CancellationToken cancellation)
    {
        return (await _dbContext.FindAsync<TDoc>([id], cancellation))!;
    }

    public async Task<IReadOnlyDictionary<TId, TDoc>> LoadManyAsync(TId[] identities,
        CancellationToken cancellationToken)
    {
        var dict = new Dictionary<TId, TDoc>();
        foreach (var id in identities)
        {
            var doc = await _dbContext.FindAsync<TDoc>([id], cancellationToken);
            if (doc != null)
            {
                dict[id] = doc;
            }
        }

        return dict;
    }

    public void StoreProjection(TDoc aggregate, IEvent? lastEvent, AggregationScope scope)
    {
        AddOrUpdate(aggregate);
    }

    public void ArchiveStream(TId sliceId, string tenantId)
    {
        // Not applicable for EF Core projection storage
    }

    private void AddOrUpdate(TDoc entity)
    {
        var entry = _dbContext.Entry(entity);
        switch (entry.State)
        {
            case EntityState.Detached:
                // Try to find existing
                var pk = entry.Metadata.FindPrimaryKey();
                if (pk != null)
                {
                    var keyValues = pk.Properties
                        .Select(p => entry.Property(p.Name).CurrentValue)
                        .ToArray();
                    var existing = _dbContext.Find<TDoc>(keyValues);
                    if (existing != null)
                    {
                        var existingEntry = _dbContext.Entry(existing);
                        existingEntry.CurrentValues.SetValues(entity);
                        existingEntry.State = EntityState.Modified;
                        return;
                    }
                }

                _dbContext.Add(entity);
                break;
            case EntityState.Unchanged:
                entry.State = EntityState.Modified;
                break;
            // Added, Modified — already tracked correctly
        }
    }
}
