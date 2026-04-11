using JasperFx.Events;
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
    private readonly SemaphoreSlim _dbContextLock = new(1, 1);

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
        _dbContextLock.Wait();
        try
        {
            SetIdentityUnsafe(document, identity);
        }
        finally
        {
            _dbContextLock.Release();
        }
    }

    private void SetIdentityUnsafe(TDoc document, TId identity)
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
        _dbContextLock.Wait();
        try
        {
            return IdentityUnsafe(document);
        }
        finally
        {
            _dbContextLock.Release();
        }
    }

    private TId IdentityUnsafe(TDoc document)
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
        _dbContextLock.Wait();
        try
        {
            AddOrUpdateUnsafe(snapshot);
        }
        finally
        {
            _dbContextLock.Release();
        }
    }

    public void Store(TDoc snapshot, TId id, string tenantId)
    {
        _dbContextLock.Wait();
        try
        {
            SetIdentityUnsafe(snapshot, id);
            AddOrUpdateUnsafe(snapshot);
        }
        finally
        {
            _dbContextLock.Release();
        }
    }

    public void Delete(TId identity)
    {
        _dbContextLock.Wait();
        try
        {
            var existing = _dbContext.Find<TDoc>(identity);
            if (existing != null)
                _dbContext.Remove(existing);
        }
        finally
        {
            _dbContextLock.Release();
        }
    }

    public void Delete(TId identity, string tenantId)
    {
        Delete(identity);
    }

    public void HardDelete(TDoc snapshot)
    {
        _dbContextLock.Wait();
        try
        {
            _dbContext.Remove(snapshot);
        }
        finally
        {
            _dbContextLock.Release();
        }
    }

    public void HardDelete(TDoc snapshot, string tenantId)
    {
        HardDelete(snapshot);
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
        await _dbContextLock.WaitAsync(cancellation);
        try
        {
            return (await _dbContext.FindAsync<TDoc>([id], cancellation))!;
        }
        finally
        {
            _dbContextLock.Release();
        }
    }

    public async Task<IReadOnlyDictionary<TId, TDoc>> LoadManyAsync(TId[] identities,
        CancellationToken cancellationToken)
    {
        await _dbContextLock.WaitAsync(cancellationToken);
        try
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
        finally
        {
            _dbContextLock.Release();
        }
    }

    public void StoreProjection(TDoc aggregate, IEvent? lastEvent, AggregationScope scope)
    {
        _dbContextLock.Wait();
        try
        {
            AddOrUpdateUnsafe(aggregate);
        }
        finally
        {
            _dbContextLock.Release();
        }
    }

    public void ArchiveStream(TId sliceId, string tenantId)
    {
        // Not applicable for EF Core projection storage
    }

    private void AddOrUpdateUnsafe(TDoc entity)
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
