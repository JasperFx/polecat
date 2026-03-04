using Microsoft.EntityFrameworkCore;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Wrapper providing access to both Polecat document operations
///     and EF Core DbContext for dual-write projections.
/// </summary>
public class EfCoreOperations<TDbContext> where TDbContext : DbContext
{
    public EfCoreOperations(IDocumentOperations polecat, TDbContext dbContext)
    {
        Polecat = polecat;
        DbContext = dbContext;
    }

    /// <summary>
    ///     Polecat document operations for writing to Polecat tables.
    /// </summary>
    public IDocumentOperations Polecat { get; }

    /// <summary>
    ///     EF Core DbContext for writing to EF-managed tables.
    /// </summary>
    public TDbContext DbContext { get; }
}
