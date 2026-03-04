using JasperFx.Events.Projections;
using Microsoft.EntityFrameworkCore;
using Polecat.Internal;
using Polecat.Projections;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Extension methods for registering EF Core-backed projections with Polecat.
/// </summary>
public static class EfCoreProjectionExtensions
{
    /// <summary>
    ///     Register an EF Core single-stream projection.
    /// </summary>
    public static void Add<TProjection, TDoc, TDbContext>(
        this PolecatProjectionOptions projections,
        StoreOptions options,
        TProjection projection,
        ProjectionLifecycle lifecycle)
        where TProjection : EfCoreSingleStreamProjection<TDoc, TDbContext>
        where TDoc : class
        where TDbContext : DbContext
    {
        projection.Lifecycle = lifecycle;
        projection.SetConnectionString(options.ConnectionString);
        projection.AssembleAndAssertValidity();
        RegisterEfCoreStorage<TDoc, Guid, TDbContext>(options);

        projections.All.Add((IProjectionSource<IDocumentSession, IQuerySession>)projection);
    }

    /// <summary>
    ///     Register an EF Core multi-stream projection.
    /// </summary>
    public static void Add<TProjection, TDoc, TId, TDbContext>(
        this PolecatProjectionOptions projections,
        StoreOptions options,
        TProjection projection,
        ProjectionLifecycle lifecycle)
        where TProjection : EfCoreMultiStreamProjection<TDoc, TId, TDbContext>
        where TDoc : class
        where TId : notnull
        where TDbContext : DbContext
    {
        projection.Lifecycle = lifecycle;
        projection.SetConnectionString(options.ConnectionString);
        projection.AssembleAndAssertValidity();
        RegisterEfCoreStorage<TDoc, TId, TDbContext>(options);

        projections.All.Add((IProjectionSource<IDocumentSession, IQuerySession>)projection);
    }

    /// <summary>
    ///     Register an EF Core event projection.
    /// </summary>
    public static void Add<TProjection, TDbContext>(
        this PolecatProjectionOptions projections,
        StoreOptions options,
        TProjection projection,
        ProjectionLifecycle lifecycle)
        where TProjection : EfCoreEventProjection<TDbContext>
        where TDbContext : DbContext
    {
        projection.Lifecycle = lifecycle;
        projection.SetConnectionString(options.ConnectionString);
        projection.AssembleAndAssertValidity();

        // Wrap the IProjection in a ProjectionWrapper to get IProjectionSource plumbing
        var wrapper = new ProjectionWrapper<IDocumentSession, IQuerySession>(projection, lifecycle);
        projections.All.Add(wrapper);
    }

    /// <summary>
    ///     Register a custom EF Core projection storage provider for a document type.
    ///     When the JasperFx projection pipeline requests storage for TDoc,
    ///     it will get an EfCoreProjectionStorage backed by TDbContext.
    /// </summary>
    internal static void RegisterEfCoreStorage<TDoc, TId, TDbContext>(StoreOptions options)
        where TDoc : class
        where TId : notnull
        where TDbContext : DbContext
    {
        if (options.CustomProjectionStorageProviders.ContainsKey(typeof(TDoc)))
            return;

        options.CustomProjectionStorageProviders[typeof(TDoc)] = (session, tenantId) =>
        {
            var connectionString = options.ConnectionString;
            var (dbContext, placeholder) = EfCoreDbContextFactory.Create<TDbContext>(connectionString);

            // Register the participant so the DbContext flushes in the same transaction
            session.AddTransactionParticipant(
                new DbContextTransactionParticipant<TDbContext>(dbContext, placeholder));

            return new EfCoreProjectionStorage<TDoc, TId, TDbContext>(dbContext, tenantId);
        };
    }
}
