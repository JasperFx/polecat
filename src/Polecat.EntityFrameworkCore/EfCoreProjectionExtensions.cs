using System.Diagnostics.CodeAnalysis;
using JasperFx.Events.Projections;
using Microsoft.EntityFrameworkCore;
using Polecat.Internal;
using Polecat.Projections;
using Weasel.EntityFrameworkCore;
using Weasel.SqlServer;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Extension methods for registering EF Core-backed projections with Polecat.
/// </summary>
public static class EfCoreProjectionExtensions
{
    /// <summary>
    ///     Register an EF Core single-stream projection.
    /// </summary>
    public static void Add<TProjection,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors
            | DynamicallyAccessedMemberTypes.NonPublicConstructors
            | DynamicallyAccessedMemberTypes.PublicFields
            | DynamicallyAccessedMemberTypes.NonPublicFields
            | DynamicallyAccessedMemberTypes.PublicProperties
            | DynamicallyAccessedMemberTypes.NonPublicProperties
            | DynamicallyAccessedMemberTypes.Interfaces)]
        TDoc,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TDbContext>(
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
    public static void Add<TProjection,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors
            | DynamicallyAccessedMemberTypes.NonPublicConstructors
            | DynamicallyAccessedMemberTypes.PublicFields
            | DynamicallyAccessedMemberTypes.NonPublicFields
            | DynamicallyAccessedMemberTypes.PublicProperties
            | DynamicallyAccessedMemberTypes.NonPublicProperties
            | DynamicallyAccessedMemberTypes.Interfaces)]
        TDoc, TId,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TDbContext>(
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
    public static void Add<TProjection,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TDbContext>(
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
    ///     Register EF Core entity tables from a <typeparamref name="TDbContext"/> with Polecat's
    ///     Weasel migration pipeline. Tables defined in the DbContext's model will be created
    ///     and migrated automatically alongside Polecat's own schema objects.
    ///     <para>
    ///     Tables with an explicit schema configured in EF Core (via <c>HasDefaultSchema</c> or
    ///     <c>ToTable("name", "schema")</c>) will retain their configured schema. Tables without
    ///     an explicit schema will use the SQL Server default ("dbo").
    ///     </para>
    /// </summary>
    public static void AddEntityTablesFromDbContext<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TDbContext>(this StoreOptions options,
        Action<DbContextOptionsBuilder<TDbContext>>? configure = null)
        where TDbContext : DbContext
    {
        var migrator = new SqlServerMigrator();

        // Create a temporary DbContext just to read its entity model.
        // The connection is never opened; it's only needed to satisfy UseSqlServer's requirement.
        var builder = new DbContextOptionsBuilder<TDbContext>();
        builder.UseSqlServer("Server=localhost");
        configure?.Invoke(builder);

        using var dbContext = (TDbContext)Activator.CreateInstance(typeof(TDbContext), builder.Options)!;

        foreach (var entityType in DbContextExtensions.GetEntityTypesForMigration(dbContext))
        {
            var table = migrator.MapToTable(entityType);
            options.ExtendedSchemaObjects.Add(table);
        }
    }

    /// <summary>
    ///     Register a custom EF Core projection storage provider for a document type.
    ///     When the JasperFx projection pipeline requests storage for TDoc,
    ///     it will get an EfCoreProjectionStorage backed by TDbContext.
    /// </summary>
    internal static void RegisterEfCoreStorage<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors
            | DynamicallyAccessedMemberTypes.NonPublicConstructors
            | DynamicallyAccessedMemberTypes.PublicFields
            | DynamicallyAccessedMemberTypes.NonPublicFields
            | DynamicallyAccessedMemberTypes.PublicProperties
            | DynamicallyAccessedMemberTypes.NonPublicProperties
            | DynamicallyAccessedMemberTypes.Interfaces)]
        TDoc, TId,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TDbContext>(StoreOptions options)
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
