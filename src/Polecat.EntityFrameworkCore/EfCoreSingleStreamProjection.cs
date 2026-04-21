using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Polecat.Internal;
using Polecat.Projections;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Base class for single-stream projections that use EF Core DbContext
///     for persistence. The DbContext participates in Polecat's transaction,
///     ensuring atomic commits of both events and EF Core entities.
/// </summary>
/// <typeparam name="TDoc">The aggregate document type (EF Core entity).</typeparam>
/// <typeparam name="TDbContext">The EF Core DbContext type.</typeparam>
public abstract class EfCoreSingleStreamProjection<TDoc, TDbContext>
    : SingleStreamProjection<TDoc, Guid>, IValidatedProjection<StoreOptions>
    where TDoc : class
    where TDbContext : DbContext
{
    private string? _connectionString;

    /// <summary>
    ///     Override to apply per-event logic with access to the DbContext.
    ///     Return the updated snapshot, or null to delete.
    /// </summary>
    protected virtual TDoc? ApplyEvent(TDoc? snapshot, Guid identity, IEvent @event,
        TDbContext dbContext, IQuerySession session)
    {
        return snapshot;
    }

    public sealed override ValueTask<(TDoc?, ActionType)> DetermineActionAsync(
        IQuerySession session,
        TDoc? snapshot,
        Guid identity,
        IIdentitySetter<TDoc, Guid> identitySetter,
        IReadOnlyList<IEvent> events,
        CancellationToken cancellation)
    {
        TDbContext? dbContext = null;

        // Try to extract DbContext from EfCoreProjectionStorage
        if (identitySetter is EfCoreProjectionStorage<TDoc, Guid, TDbContext> efStorage)
        {
            dbContext = efStorage.DbContext;
        }

        // Create new DbContext if not available (e.g., live aggregation)
        if (dbContext == null && _connectionString != null)
        {
            var (ctx, placeholder) = EfCoreDbContextFactory.Create<TDbContext>(_connectionString);
            dbContext = ctx;

            // Register participant so DbContext flushes in same transaction
            if (session is ITransactionParticipantRegistrar registrar)
            {
                registrar.AddTransactionParticipant(
                    new DbContextTransactionParticipant<TDbContext>(dbContext, placeholder));
            }
        }

        if (dbContext == null)
        {
            // Fallback: use base class conventional methods
            return base.DetermineActionAsync(session, snapshot, identity, identitySetter, events, cancellation);
        }

        // Apply events through the DbContext-aware method
        var current = snapshot;
        foreach (var @event in events)
        {
            current = ApplyEvent(current, identity, @event, dbContext, session);
        }

        var action = current == null ? ActionType.Delete : ActionType.Store;
        return ValueTask.FromResult((current, action));
    }

    internal void SetConnectionString(string connectionString)
    {
        _connectionString = connectionString;
    }

    IEnumerable<string> IValidatedProjection<StoreOptions>.ValidateConfiguration(StoreOptions options)
    {
        if (options.Events.TenancyStyle == TenancyStyle.Conjoined
            && !typeof(TDoc).IsAssignableTo(typeof(Metadata.ITenanted)))
        {
            yield return
                $"EF Core projection aggregate type {typeof(TDoc).Name} must implement ITenanted when using conjoined tenancy.";
        }
    }
}
