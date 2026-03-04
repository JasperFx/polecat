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
///     Base class for multi-stream projections that use EF Core DbContext
///     for persistence. Events from multiple streams are routed to aggregates
///     via Identity/Identities configuration.
/// </summary>
/// <typeparam name="TDoc">The aggregate document type (EF Core entity).</typeparam>
/// <typeparam name="TId">The identity type used to route events to aggregates.</typeparam>
/// <typeparam name="TDbContext">The EF Core DbContext type.</typeparam>
public abstract class EfCoreMultiStreamProjection<TDoc, TId, TDbContext>
    : MultiStreamProjection<TDoc, TId>, IValidatedProjection<StoreOptions>
    where TDoc : class
    where TId : notnull
    where TDbContext : DbContext
{
    private string? _connectionString;

    /// <summary>
    ///     Override to apply per-event logic with access to the DbContext.
    ///     Return the updated snapshot, or null to delete.
    /// </summary>
    protected virtual TDoc? ApplyEvent(TDoc? snapshot, TId identity, IEvent @event,
        TDbContext dbContext)
    {
        return snapshot;
    }

    public sealed override ValueTask<(TDoc?, ActionType)> DetermineActionAsync(
        IQuerySession session,
        TDoc? snapshot,
        TId identity,
        IIdentitySetter<TDoc, TId> identitySetter,
        IReadOnlyList<IEvent> events,
        CancellationToken cancellation)
    {
        TDbContext? dbContext = null;
        SqlConnection? placeholderConnection = null;

        // Try to extract DbContext from EfCoreProjectionStorage
        if (identitySetter is EfCoreProjectionStorage<TDoc, TId, TDbContext> efStorage)
        {
            dbContext = efStorage.DbContext;
        }

        // Create new DbContext if not available
        if (dbContext == null && _connectionString != null)
        {
            var (ctx, placeholder) = EfCoreDbContextFactory.Create<TDbContext>(_connectionString);
            dbContext = ctx;
            placeholderConnection = placeholder;

            if (session is ITransactionParticipantRegistrar registrar)
            {
                registrar.AddTransactionParticipant(
                    new DbContextTransactionParticipant<TDbContext>(dbContext, placeholder));
            }
        }

        if (dbContext == null)
        {
            return base.DetermineActionAsync(session, snapshot, identity, identitySetter, events, cancellation);
        }

        var current = snapshot;
        foreach (var @event in events)
        {
            current = ApplyEvent(current, identity, @event, dbContext);
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
