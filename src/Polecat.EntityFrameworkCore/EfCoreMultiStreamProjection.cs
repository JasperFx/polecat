using System.Diagnostics.CodeAnalysis;
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
public abstract class EfCoreMultiStreamProjection<
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors
        | DynamicallyAccessedMemberTypes.NonPublicConstructors
        | DynamicallyAccessedMemberTypes.PublicFields
        | DynamicallyAccessedMemberTypes.NonPublicFields
        | DynamicallyAccessedMemberTypes.PublicProperties
        | DynamicallyAccessedMemberTypes.NonPublicProperties
        | DynamicallyAccessedMemberTypes.Interfaces)]
    TDoc, TId,
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
    TDbContext>
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
        // The projection's own storage already owns a DbContext for this tenant and batch, and the
        // participant registered alongside it owns its disposal (#650).
        if (identitySetter is EfCoreProjectionStorage<TDoc, TId, TDbContext> efStorage)
        {
            return ValueTask.FromResult(Apply(snapshot, identity, events, efStorage.DbContext));
        }

        if (_connectionString == null)
        {
            return base.DetermineActionAsync(session, snapshot, identity, identitySetter, events, cancellation);
        }

        var (dbContext, placeholder) = EfCoreDbContextFactory.Create<TDbContext>(_connectionString);

        if (session is ITransactionParticipantRegistrar registrar)
        {
            registrar.AddTransactionParticipant(
                new DbContextTransactionParticipant<TDbContext>(dbContext, placeholder));

            return ValueTask.FromResult(Apply(snapshot, identity, events, dbContext));
        }

        // #650: see the single-stream twin — a plain IQuerySession is not a registrar, so nothing
        // would have owned either object on this route.
        return DisposeAfterApplying(snapshot, identity, events, dbContext, placeholder);
    }

    private (TDoc?, ActionType) Apply(TDoc? snapshot, TId identity, IReadOnlyList<IEvent> events,
        TDbContext dbContext)
    {
        var current = snapshot;
        foreach (var @event in events)
        {
            current = ApplyEvent(current, identity, @event, dbContext);
        }

        return (current, current == null ? ActionType.Delete : ActionType.Store);
    }

    private async ValueTask<(TDoc?, ActionType)> DisposeAfterApplying(TDoc? snapshot, TId identity,
        IReadOnlyList<IEvent> events, TDbContext dbContext, SqlConnection placeholder)
    {
        try
        {
            return Apply(snapshot, identity, events, dbContext);
        }
        finally
        {
            await dbContext.DisposeAsync();
            await placeholder.DisposeAsync();
        }
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
