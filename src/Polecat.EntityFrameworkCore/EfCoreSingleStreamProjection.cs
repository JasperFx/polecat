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
///     Base class for single-stream projections that use EF Core DbContext
///     for persistence. The DbContext participates in Polecat's transaction,
///     ensuring atomic commits of both events and EF Core entities.
/// </summary>
/// <typeparam name="TDoc">The aggregate document type (EF Core entity).</typeparam>
/// <typeparam name="TDbContext">The EF Core DbContext type.</typeparam>
public abstract class EfCoreSingleStreamProjection<
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors
        | DynamicallyAccessedMemberTypes.NonPublicConstructors
        | DynamicallyAccessedMemberTypes.PublicFields
        | DynamicallyAccessedMemberTypes.NonPublicFields
        | DynamicallyAccessedMemberTypes.PublicProperties
        | DynamicallyAccessedMemberTypes.NonPublicProperties
        | DynamicallyAccessedMemberTypes.Interfaces)]
    TDoc,
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
    TDbContext>
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
        // The projection's own storage already owns a DbContext for this tenant and batch, and the
        // participant registered alongside it owns its disposal (#650).
        if (identitySetter is EfCoreProjectionStorage<TDoc, Guid, TDbContext> efStorage)
        {
            return ValueTask.FromResult(Apply(snapshot, identity, events, session, efStorage.DbContext));
        }

        if (_connectionString == null)
        {
            // Fallback: use base class conventional methods
            return base.DetermineActionAsync(session, snapshot, identity, identitySetter, events, cancellation);
        }

        // No EF-backed storage arrived, so build a DbContext just for this call. Live aggregation
        // takes this route on every AggregateStreamAsync.
        var (dbContext, placeholder) = EfCoreDbContextFactory.Create<TDbContext>(_connectionString);

        // Register participant so DbContext flushes in same transaction — and so something owns it.
        if (session is ITransactionParticipantRegistrar registrar)
        {
            registrar.AddTransactionParticipant(
                new DbContextTransactionParticipant<TDbContext>(dbContext, placeholder));

            return ValueTask.FromResult(Apply(snapshot, identity, events, session, dbContext));
        }

        // #650: a plain IQuerySession is NOT an ITransactionParticipantRegistrar, which is exactly
        // what the live-aggregation path passes — so on that route the participant above would have
        // had no owner and nothing would ever have disposed either object. We created them here, so
        // we dispose them here.
        return DisposeAfterApplying(snapshot, identity, events, session, dbContext, placeholder);
    }

    private (TDoc?, ActionType) Apply(TDoc? snapshot, Guid identity, IReadOnlyList<IEvent> events,
        IQuerySession session, TDbContext dbContext)
    {
        var current = snapshot;
        foreach (var @event in events)
        {
            current = ApplyEvent(current, identity, @event, dbContext, session);
        }

        return (current, current == null ? ActionType.Delete : ActionType.Store);
    }

    private async ValueTask<(TDoc?, ActionType)> DisposeAfterApplying(TDoc? snapshot, Guid identity,
        IReadOnlyList<IEvent> events, IQuerySession session, TDbContext dbContext, SqlConnection placeholder)
    {
        try
        {
            return Apply(snapshot, identity, events, session, dbContext);
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
