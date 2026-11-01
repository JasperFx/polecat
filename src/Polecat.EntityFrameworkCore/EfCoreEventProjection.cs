using System.Diagnostics.CodeAnalysis;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.EntityFrameworkCore;
using Polecat.Internal;
using Polecat.Projections;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Base class for per-event projections that need both Polecat document
///     operations and EF Core DbContext. Creates a DbContext per batch
///     and registers it as a transaction participant for atomic commits.
/// </summary>
/// <typeparam name="TDbContext">The EF Core DbContext type.</typeparam>
public abstract class EfCoreEventProjection<
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
    TDbContext> : ProjectionBase, IProjection
    where TDbContext : DbContext
{
    private string? _connectionString;

    /// <summary>
    ///     Override to project events using both EF Core and Polecat operations.
    /// </summary>
    protected abstract Task ProjectAsync(IEvent @event, TDbContext dbContext,
        IDocumentOperations operations, CancellationToken token);

    /// <summary>
    ///     Called by the projection pipeline for each batch of events.
    ///     Creates a single DbContext for the batch and registers it as a transaction participant.
    /// </summary>
    async Task IJasperFxProjection<IDocumentSession>.ApplyAsync(IDocumentSession operations,
        IReadOnlyList<IEvent> events, CancellationToken cancellation)
    {
        if (_connectionString == null)
        {
            throw new InvalidOperationException(
                "EfCoreEventProjection requires a connection string. Register via EfCoreProjectionExtensions.");
        }

        var (dbContext, placeholder) = EfCoreDbContextFactory.CreateFor<TDbContext>(operations, _connectionString);
        var participant = new DbContextTransactionParticipant<TDbContext>(dbContext, placeholder,
            operations as DocumentSessionBase);

        try
        {
            foreach (var @event in events)
            {
                await ProjectAsync(@event, dbContext, operations, cancellation);
            }
        }
        catch
        {
            // #870: nothing else knows this context exists yet. It is not registered first because
            // it now holds the changes of a batch that failed part way, and a session can outlive
            // that failure: a second save on it would flush them beside its own attempt's.
            await participant.DisposeAsync();
            throw;
        }

        operations.AddTransactionParticipant(participant);
    }

    internal void SetConnectionString(string connectionString)
    {
        _connectionString = connectionString;
    }
}
