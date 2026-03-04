using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.EntityFrameworkCore;
using Polecat.Projections;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Base class for per-event projections that need both Polecat document
///     operations and EF Core DbContext. Creates a DbContext per batch
///     and registers it as a transaction participant for atomic commits.
/// </summary>
/// <typeparam name="TDbContext">The EF Core DbContext type.</typeparam>
public abstract class EfCoreEventProjection<TDbContext> : ProjectionBase, IProjection
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

        var (dbContext, placeholder) = EfCoreDbContextFactory.Create<TDbContext>(_connectionString);

        foreach (var @event in events)
        {
            await ProjectAsync(@event, dbContext, operations, cancellation);
        }

        if (operations is ITransactionParticipantRegistrar registrar)
        {
            registrar.AddTransactionParticipant(
                new DbContextTransactionParticipant<TDbContext>(dbContext, placeholder));
        }
    }

    internal void SetConnectionString(string connectionString)
    {
        _connectionString = connectionString;
    }
}
