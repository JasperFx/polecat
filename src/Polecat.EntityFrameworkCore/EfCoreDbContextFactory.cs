using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Polecat.Internal;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Factory for creating the DbContext instances EF Core projections work through.
/// </summary>
/// <remarks>
///     <para>
///         #846: a DbContext reads from the moment a projection applies, not from the moment it is
///         flushed, so where those reads go is decided here, by what the session is doing when the
///         context is built.
///     </para>
///     <para>
///         <b>Inside an open transaction (an inline projection, or a fold under an exclusive fetch)</b>
///         the context is attached to that transaction's connection, and there is no second
///         connection. It used to get a placeholder from the same pool, which EF opened for every
///         read while the transaction held a connection, so each save held one and waited for
///         another and concurrency at the pool size deadlocked until the timeout. Reads also have to
///         be inside the transaction to see what the session has written, and a read from a
///         different connection could block on the session's own locks. Marten's fix, opening the
///         placeholder only around each read, cannot deliver either.
///     </para>
///     <para>
///         <b>Outside one (the async daemon's sessions, and live aggregation)</b> there is no
///         transaction to read in. The context gets a placeholder from the session's own connection
///         factory, closed until EF needs it and closed again after each read, so a daemon batch
///         spanning many tenants pins nothing (#683), and the read reaches the database the session
///         belongs to. It used to come from <see cref="StoreOptions.ConnectionString" />, which under
///         database-per-tenant tenancy is the wrong database: the fold missed the row, or found
///         another tenant's.
///     </para>
/// </remarks>
public static class EfCoreDbContextFactory
{
    /// <summary>
    ///     Create a DbContext with a placeholder SqlConnection on <paramref name="connectionString" />.
    ///     EF Core opens the placeholder around each read until it is swapped for a real connection.
    /// </summary>
    public static (TDbContext DbContext, SqlConnection PlaceholderConnection) Create<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TDbContext>(
        string connectionString)
        where TDbContext : DbContext
    {
        return Create<TDbContext>(new SqlConnection(connectionString));
    }

    /// <summary>
    ///     Create the DbContext a projection should use on behalf of <paramref name="session" />.
    /// </summary>
    /// <param name="session">The session the projection is running under.</param>
    /// <param name="fallbackConnectionString">
    ///     Used only for an <see cref="IQuerySession" /> that is not one of Polecat's own, which has
    ///     no connection factory to ask.
    /// </param>
    /// <returns>
    ///     The context, and its placeholder connection if it has one. A context attached to the
    ///     session's transaction does not: the connection is the session's, and so is closing it.
    /// </returns>
    internal static (TDbContext DbContext, SqlConnection? PlaceholderConnection) CreateFor<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TDbContext>(
        IQuerySession session,
        string? fallbackConnectionString = null)
        where TDbContext : DbContext
    {
        var inner = session switch
        {
            QuerySession query => query,
            NestedTenantSession { Parent: QuerySession parent } => parent,
            _ => null
        };

        if (inner is DocumentSessionBase documentSession)
        {
            documentSession.AssertTransactionIsUsable();

            if (documentSession.ActiveTransaction is { } transaction)
            {
                var (attached, _) = Create<TDbContext>(transaction.Connection!);
                attached.Database.UseTransaction(transaction);
                return (attached, null);
            }
        }

        return Create<TDbContext>(inner?.ConnectionFactory.Create() ?? new SqlConnection(fallbackConnectionString));
    }

    private static (TDbContext DbContext, SqlConnection Connection) Create<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TDbContext>(
        SqlConnection connection)
        where TDbContext : DbContext
    {
        var optionsBuilder = new DbContextOptionsBuilder<TDbContext>();
        optionsBuilder.UseSqlServer(connection);

        var dbContext = (TDbContext)Activator.CreateInstance(typeof(TDbContext), optionsBuilder.Options)!;
        return (dbContext, connection);
    }
}
