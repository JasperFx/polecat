using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Factory for creating DbContext instances with placeholder connections.
///     The placeholder is swapped for Polecat's real connection at commit time.
/// </summary>
public static class EfCoreDbContextFactory
{
    /// <summary>
    ///     Create a DbContext with a placeholder SqlConnection.
    ///     The placeholder is never opened — EF Core creates its own for reads.
    ///     At commit time, the DbContextTransactionParticipant swaps to the real connection.
    /// </summary>
    public static (TDbContext DbContext, SqlConnection PlaceholderConnection) Create<TDbContext>(
        string connectionString)
        where TDbContext : DbContext
    {
        var placeholder = new SqlConnection(connectionString);
        var optionsBuilder = new DbContextOptionsBuilder<TDbContext>();
        optionsBuilder.UseSqlServer(placeholder);

        var dbContext = (TDbContext)Activator.CreateInstance(typeof(TDbContext), optionsBuilder.Options)!;
        return (dbContext, placeholder);
    }
}
