using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Transaction participant that swaps a DbContext's placeholder connection
///     to Polecat's real connection/transaction before commit, then calls
///     DbContext.SaveChangesAsync() within the same atomic transaction.
/// </summary>
internal class DbContextTransactionParticipant<TDbContext> : ITransactionParticipant
    where TDbContext : DbContext
{
    private readonly TDbContext _dbContext;
    private readonly SqlConnection _placeholderConnection;

    public DbContextTransactionParticipant(TDbContext dbContext, SqlConnection placeholderConnection)
    {
        _dbContext = dbContext;
        _placeholderConnection = placeholderConnection;
    }

    public async Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        // Swap DbContext to Polecat's real connection and transaction
        _dbContext.Database.SetDbConnection(connection);
        await _dbContext.Database.UseTransactionAsync(transaction, token);

        // Flush EF Core changes within Polecat's transaction
        await _dbContext.SaveChangesAsync(token);

        // Dispose the placeholder connection (never actually opened for SQL Server)
        await _placeholderConnection.DisposeAsync();
    }
}
