using Microsoft.Data.SqlClient;

namespace Polecat;

/// <summary>
///     Allows external components (e.g., EF Core DbContext) to participate
///     in a Polecat session's transaction. BeforeCommitAsync is called after
///     all Polecat SQL operations execute but BEFORE the transaction commits.
/// </summary>
public interface ITransactionParticipant
{
    Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token);
}
