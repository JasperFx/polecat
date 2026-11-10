using Microsoft.Data.SqlClient;

namespace Polecat;

/// <summary>
///     Allows external components (e.g., EF Core DbContext) to participate
///     in a Polecat session's transaction. BeforeCommitAsync is called after
///     all Polecat SQL operations execute but BEFORE the transaction commits.
/// </summary>
/// <remarks>
///     The SQL Server shape of Weasel.Storage's shared participant contract (#807). Its
///     <see cref="Weasel.Storage.ITransactionParticipant{TConnection,TTransaction}.AfterCommitAsync" />
///     runs once, after a session's save or a daemon batch has committed, and never for an attempt
///     that rolled back.
/// </remarks>
public interface ITransactionParticipant : Weasel.Storage.ITransactionParticipant<SqlConnection, SqlTransaction>;
