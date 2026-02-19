using System.Data.Common;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Executes a SQL query and materializes the result.
/// </summary>
internal interface IQueryHandler<T>
{
    Task<T> HandleAsync(DbDataReader reader, CancellationToken token);
}
