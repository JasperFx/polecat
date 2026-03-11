using System.Linq.Expressions;

namespace Polecat.Linq;

/// <summary>
///     Shared interface for Polecat query providers that support async execution.
/// </summary>
internal interface IPolecatAsyncQueryProvider : IQueryProvider
{
    Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken token);
}
