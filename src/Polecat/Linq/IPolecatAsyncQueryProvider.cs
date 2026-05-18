using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Polecat.Linq;

/// <summary>
///     Shared interface for Polecat query providers that support async execution.
/// </summary>
internal interface IPolecatAsyncQueryProvider : IQueryProvider
{
    [RequiresDynamicCode("LINQ execution closes generic handler types over the document/result type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("LINQ execution reflects over the document type and over handler types (Activator.CreateInstance, MethodInfo.Invoke).")]
    Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken token);
}
