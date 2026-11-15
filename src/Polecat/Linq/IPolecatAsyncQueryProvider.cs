using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace Polecat.Linq;

/// <summary>
///     Shared interface for Polecat query providers that support async execution.
/// </summary>
internal interface IPolecatAsyncQueryProvider : IQueryProvider
{
    [RequiresDynamicCode("LINQ execution closes generic handler types over the document/result type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("LINQ execution reflects over the document type and over handler types (Activator.CreateInstance, MethodInfo.Invoke).")]
    Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken token);

    /// <summary>
    ///     #801: the rows of a query as they are read, for
    ///     <see cref="PolecatQueryableExtensions.ToAsyncEnumerable{T}" />. A provider that cannot read row by
    ///     row materializes the list and hands it out, which is what this default does.
    /// </summary>
    [RequiresDynamicCode("LINQ execution closes generic handler types over the document/result type via Type.MakeGenericType.")]
    [RequiresUnreferencedCode("LINQ execution reflects over the document type and over handler types (Activator.CreateInstance, MethodInfo.Invoke).")]
    async IAsyncEnumerable<T> ExecuteStreamAsync<T>(Expression expression,
        [EnumeratorCancellation] CancellationToken token)
    {
        var all = await ExecuteAsync<IReadOnlyList<T>>(expression, token).ConfigureAwait(false);
        foreach (var item in all)
        {
            yield return item;
        }
    }
}
