using System.Data.Common;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Executes a SQL query and materializes the result, without naming the result type.
/// </summary>
/// <remarks>
///     <para>
///         ⚠️ <b>#741: this exists so the LINQ provider never calls
///         <c>handlerType.GetMethod("HandleAsync")</c>, which is what broke every LINQ query under
///         Native AOT.</b> A handler is built over a runtime item type, so the provider held it as
///         <see cref="object" /> and reached its method reflectively — and
///         <see cref="System.Type.GetMethod(string)" /> returns <b>null</b> for a member the trimmer
///         removed rather than throwing, so the null-forgiving operator one line later produced a
///         bare <see cref="System.NullReferenceException" /> naming nothing.
///     </para>
///     <para>
///         <b>A statically-referenced interface survives trimming; a reflected member does not.</b>
///         That is the same lesson weasel#689 settled for the identity seam, applied here. The
///         provider casts to this and calls an ordinary interface method.
///     </para>
///     <para>
///         The result is boxed, which is why this is the AOT-shaped seam rather than the only one:
///         <see cref="IQueryHandler{T}.HandleAsync" /> stays for callers that know their type.
///     </para>
/// </remarks>
internal interface IQueryHandler
{
    Task<object?> HandleAsObjectAsync(DbDataReader reader, CancellationToken token);
}

/// <summary>
///     Executes a SQL query and materializes the result.
/// </summary>
internal interface IQueryHandler<T> : IQueryHandler
{
    Task<T> HandleAsync(DbDataReader reader, CancellationToken token);

    /// <inheritdoc />
    async Task<object?> IQueryHandler.HandleAsObjectAsync(DbDataReader reader, CancellationToken token)
        => await HandleAsync(reader, token).ConfigureAwait(false);
}
