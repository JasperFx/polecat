using System.Linq.Expressions;
using JasperFx.Events.Vectors;

namespace Polecat.Internal;

/// <summary>
///     Polecat's implementation of the store-neutral similarity-search contract (jasperfx#842),
///     reached through <see cref="JasperFx.Events.Documents.IDocumentReadOperations.Search" />.
/// </summary>
/// <remarks>
///     <para>
///         <b>A separate object rather than members on the session, and that is the whole design of
///         the shared contract.</b> Polecat's extension methods are already named
///         <c>VectorSearchWithScoresAsync</c> and <c>HybridSearchWithScoresAsync</c>. Putting members
///         of those names on an interface <c>IQuerySession</c> implements would make the instance
///         member win overload resolution over the extension at every existing call site — silently,
///         with no error, and against a different implementation. Keeping the contract behind the
///         <c>Search</c> accessor makes that collision impossible to have.
///     </para>
///     <para>
///         Nothing is reimplemented here: both methods forward to the extension methods, so tenancy,
///         soft deletes, the vector-declaration refusals and the full-text member inference are the
///         ones a Polecat caller already gets. What this buys is that code written against
///         <c>IDocumentReadOperations</c> — a library that must compile once against Marten, Polecat
///         and Fisher — can ask for the ten nearest documents without naming a Polecat type.
///     </para>
/// </remarks>
internal sealed class PolecatSearchOperations(IQuerySession session): IDocumentSearchOperations
{
    public Task<IReadOnlyList<VectorMatch<T>>> VectorSearchWithScoresAsync<T>(
        Expression<Func<T, object?>> member,
        ReadOnlyMemory<float> query,
        int limit = 10,
        DistanceFunction? distance = null,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken token = default) where T : notnull
        => session.VectorSearchWithScoresAsync(member, query, limit, distance, filter, token);

    public Task<IReadOnlyList<HybridMatch<T>>> HybridSearchWithScoresAsync<T>(
        Expression<Func<T, object?>> vectorMember,
        string text,
        ReadOnlyMemory<float> query,
        int limit = 10,
        HybridSearchOptions? options = null,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken token = default) where T : notnull
        => session.HybridSearchWithScoresAsync(vectorMember, text, query, limit, options, filter, token);
}
