using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Polecat.Serialization;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     gh-1016: reads the rows a <c>SelectMany()</c> flattened -- each element's JSON, and its parent's when the
///     projection reads it -- and projects each pair in memory.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: deserializes the rows via ISerializer.FromJson. Element and parent types flow in from the caller's query and are preserved per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson is annotated RDC. AOT consumers supply a source-generator-backed impl.")]
internal class FlattenedRowsHandler<TParent, TElement, TResult> : IQueryHandler<IReadOnlyList<TResult>>
{
    private readonly ISerializer _serializer;
    private readonly Func<TParent, TElement, TResult> _projection;
    private readonly bool _readsParent;

    public FlattenedRowsHandler(ISerializer serializer, Func<TParent, TElement, TResult> projection, bool readsParent)
    {
        _serializer = serializer;
        _projection = projection;
        _readsParent = readsParent;
    }

    public async Task<IReadOnlyList<TResult>> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var list = new List<TResult>();
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            var element = _serializer.FromJson<TElement>(reader.GetString(0));
            var parent = _readsParent ? _serializer.FromJson<TParent>(reader.GetString(1)) : default!;
            list.Add(_projection(parent, element));
        }

        return list;
    }
}
