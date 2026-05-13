using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Polecat.Serialization;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads two data columns (outer and inner) and applies a compiled projection function.
///     For LEFT JOIN, the inner column may be NULL.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: invokes ISerializer.FromJson<T>, which is annotated RUC because the default STJ-reflection serializer requires unreferenced code. AOT consumers supply a source-generator-backed ISerializer impl per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson<T> is annotated RDC for the same reason as IL2026 above. AOT consumers supply a source-generator-backed ISerializer impl.")]
internal class JoinListHandler<TOuter, TInner, TResult> : IQueryHandler<IReadOnlyList<TResult>>
    where TOuter : class
    where TInner : class
{
    private readonly ISerializer _serializer;
    private readonly Func<TOuter, TInner?, TResult> _projection;
    private readonly bool _isLeftJoin;

    public JoinListHandler(ISerializer serializer, Func<TOuter, TInner?, TResult> projection, bool isLeftJoin)
    {
        _serializer = serializer;
        _projection = projection;
        _isLeftJoin = isLeftJoin;
    }

    public async Task<IReadOnlyList<TResult>> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var list = new List<TResult>();
        while (await reader.ReadAsync(token))
        {
            var outerJson = reader.GetString(0);
            var outer = _serializer.FromJson<TOuter>(outerJson);

            TInner? inner = null;
            if (!reader.IsDBNull(1))
            {
                var innerJson = reader.GetString(1);
                inner = _serializer.FromJson<TInner>(innerJson);
            }

            list.Add(_projection(outer, inner));
        }

        return list;
    }
}
