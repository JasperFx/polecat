using System.Data.Common;
using System.Linq.Expressions;
using Polecat.Linq.Selectors;
using Polecat.Serialization;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads full documents and projects them using a compiled Select lambda.
/// </summary>
internal class ProjectionListHandler<TSource, TResult> : IQueryHandler<IReadOnlyList<TResult>> where TSource : class
{
    private readonly DeserializingSelector<TSource> _selector;
    private readonly Func<TSource, TResult> _projection;

    public ProjectionListHandler(ISerializer serializer, LambdaExpression selectExpression)
    {
        _selector = new DeserializingSelector<TSource>(serializer);
        _projection = ((Expression<Func<TSource, TResult>>)selectExpression).Compile();
    }

    public async Task<IReadOnlyList<TResult>> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var list = new List<TResult>();
        while (await reader.ReadAsync(token))
        {
            var source = _selector.Resolve(reader);
            list.Add(_projection(source));
        }

        return list;
    }
}
