using System.Data.Common;
using Polecat.Linq.Selectors;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads all rows from a query and returns them as a list.
/// </summary>
internal class ListQueryHandler<T> : IQueryHandler<IReadOnlyList<T>> where T : class
{
    private readonly DeserializingSelector<T> _selector;

    public ListQueryHandler(DeserializingSelector<T> selector)
    {
        _selector = selector;
    }

    public async Task<IReadOnlyList<T>> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var list = new List<T>();
        while (await reader.ReadAsync(token))
        {
            var item = _selector.Resolve(reader);
            list.Add(item);
        }

        return list;
    }
}
