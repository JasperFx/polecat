using System.Data.Common;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads all rows from a query and returns them as a list. Materialization goes through
///     the shared selector contract — either the closed-shape QueryOnly selector (root
///     documents, #273 E2d) or Polecat's DeserializingSelector (subclass + event queries).
/// </summary>
internal class ListQueryHandler<T> : IQueryHandler<IReadOnlyList<T>> where T : class
{
    private readonly Weasel.Storage.ISelector<T> _selector;

    public ListQueryHandler(Weasel.Storage.ISelector<T> selector)
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
