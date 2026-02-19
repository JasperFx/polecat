using System.Data.Common;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads a list of scalar values from a query (e.g., Select(x => x.Name)).
/// </summary>
internal class ScalarListHandler<T> : IQueryHandler<IReadOnlyList<T>>
{
    public async Task<IReadOnlyList<T>> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var list = new List<T>();
        while (await reader.ReadAsync(token))
        {
            if (await reader.IsDBNullAsync(0, token))
            {
                list.Add(default!);
            }
            else
            {
                var value = reader.GetValue(0);
                list.Add((T)Convert.ChangeType(value, Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T)));
            }
        }

        return list;
    }
}
