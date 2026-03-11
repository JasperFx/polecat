using System.Data.Common;
using Polecat.Serialization;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads JSON_OBJECT results from a GroupBy query and deserializes each row.
/// </summary>
internal class GroupByListHandler<T>
{
    private readonly ISerializer _serializer;

    public GroupByListHandler(ISerializer serializer)
    {
        _serializer = serializer;
    }

    public async Task<IReadOnlyList<T>> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var results = new List<T>();

        while (await reader.ReadAsync(token))
        {
            var json = reader.GetString(0);
            var item = _serializer.FromJson<T>(json);
            results.Add(item);
        }

        return results;
    }
}
