using System.Data.Common;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads a scalar value from a query (Count, Sum, Min, Max, Average).
/// </summary>
internal class ScalarHandler<T> : IQueryHandler<T>
{
    public async Task<T> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        await reader.ReadAsync(token);

        if (await reader.IsDBNullAsync(0, token))
        {
            return default!;
        }

        var value = reader.GetValue(0);
        return (T)Convert.ChangeType(value, typeof(T));
    }
}
