using System.Data.Common;
using Polecat.Linq.Selectors;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads a single result from a query (First, Single, etc.).
/// </summary>
internal class OneResultHandler<T> : IQueryHandler<T?> where T : class
{
    private readonly DeserializingSelector<T> _selector;
    private readonly bool _canBeNull;
    private readonly bool _canBeMultiples;

    public OneResultHandler(DeserializingSelector<T> selector, bool canBeNull, bool canBeMultiples)
    {
        _selector = selector;
        _canBeNull = canBeNull;
        _canBeMultiples = canBeMultiples;
    }

    public async Task<T?> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var hasResult = await reader.ReadAsync(token);

        if (!hasResult)
        {
            if (!_canBeNull)
                throw new InvalidOperationException("Sequence contains no elements");

            return default;
        }

        var result = _selector.Resolve(reader);

        if (!_canBeMultiples && await reader.ReadAsync(token))
            throw new InvalidOperationException("Sequence contains more than one element");

        return result;
    }
}
