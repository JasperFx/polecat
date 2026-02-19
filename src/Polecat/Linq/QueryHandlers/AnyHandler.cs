using System.Data.Common;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads the result of an EXISTS check.
/// </summary>
internal class AnyHandler : IQueryHandler<bool>
{
    public async Task<bool> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        await reader.ReadAsync(token);
        return reader.GetInt32(0) == 1;
    }
}
